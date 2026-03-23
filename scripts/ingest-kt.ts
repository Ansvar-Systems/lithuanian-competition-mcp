/**
 * Ingestion crawler for the KT (Konkurencijos taryba — Competition Council
 * of Lithuania) MCP server.
 *
 * Scrapes competition enforcement decisions, merger control decisions, and
 * sector data from kt.gov.lt and populates the SQLite database.
 *
 * Data sources:
 *   - https://kt.gov.lt/lt/dokumentai/sarasas/status.33  — Competition Council
 *     decisions (nutarimai): antitrust enforcement, sector inquiries,
 *     unfair commercial practices
 *   - https://kt.gov.lt/lt/dokumentai/koncentracijos      — Merger control
 *     (koncentracijos): clearance decisions, conditional approvals, prohibitions
 *
 * KT listing pages render decisions inline as `div.docs_item` elements
 * containing date, case number, and a link to the decision PDF. There are
 * no individual HTML decision pages — all metadata lives on the listing
 * pages themselves.
 *
 * Usage:
 *   npx tsx scripts/ingest-kt.ts
 *   npx tsx scripts/ingest-kt.ts --dry-run
 *   npx tsx scripts/ingest-kt.ts --resume
 *   npx tsx scripts/ingest-kt.ts --force
 *   npx tsx scripts/ingest-kt.ts --max-pages 5
 */

// kt.gov.lt serves an incomplete certificate chain (PerfectSSL intermediate
// CA missing). Node.js rejects this by default. Allow it so fetch() works.
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import type { AnyNode } from "domhandler";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["KT_LT_DB_PATH"] ?? "data/kt-lt.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://kt.gov.lt";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarKTCrawler/1.0 (+https://github.com/Ansvar-Systems/lithuanian-competition-mcp)";

/**
 * Listing categories on kt.gov.lt.
 *
 * Decisions use /lt/dokumentai/sarasas/status.33 with ?page=N pagination.
 * Merger decisions live under /lt/dokumentai/koncentracijos (sub-pages).
 */
const LISTING_CATEGORIES = [
  {
    id: "nutarimai",
    path: "/lt/dokumentai/sarasas/status.33",
    maxPages: 50,
    isMerger: false,
  },
  {
    id: "koncentracijos",
    path: "/lt/dokumentai/koncentracijos",
    maxPages: 100,
    isMerger: true,
  },
  {
    id: "koncentracijos-be-leidimo",
    path: "/lt/dokumentai/koncentracijos-igyvendinimas-be-konkurencijos-tarybos-leidimo",
    maxPages: 10,
    isMerger: true,
  },
  {
    id: "konkurencija-ribojantys-susitarimai",
    path: "/lt/dokumentai/konkurencija-ribojantys-susitarimai",
    maxPages: 30,
    isMerger: false,
  },
  {
    id: "viesieji-subjektai",
    path: "/lt/dokumentai/viesieji-subjektai",
    maxPages: 20,
    isMerger: false,
  },
] as const;

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPagesOverride = maxPagesArg ? parseInt(maxPagesArg, 10) : null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

/**
 * An item scraped directly from a listing page's `div.docs_item`.
 * KT has no individual HTML decision pages — decisions link to PDFs.
 * All metadata is inline on the listing page.
 */
interface ListingItem {
  /** Decision date from `.docs_dates` (e.g. "2026 02 10") */
  dateRaw: string;
  /** Case number from `.docs_dates` (e.g. "Nutarimo Nr.: 1S-12 (2026)") */
  caseNumberRaw: string;
  /** Publication date if present */
  pubDateRaw: string;
  /** Decision title from `.docs_link a` text */
  title: string;
  /** PDF URL (absolute) */
  pdfUrl: string;
  /** The listing category this came from */
  category: (typeof LISTING_CATEGORIES)[number];
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "lt,en;q=0.5",
        },
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Listing page parsing — scrape inline decision data from listing pages
// ---------------------------------------------------------------------------

/**
 * Parse a single `div.docs_item` element from a listing page.
 *
 * Structure (confirmed 2026-03-23):
 *   <div class="docs_item">
 *     <div class="docs_item_head clearfix">
 *       <ul class="docs_dates">
 *         <li class="float-start">2026 02 10</li>                       ← decision date
 *         <li class="float-start">Nutarimo Nr.: 1S-12 (2026)</li>      ← case number
 *         <li class="float-end">Paskelbimo data: 2026 02 12</li>       ← publication date
 *       </ul>
 *     </div>
 *     <div class="docs_link">
 *       <a href="/uploads/docs/docs/2026-02/hash.pdf" ...>
 *         <p>Title text</p>
 *       </a>
 *     </div>
 *   </div>
 */
function parseDocsItem(
  $: cheerio.CheerioAPI,
  el: AnyNode,
  category: (typeof LISTING_CATEGORIES)[number],
): ListingItem | null {
  const $el = $(el);

  // Extract date items from .docs_dates li elements
  const dateItems: string[] = [];
  $el.find(".docs_dates li").each((_i, li) => {
    dateItems.push($(li).text().trim());
  });

  // First li is typically the decision date (e.g. "2026 02 10")
  const dateRaw = dateItems[0] ?? "";

  // Second li is typically the case number (e.g. "Nutarimo Nr.: 1S-12 (2026)")
  const caseNumberRaw = dateItems.length > 1 ? dateItems[1]! : "";

  // Third li (if present) is the publication date
  const pubDateRaw = dateItems.length > 2 ? dateItems[2]! : "";

  // Title from the link text inside .docs_link
  const linkEl = $el.find(".docs_link a").first();
  const title = linkEl.text().replace(/\s+/g, " ").trim();
  if (!title) return null;

  // PDF URL
  let pdfHref = linkEl.attr("href") ?? "";
  if (pdfHref && !pdfHref.startsWith("http")) {
    // Handle protocol-relative (//kt.gov.lt/...) or path-relative (/uploads/...)
    if (pdfHref.startsWith("//")) {
      pdfHref = `https:${pdfHref}`;
    } else {
      pdfHref = `${BASE_URL}${pdfHref}`;
    }
  }

  return {
    dateRaw,
    caseNumberRaw,
    pubDateRaw,
    title,
    pdfUrl: pdfHref,
    category,
  };
}

/**
 * Parse the case number from the raw listing text.
 *
 * Input examples:
 *   "Nutarimo Nr.: 1S-12 (2026)"
 *   "Nutarimo Nr.: 1S-120 (2025)"
 *   "Nutarimo Nr.: 1S-29(2026)"
 *   "Nutarimo Nr.: 1S-9 (2026)"
 * Returns: "1S-12 (2026)" etc.
 */
function parseCaseNumberFromListing(raw: string): string {
  // Strip common prefix labels
  const cleaned = raw
    .replace(/^nutarimo\s+nr\.?\s*:?\s*/i, "")
    .replace(/^nr\.?\s*:?\s*/i, "")
    .trim();
  return cleaned || raw.trim();
}

/**
 * Parse a KT listing date ("2026 02 10") to ISO format.
 * Falls back to parseLithuanianDate for other formats.
 */
function parseListingDate(raw: string): string | null {
  if (!raw) return null;
  // "2026 02 10" → "2026-02-10"
  const spaceMatch = raw.match(/^(\d{4})\s+(\d{2})\s+(\d{2})$/);
  if (spaceMatch) {
    return `${spaceMatch[1]}-${spaceMatch[2]}-${spaceMatch[3]}`;
  }
  return parseLithuanianDate(raw);
}

/**
 * Crawl paginated listing pages and extract structured items directly.
 *
 * KT listing pages use ?page=N pagination (1-indexed). Each page shows
 * ~12 items as div.docs_item elements. Decision content is in PDFs, not
 * individual HTML pages.
 */
async function scrapeListingItems(
  category: (typeof LISTING_CATEGORIES)[number],
  maxPages: number,
): Promise<ListingItem[]> {
  const items: ListingItem[] = [];
  const seenPdfUrls = new Set<string>();
  const effectiveMax = maxPagesOverride
    ? Math.min(maxPagesOverride, maxPages)
    : maxPages;

  console.log(
    `\n  Scraping items from ${category.id} (up to ${effectiveMax} pages)...`,
  );

  for (let page = 1; page <= effectiveMax; page++) {
    const listUrl =
      page === 1
        ? `${BASE_URL}${category.path}`
        : `${BASE_URL}${category.path}?page=${page}`;

    if (page % 10 === 1 || page === 1) {
      console.log(
        `    Fetching listing page ${page}/${effectiveMax}... (${items.length} items so far)`,
      );
    }

    const html = await rateLimitedFetch(listUrl);
    if (!html) {
      console.warn(`    [WARN] Could not fetch listing page ${page}`);
      continue;
    }

    const $ = cheerio.load(html);
    let pageItems = 0;

    $("div.docs_item").each((_i, el) => {
      const item = parseDocsItem($, el, category);
      if (!item) return;

      // Deduplicate by PDF URL
      if (seenPdfUrls.has(item.pdfUrl)) return;
      seenPdfUrls.add(item.pdfUrl);

      items.push(item);
      pageItems++;
    });

    // If no new items found on this page, pagination is exhausted
    if (pageItems === 0 && page > 1) {
      console.log(
        `    No new items on page ${page} — stopping pagination for ${category.id}`,
      );
      break;
    }
  }

  console.log(`    Scraped ${items.length} items from ${category.id}`);
  return items;
}

// ---------------------------------------------------------------------------
// Page parsing — extract structured data from individual decision pages
// ---------------------------------------------------------------------------

/**
 * Extract labelled metadata fields from a KT decision page.
 *
 * KT pages present metadata in varying formats:
 *   - Definition list (dt/dd) or label-value patterns
 *   - Bold/strong labels followed by text in paragraphs
 *   - Inline "Nutarimo Nr. XYZ" patterns in the body text
 */
function extractMetadata(
  $: cheerio.CheerioAPI,
): Record<string, string> {
  const meta: Record<string, string> = {};

  const labelPatterns: Array<{ label: string; keys: string[] }> = [
    {
      label: "bylos_numeris",
      keys: [
        "bylos numeris",
        "nutarimo nr",
        "nutarimo numeris",
        "bylos nr",
        "tyrimo nr",
        "nr.",
      ],
    },
    {
      label: "data",
      keys: [
        "nutarimo data",
        "sprendimo data",
        "data",
        "priimta",
        "priėmimo data",
      ],
    },
    {
      label: "salys",
      keys: [
        "šalys",
        "salys",
        "dalyviai",
        "ūkio subjektai",
        "ukio subjektai",
        "įmonės",
        "imones",
      ],
    },
    {
      label: "sektorius",
      keys: ["sektorius", "rinka", "rinkos", "veiklos sritis"],
    },
    {
      label: "sprendimas",
      keys: [
        "sprendimas",
        "nutarimas",
        "rezoliucinė dalis",
        "rezoliucine dalis",
      ],
    },
  ];

  // Pattern 1: Definition list (dl/dt/dd)
  $("dl dt, .field--label, .field-label, .label").each((_i, el) => {
    const rawLabel = $(el).text().trim().replace(/:$/, "").toLowerCase();
    const valueEl =
      $(el).next("dd").length > 0
        ? $(el).next("dd")
        : $(el)
            .next(".field--item, .field-item, .field__item")
            .first();
    if (valueEl.length > 0) {
      meta[rawLabel] = valueEl.text().trim();
    }
  });

  // Pattern 2: Structured blocks — look for bold/strong labels followed by text
  $("p, div, span").each((_i, el) => {
    const text = $(el).text().trim();
    for (const { label, keys } of labelPatterns) {
      for (const key of keys) {
        const regex = new RegExp(`^${escapeRegExp(key)}[:\\s]+(.+)`, "i");
        const match = text.match(regex);
        if (match?.[1]) {
          meta[label] = match[1].trim();
        }
      }
    }
  });

  // Pattern 3: Inline case number from body text
  if (!meta["bylos_numeris"]) {
    const bodyText = $("main, article, .content, body").text();

    // KT case numbers: "Nr. 1S-XX/YYYY" or "Nr. 2S-XX/YYYY" etc.
    const caseMatch = bodyText.match(
      /(?:Nr\.?|Nutarimo\s+Nr\.?)\s*(\d+S-\d+\/\d{4})/i,
    );
    if (caseMatch) {
      meta["bylos_numeris"] = caseMatch[1]!;
    }

    // Alternative format: "KT-YYYY-NNNN"
    if (!meta["bylos_numeris"]) {
      const altMatch = bodyText.match(
        /(?:Nr\.?|bylos)\s*[:\s]+(KT[\-\/]\d{4}[\-\/]\d+)/i,
      );
      if (altMatch?.[1]) {
        meta["bylos_numeris"] = altMatch[1];
      }
    }

    // Older format: "Nr. 1S-25" or plain "2S-123/2020"
    if (!meta["bylos_numeris"]) {
      const olderMatch = bodyText.match(
        /(?:Nr\.?|nutarimo|bylos)\s*[:\s]+(\d+[A-Z]-\d+(?:\/\d{4})?)/i,
      );
      if (olderMatch?.[1]) {
        meta["bylos_numeris"] = olderMatch[1];
      }
    }
  }

  return meta;
}

/** Escape special regex characters in a string. */
function escapeRegExp(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Parse a Lithuanian date string to ISO format (yyyy-MM-dd).
 *
 * Handles:
 *   - yyyy-MM-dd (ISO, common on newer pages)
 *   - yyyy m. MM d. NN (Lithuanian government style: "2023 m. liepos 18 d.")
 *   - d.MM.yyyy or dd.MM.yyyy
 *   - Lithuanian textual months
 */
function parseLithuanianDate(raw: string): string | null {
  if (!raw) return null;

  // Already ISO: yyyy-MM-dd
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return isoMatch[0];

  // Lithuanian government date format: "2023 m. liepos 18 d."
  const LITHUANIAN_MONTHS: Record<string, string> = {
    sausio: "01",
    vasario: "02",
    kovo: "03",
    balandžio: "04",
    balandzio: "04",
    gegužės: "05",
    gegužes: "05",
    geguzės: "05",
    geguz: "05",
    birželio: "06",
    birze: "06",
    birzel: "06",
    liepos: "07",
    rugpjūčio: "08",
    rugpjuc: "08",
    rugpjūč: "08",
    rugsėjo: "09",
    rugse: "09",
    spalio: "10",
    lapkričio: "11",
    lapkric: "11",
    gruodžio: "12",
    gruodz: "12",
    // Nominative forms (for alternative patterns)
    sausis: "01",
    vasaris: "02",
    kovas: "03",
    balandis: "04",
    gegužė: "05",
    geguze: "05",
    birželis: "06",
    birzelis: "06",
    liepa: "07",
    rugpjūtis: "08",
    rugpjutis: "08",
    rugsėjis: "09",
    rugsejis: "09",
    spalis: "10",
    lapkritis: "11",
    gruodis: "12",
  };

  // "2023 m. liepos 18 d." or "2023 m. liepos mėn. 18 d."
  const govMatch = raw.match(
    /(\d{4})\s*m\.?\s+(\w+)\s+(?:mėn\.?\s+)?(\d{1,2})\s*d\.?/i,
  );
  if (govMatch) {
    const [, year, monthName, day] = govMatch;
    const monthNum = findLithuanianMonth(monthName!, LITHUANIAN_MONTHS);
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // "liepos 18 d., 2023" or "liepos 18, 2023"
  const textMatch = raw.match(
    /(\w+)\s+(\d{1,2})\s*(?:d\.?)?,?\s*(\d{4})/i,
  );
  if (textMatch) {
    const [, monthName, day, year] = textMatch;
    const monthNum = findLithuanianMonth(monthName!, LITHUANIAN_MONTHS);
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // "18 d. liepos 2023" (day-first variant)
  const dayFirstMatch = raw.match(
    /(\d{1,2})\s*d\.?\s+(\w+)\s+(\d{4})/i,
  );
  if (dayFirstMatch) {
    const [, day, monthName, year] = dayFirstMatch;
    const monthNum = findLithuanianMonth(monthName!, LITHUANIAN_MONTHS);
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // dd.MM.yyyy or d.MM.yyyy
  const dotMatch = raw.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (dotMatch) {
    const [, day, month, year] = dotMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  return null;
}

/** Find a Lithuanian month number by prefix-matching against known forms. */
function findLithuanianMonth(
  input: string,
  months: Record<string, string>,
): string | null {
  const lower = input.toLowerCase();
  // Exact match first
  if (months[lower]) return months[lower]!;
  // Prefix match (handles diacritics variations)
  for (const [key, value] of Object.entries(months)) {
    if (lower.startsWith(key.slice(0, 4)) || key.startsWith(lower.slice(0, 4))) {
      return value;
    }
  }
  return null;
}

/**
 * Extract a fine/penalty amount from Lithuanian text.
 *
 * Lithuanian uses comma as decimal separator and spaces or dots as
 * thousands separator. Common patterns:
 *   - "1 850 000 eurų bauda"
 *   - "bauda — 4,2 mln. eurų"
 *   - "skirta 2 900 000 Eur piniginė bauda"
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    // "N mln. eurų" / "N mln eurų" / "N milijonų eurų"
    /([\d,.\s]+)\s*(?:mln\.?|milijon[ųu])\s*(?:eur[ųu]|EUR)/gi,
    // "N mlrd. eurų" / "N mlrd eurų"
    /([\d,.\s]+)\s*(?:mlrd\.?|milijard[ųu])\s*(?:eur[ųu]|EUR)/gi,
    // "€ 1.234.567" or "EUR 1 234 567"
    /(?:€|EUR)\s*([\d\s.]+(?:,\d+)?)/gi,
    // "N eurų bauda" / "N Eur piniginė bauda" / "N eurų piniginę baudą"
    /([\d\s.]+(?:,\d+)?)\s*(?:eur[ųu]|Eur)\s+(?:pinigin[ęe]?\s*)?baud/gi,
    // "bauda — N eurų" / "bauda – N eurų"
    /baud[aąų]\s*[—–\-]\s*([\d\s.]+(?:,\d+)?)\s*(?:eur[ųu]|EUR)/gi,
    // "skirta N eurų" / "skirta N Eur"
    /skirt[ao]\s+(?:(?:pinigin[ęe]\s+)?baud[aąų]\s+)?([\d\s.]+(?:,\d+)?)\s*(?:eur[ųu]|EUR)/gi,
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1]) {
      let numStr = match[1].trim();

      // Detect "mlrd" / "milijardų"
      if (pattern.source.includes("mlrd") || pattern.source.includes("milijard")) {
        numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000_000;
      }

      // Detect "mln" / "milijonų"
      if (pattern.source.includes("mln") || pattern.source.includes("milijon")) {
        numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000;
      }

      // Direct amount: Lithuanian uses space as thousands separator,
      // comma for decimal
      numStr = numStr.replace(/[\s.]/g, "").replace(",", ".");
      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 1000) return val;
    }
  }

  return null;
}

/**
 * Extract cited Lithuanian competition law articles and EU treaty articles
 * from the decision text.
 *
 * Lithuanian Competition Act (Konkurencijos įstatymas):
 *   - 5 str. — agreements restricting competition (cf. TFEU Art. 101)
 *   - 7 str. — abuse of dominant position (cf. TFEU Art. 102)
 *   - 9 str. — unfair competition actions by public administration
 *   - 10-14 str. — merger control
 *   - 36 str. — sanctions / fines
 *   - 40 str. — commitment decisions
 *   - 41 str. — leniency
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  let m: RegExpExecArray | null;

  // Konkurencijos įstatymo N straipsnis / N str. / N straipsnio
  const kiPattern =
    /(?:konkurencijos\s+[iį]statymo|K[IĮ])\s+(\d+)\s*(?:straipsn[iįy]|str\.)/gi;
  while ((m = kiPattern.exec(text)) !== null) {
    articles.add(`KĮ ${m[1]} str.`);
  }

  // Standalone "N straipsnis" / "N str." near competition law context
  const sectionPattern = /(\d+)\s*(?:straipsn[iįy]|str\.)/g;
  while ((m = sectionPattern.exec(text)) !== null) {
    const num = parseInt(m[1]!, 10);
    // Only capture articles commonly referenced in Lithuanian competition law
    if ([5, 7, 9, 10, 11, 12, 13, 14, 36, 40, 41].includes(num)) {
      articles.add(`KĮ ${num} str.`);
    }
  }

  // Nesąžiningos prekybos praktikos (NSP) įstatymas
  const nspPattern =
    /(?:nesąžiningos\s+prekybos|NSP)\s+(?:praktikos\s+)?(?:[iį]statymo\s+)?(\d+)\s*(?:straipsn[iįy]|str\.)/gi;
  while ((m = nspPattern.exec(text)) !== null) {
    articles.add(`NSP ${m[1]} str.`);
  }

  // EU treaty articles: SESV 101 / 102 (TFEU in Lithuanian)
  const euPattern =
    /(?:SESV|TFEU|Sutarties\s+dėl\s+Europos\s+Sąjungos\s+veikimo)\s*(?:straipsn[iįy]\s*)?(\d{2,3})\s*(?:straipsn[iįy]|str\.)?/gi;
  while ((m = euPattern.exec(text)) !== null) {
    const artNum = parseInt(m[1]!, 10);
    if (artNum === 101 || artNum === 102) {
      articles.add(`SESV ${artNum}`);
    }
  }

  // "Art. 101" / "Art. 102" patterns
  const artPattern = /Art(?:ikl)?\.?\s*(101|102)/gi;
  while ((m = artPattern.exec(text)) !== null) {
    articles.add(`SESV ${m[1]}`);
  }

  return [...articles];
}

/**
 * Classify a KT decision based on its URL category, metadata, and content.
 */
function classifyDecisionType(
  title: string,
  bodyText: string,
): {
  type: string | null;
  outcome: string | null;
} {
  const lowerTitle = title.toLowerCase();
  const lowerBody = bodyText.toLowerCase().slice(0, 3000);
  const all = `${lowerTitle} ${lowerBody}`;

  // --- Type classification ---
  let type: string | null = null;

  if (
    all.includes("kartel") ||
    all.includes("draudžiam") ||
    all.includes("draudžiamasis susitarimas") ||
    all.includes("suderint") ||
    all.includes("kainų nustatymas") ||
    (all.includes("5 str") && all.includes("susitarim"))
  ) {
    type = "cartel";
  } else if (
    all.includes("piktnaudžiav") ||
    all.includes("dominuojančia padėtimi") ||
    all.includes("dominuojančią padėtį") ||
    all.includes("piktnaudžiavimas") ||
    (all.includes("7 str") && all.includes("dominuoj"))
  ) {
    type = "abuse_of_dominance";
  } else if (
    all.includes("sektori") && all.includes("tyrim") ||
    all.includes("rinkos tyrim") ||
    all.includes("stebėsen")
  ) {
    type = "sector_inquiry";
  } else if (
    all.includes("nesąžining") && all.includes("prekybos praktik") ||
    all.includes("nsp")
  ) {
    type = "unfair_trading_practice";
  } else if (
    all.includes("valstybės pagalb") ||
    all.includes("valstybes pagalb")
  ) {
    type = "state_aid";
  } else if (
    all.includes("viešasis pirkimas") ||
    all.includes("viesasis pirkimas") ||
    all.includes("viešųjų pirkimų")
  ) {
    type = "public_procurement";
  } else if (
    all.includes("reklam") &&
    (all.includes("klaidinanti") || all.includes("nesąžining"))
  ) {
    type = "misleading_advertising";
  } else if (
    all.includes("įsipareigojim") ||
    all.includes("isipareigojim") ||
    all.includes("40 str")
  ) {
    type = "commitment_decision";
  } else {
    type = "decision";
  }

  // --- Outcome classification ---
  let outcome: string | null = null;

  if (
    (all.includes("baud") || all.includes("seuraamusmaksu")) &&
    (all.includes("skirt") || all.includes("paskirta"))
  ) {
    outcome = "fine";
  } else if (
    all.includes("atsisakymo pradėti tyrimą") ||
    all.includes("atsisakyta pradėti")
  ) {
    outcome = "dismissed";
  } else if (
    all.includes("tyrimo nutraukim") ||
    all.includes("tyrimas nutrauktas") ||
    all.includes("nutraukė tyrimą")
  ) {
    outcome = "closed";
  } else if (
    all.includes("įsipareigojim") &&
    (all.includes("priėm") || all.includes("priem"))
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("pažeidimo nenustat") ||
    all.includes("pažeidimas nenustat") ||
    all.includes("nenustatyta pažeidim")
  ) {
    outcome = "cleared";
  }

  return { type, outcome };
}

/**
 * Classify a merger outcome based on page content.
 */
function classifyMergerOutcome(
  title: string,
  bodyText: string,
): string | null {
  const all = `${title} ${bodyText}`.toLowerCase();

  if (
    all.includes("draudžia") ||
    all.includes("neleidžia") ||
    all.includes("atsisakė leisti")
  ) {
    return "blocked";
  }
  if (
    all.includes("su sąlyg") ||
    all.includes("su salygom") ||
    all.includes("sąlyginis") ||
    all.includes("įpareigojim") &&
    all.includes("leid")
  ) {
    return "cleared_with_conditions";
  }
  if (
    all.includes("atsisakym") ||
    all.includes("nebuvo pranešta")
  ) {
    return "dismissed";
  }
  if (
    all.includes("atšaukt") ||
    all.includes("atsaukt") ||
    all.includes("atsiėm")
  ) {
    return "withdrawn";
  }
  if (
    all.includes("be konkurencijos tarybos leidimo") ||
    all.includes("nenotifikuota")
  ) {
    return "gun_jumping";
  }
  if (
    all.includes("leid") &&
    (all.includes("koncentracij") || all.includes("vykdyti"))
  ) {
    // Check for phase 2
    if (
      all.includes("papildom") ||
      all.includes("antrasis etap") ||
      all.includes("ii etap") ||
      all.includes("phase 2") ||
      all.includes("phase ii")
    ) {
      return "cleared_phase2";
    }
    return "cleared_phase1";
  }

  // Default for merger pages: assume clearance
  return "cleared_phase1";
}

/**
 * Map Lithuanian keywords in title/body to sector IDs.
 */
function classifySector(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText.slice(0, 2000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energy",
      patterns: [
        "energetik",
        "elektros energij",
        "gamtinių dujų",
        "gamtiniu duju",
        "šilumos",
        "silumos",
        "atsinaujinant",
        "naftos",
        "kuro",
        "degalin",
        "ignitis",
        "litgrid",
        "energij",
      ],
    },
    {
      id: "retail",
      patterns: [
        "mažmeninė prekybos",
        "mažmenin",
        "mazmenin",
        "maisto produkt",
        "parduotuv",
        "prekybos tinkl",
        "maxima",
        "iki retail",
        "norfa",
        "rimi",
        "lidl",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telekomunikacij",
        "mobiliojo ryšio",
        "mobiliojo rysio",
        "plačiajuost",
        "placiajuost",
        "interneto",
        "televizij",
        "telia",
        "bite",
        "tele2",
        "cgates",
      ],
    },
    {
      id: "pharmaceutical",
      patterns: [
        "farmacij",
        "vaist",
        "vaistinių",
        "vaistini",
        "receptin",
        "eurovaistinė",
        "camelia",
        "gintarinė",
        "tamro",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transport",
        "krovinių vežim",
        "kroviniu vezim",
        "logistik",
        "viešasis transportas",
        "keleivin",
        "vežim",
        "girteka",
      ],
    },
    {
      id: "financial_services",
      patterns: [
        "bankin",
        "finansin",
        "draudim",
        "kredito",
        "mokėjim",
        "mokejim",
        "investicij",
        "vertybinių popierių",
        "lizingo",
      ],
    },
    {
      id: "construction",
      patterns: [
        "statyb",
        "statybinių",
        "nekilnojam",
        "asfalto",
        "betono",
        "rangov",
        "projektavim",
      ],
    },
    {
      id: "food_industry",
      patterns: [
        "pieno",
        "mėsos",
        "mesos",
        "grūdų",
        "grudu",
        "alkoholio",
        "gėrimų",
        "gerimu",
        "žemės ūkio",
        "zemes ukio",
        "žuvininkystė",
        "cukraus",
      ],
    },
    {
      id: "digital_economy",
      patterns: [
        "skaitmenin",
        "e-komercij",
        "elektronin",
        "platformų",
        "platformu",
        "programin",
        "IT paslaugų",
        "duomenų",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "sveikatos",
        "ligoninės",
        "ligonines",
        "medicin",
        "gydymo",
        "medicinos priemoni",
        "dantų",
        "odontolog",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "atliekų tvarkymo",
        "atliekas",
        "perdirbim",
        "antrinių žaliavų",
      ],
    },
    {
      id: "media",
      patterns: [
        "žiniasklaid",
        "reklam",
        "leidyb",
        "spaud",
        "televizij",
        "radij",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

/**
 * Extract acquiring party and target from a merger title/body.
 *
 * KT merger decision titles follow patterns like:
 *   "DĖL LEIDIMO UAB „X" ĮSIGYTI UAB „Y" AKCIJŲ"
 *   "DĖL LEIDIMO X VYKDYTI KONCENTRACIJĄ ĮSIGYJANT Y"
 */
function extractMergerParties(
  title: string,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  // Pattern 1a: "LEIDIMO X ĮSIGYTI/ĮSIGYJANT Y AKCIJŲ"
  const acquireMatch = title.match(
    /(?:LEIDIMO|LEIDO)\s+(.+?)\s+(?:ĮSIGYTI|ĮSIGYJANT|VYKDYTI\s+KONCENTRACIJĄ\s+ĮSIGYJANT)\s+(.+?)(?:\s+AKCI[JŲ]|\s+TURTO|\s*$)/i,
  );
  if (acquireMatch) {
    return {
      acquiring: cleanPartyName(acquireMatch[1]!),
      target: cleanPartyName(acquireMatch[2]!),
    };
  }

  // Pattern 1b: Listing page format "Leisti vykdyti koncentraciją X įsigyjant Y akcijų"
  const listingMatch = title.match(
    /(?:leisti\s+vykdyti\s+koncentraciją|koncentracija)\s+(.+?)\s+(?:įsigyjant|įsigyti)\s+(.+?)(?:\s+akci[jų]|\s+ir\s+(?:tokiu\s+)?būdu|\s*$)/i,
  );
  if (listingMatch) {
    return {
      acquiring: cleanPartyName(listingMatch[1]!),
      target: cleanPartyName(listingMatch[2]!),
    };
  }

  // Pattern 2: "X" IR "Y" STEIGTI BENDRĄ ĮMONĘ (joint venture)
  const jvMatch = title.match(
    /(?:LEIDIMO|LEIDO)\s+(.+?)\s+(?:IR\s+)?(.+?)\s+STEIGTI\s+BENDR/i,
  );
  if (jvMatch) {
    return {
      acquiring: cleanPartyName(jvMatch[1]!),
      target: cleanPartyName(jvMatch[2]!),
    };
  }

  // Pattern 3: Look in body text for "X įsigijo Y" / "X perėmė Y kontrolę"
  const bodyMatch = bodyText.match(
    /(.{3,80}?)\s+(?:įsigijo|įsigyja|perėmė|perem)\s+(.{3,80}?)(?:\s+kontrol|\s+akci[jų]|\.|,)/i,
  );
  if (bodyMatch) {
    return {
      acquiring: cleanPartyName(bodyMatch[1]!),
      target: cleanPartyName(bodyMatch[2]!),
    };
  }

  return { acquiring: null, target: null };
}

/** Clean up a party name extracted from a decision title. */
function cleanPartyName(raw: string): string {
  return raw
    .replace(/^[„"«»"]+/, "")
    .replace(/[„"«»"]+$/, "")
    .replace(/\s*,\s*$/, "")
    .trim()
    .slice(0, 300);
}

/**
 * Generate a case number from the URL slug when none is found in page metadata.
 */
function generateCaseNumber(url: string): string {
  const slug = url
    .replace(BASE_URL, "")
    .replace(/^\/lt\/dokumentai\//, "")
    .replace(/\/$/, "")
    .replace(/\//g, "-");
  const shortSlug = slug.slice(0, 80).replace(/-+$/, "");
  return `KT-WEB/${shortSlug}`;
}

/**
 * Parse a single KT decision/merger detail page.
 */
function parsePage(
  html: string,
  url: string,
  isMerger: boolean,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // --- Title ---
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    $("title")
      .text()
      .trim()
      .replace(/\s*[-–|]\s*(?:Konkurencijos taryba).*$/i, "") ||
    "";

  if (!title) {
    return { decision: null, merger: null };
  }

  // --- Metadata fields ---
  const meta = extractMetadata($);

  // --- Body text ---
  const bodySelectors = [
    ".field--name-body",
    "article .body",
    ".node__content .field--name-body",
    ".content-area",
    ".text-content",
    "main article",
    ".region-content",
  ];

  let bodyText = "";
  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0) {
      bodyText = el.text().trim();
      if (bodyText.length > 100) break;
    }
  }

  // Fallback: gather all paragraphs from main
  if (!bodyText || bodyText.length < 100) {
    const paragraphs: string[] = [];
    $("main p, article p, .content p").each((_i, el) => {
      const text = $(el).text().trim();
      if (text.length > 20) paragraphs.push(text);
    });
    bodyText = paragraphs.join("\n\n");
  }

  // Last resort: strip nav/footer and take what remains
  if (!bodyText || bodyText.length < 50) {
    $(
      "nav, footer, header, .menu, .breadcrumb, script, style, .skip-link",
    ).remove();
    bodyText = $("main, article, .content").text().trim();
  }

  if (!bodyText || bodyText.length < 30) {
    return { decision: null, merger: null };
  }

  // --- Case number ---
  const caseNumber =
    meta["bylos_numeris"] ?? generateCaseNumber(url);

  // --- Date ---
  const rawDate =
    meta["data"] ??
    meta["priimta"] ??
    "";
  const date = parseLithuanianDate(rawDate);

  // --- Sector ---
  const sector = classifySector(title, bodyText);

  // --- Summary (first ~500 chars of body) ---
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();

  // --- Auto-detect merger from title if not already classified ---
  const isMergerFromTitle =
    isMerger ||
    title.toLowerCase().includes("koncentracij") ||
    title.toLowerCase().includes("leidimo") &&
      (title.toLowerCase().includes("įsigyti") ||
        title.toLowerCase().includes("vykdyti koncentracij"));

  // --- Route to merger vs. decision ---
  if (isMergerFromTitle) {
    const { acquiring, target } = extractMergerParties(title, bodyText);
    const outcome = classifyMergerOutcome(title, bodyText);

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date,
        sector,
        acquiring_party: acquiring,
        target,
        summary,
        full_text: bodyText,
        outcome,
        turnover: null, // Turnover not reliably extractable from HTML
      },
    };
  }

  // Non-merger decision
  const { type, outcome } = classifyDecisionType(title, bodyText);
  const parties = meta["salys"] ?? null;
  const fineAmount = extractFineAmount(bodyText);
  const legalArticles = extractLegalArticles(bodyText);

  return {
    decision: {
      case_number: caseNumber,
      title,
      date,
      type,
      sector,
      parties: parties
        ? JSON.stringify(
            parties
              .split(/[,;]/)
              .map((p) => p.trim())
              .filter(Boolean),
          )
        : null,
      summary,
      full_text: bodyText,
      outcome: outcome ?? (fineAmount ? "fine" : null),
      fine_amount: fineAmount,
      gwb_articles:
        legalArticles.length > 0
          ? JSON.stringify(legalArticles)
          : null,
      status: "final",
    },
    merger: null,
  };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Sector metadata (Lithuanian + English names)
// ---------------------------------------------------------------------------

const SECTOR_META: Record<string, { name: string; name_en: string }> = {
  energy: { name: "Energetika", name_en: "Energy" },
  retail: { name: "Mažmeninė prekyba", name_en: "Retail" },
  telecommunications: {
    name: "Telekomunikacijos",
    name_en: "Telecommunications",
  },
  pharmaceutical: { name: "Farmacija", name_en: "Pharmaceutical" },
  transport: { name: "Transportas", name_en: "Transport" },
  financial_services: {
    name: "Finansinės paslaugos",
    name_en: "Financial Services",
  },
  construction: { name: "Statyba", name_en: "Construction" },
  food_industry: {
    name: "Maisto pramonė",
    name_en: "Food Industry",
  },
  digital_economy: {
    name: "Skaitmeninė ekonomika",
    name_en: "Digital Economy",
  },
  healthcare: {
    name: "Sveikatos priežiūra",
    name_en: "Healthcare",
  },
  waste_management: {
    name: "Atliekų tvarkymas",
    name_en: "Waste Management",
  },
  media: { name: "Žiniasklaida", name_en: "Media" },
};

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== KT Konkurencijos taryba — sprendimų crawler ===");
  console.log(`  Duomenų bazė: ${DB_PATH}`);
  console.log(`  Bandomasis:   ${dryRun}`);
  console.log(`  Tęsti:        ${resume}`);
  console.log(`  Perrašyti:    ${force}`);
  console.log(
    `  Max puslapių: ${maxPagesOverride ?? "pagal kategorijas"}`,
  );
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // Step 1: Scrape listing pages to extract items directly.
  // KT has no individual HTML decision pages — all metadata is inline
  // on the listing pages, and decisions link to PDFs.
  const allItems: ListingItem[] = [];

  for (const category of LISTING_CATEGORIES) {
    const items = await scrapeListingItems(category, category.maxPages);
    allItems.push(...items);
  }

  // Deduplicate by PDF URL across categories
  const seenPdfUrls = new Set<string>();
  const dedupedItems = allItems.filter((item) => {
    if (seenPdfUrls.has(item.pdfUrl)) return false;
    seenPdfUrls.add(item.pdfUrl);
    return true;
  });

  // Filter already-processed items (for --resume, keyed by PDF URL)
  const itemsToProcess = resume
    ? dedupedItems.filter((item) => !processedSet.has(item.pdfUrl))
    : dedupedItems;

  console.log(`\nRasta elementų:      ${dedupedItems.length}`);
  console.log(`Apdoroti reikia:     ${itemsToProcess.length}`);
  if (resume && dedupedItems.length !== itemsToProcess.length) {
    console.log(
      `  Praleidžiama ${dedupedItems.length - itemsToProcess.length} jau apdorotų elementų`,
    );
  }

  if (itemsToProcess.length === 0) {
    console.log("Nėra ką apdoroti. Baigta.");
    return;
  }

  // Step 2: Initialize database (unless dry run)
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // Step 3: Process each scraped item — convert to decision or merger
  let decisionsIngested = 0;
  let mergersIngested = 0;
  let errors = 0;
  let skipped = 0;

  for (let i = 0; i < itemsToProcess.length; i++) {
    const item = itemsToProcess[i]!;
    const progress = `[${i + 1}/${itemsToProcess.length}]`;

    console.log(`${progress} ${item.category.id} | ${item.title.slice(0, 90)}`);

    try {
      const caseNumber = parseCaseNumberFromListing(item.caseNumberRaw);
      const date = parseListingDate(item.dateRaw);
      const isMerger = item.category.isMerger;
      const title = item.title;
      // Use title as body text for classification (no separate page to fetch)
      const bodyText = title;
      const sector = classifySector(title, bodyText);
      const summary = title;

      const isMergerFromTitle =
        isMerger ||
        title.toLowerCase().includes("koncentracij") ||
        (title.toLowerCase().includes("leidimo") &&
          (title.toLowerCase().includes("įsigyti") ||
            title.toLowerCase().includes("vykdyti koncentracij")));

      if (isMergerFromTitle) {
        const { acquiring, target } = extractMergerParties(title, bodyText);
        const outcome = classifyMergerOutcome(title, bodyText);

        const merger: ParsedMerger = {
          case_number: caseNumber || `KT-PDF/${item.pdfUrl.split("/").pop()?.replace(".pdf", "") ?? "unknown"}`,
          title,
          date,
          sector,
          acquiring_party: acquiring,
          target,
          summary,
          full_text: title,
          outcome,
          turnover: null,
        };

        if (dryRun) {
          console.log(
            `  KONCENTRACIJA: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
          );
          console.log(
            `    sektorius=${merger.sector}, baigtis=${merger.outcome}, įsigyjantis=${merger.acquiring_party?.slice(0, 50)}`,
          );
        } else {
          const stmt = force
            ? stmts!.upsertMerger
            : stmts!.insertMerger;
          stmt.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          console.log(
            `  ĮRAŠYTA koncentracija: ${merger.case_number}`,
          );
        }

        mergersIngested++;
      } else {
        const { type, outcome } = classifyDecisionType(title, bodyText);
        const fineAmount = extractFineAmount(title);
        const legalArticles = extractLegalArticles(title);

        const decision: ParsedDecision = {
          case_number: caseNumber || `KT-PDF/${item.pdfUrl.split("/").pop()?.replace(".pdf", "") ?? "unknown"}`,
          title,
          date,
          type,
          sector,
          parties: null,
          summary,
          full_text: title,
          outcome: outcome ?? (fineAmount ? "fine" : null),
          fine_amount: fineAmount,
          gwb_articles:
            legalArticles.length > 0
              ? JSON.stringify(legalArticles)
              : null,
          status: "final",
        };

        if (dryRun) {
          console.log(
            `  SPRENDIMAS: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
          );
          console.log(
            `    tipas=${decision.type}, sektorius=${decision.sector}, baigtis=${decision.outcome}, bauda=${decision.fine_amount}`,
          );
        } else {
          const stmt = force
            ? stmts!.upsertDecision
            : stmts!.insertDecision;
          stmt.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.gwb_articles,
            decision.status,
          );
          console.log(
            `  ĮRAŠYTAS sprendimas: ${decision.case_number}`,
          );
        }

        decisionsIngested++;
      }

      // Mark item as processed (keyed by PDF URL)
      processedSet.add(item.pdfUrl);
      state.processedUrls.push(item.pdfUrl);
    } catch (err) {
      const message =
        err instanceof Error ? err.message : String(err);
      console.error(`  KLAIDA: ${message}`);
      state.errors.push(`parse_error: ${item.pdfUrl}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 items)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested += decisionsIngested;
      state.mergersIngested += mergersIngested;
      saveState(state);
      console.log(
        `  [tarpinė būsena] Būsena išsaugota po ${i + 1} elementų`,
      );
    }
  }

  // Step 4: Update sector counts from the database
  if (!dryRun && db && stmts) {
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = {
          decisions: 0,
          mergers: 0,
        };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = {
          decisions: 0,
          mergers: 0,
        };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(
        finalSectorCounts,
      )) {
        const meta = SECTOR_META[id];
        stmts!.upsertSector.run(
          id,
          meta?.name ?? id,
          meta?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nAtnaujinta ${Object.keys(finalSectorCounts).length} sektorių įrašų`,
    );
  }

  // Step 5: Final state save
  state.decisionsIngested += decisionsIngested;
  state.mergersIngested += mergersIngested;
  saveState(state);

  // Step 6: Summary
  if (!dryRun && db) {
    const decisionCount = (
      db
        .prepare("SELECT count(*) as cnt FROM decisions")
        .get() as { cnt: number }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Duomenų surinkimas baigtas ===");
    console.log(`  Sprendimai DB:        ${decisionCount}`);
    console.log(`  Koncentracijos DB:    ${mergerCount}`);
    console.log(`  Sektoriai DB:         ${sectorCount}`);
    console.log(`  Nauji sprendimai:     ${decisionsIngested}`);
    console.log(`  Naujos koncentracijos: ${mergersIngested}`);
    console.log(`  Klaidos:              ${errors}`);
    console.log(`  Praleista:            ${skipped}`);
    console.log(`  Būsena:               ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Bandomasis vykdymas baigtas ===");
    console.log(`  Rasti sprendimai:      ${decisionsIngested}`);
    console.log(`  Rastos koncentracijos: ${mergersIngested}`);
    console.log(`  Klaidos:               ${errors}`);
    console.log(`  Praleista:             ${skipped}`);
  }

  console.log(`\nBaigta.`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Kritinė klaida:", err);
  process.exit(1);
});
