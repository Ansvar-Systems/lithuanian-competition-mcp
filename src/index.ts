#!/usr/bin/env node

/**
 * Lithuanian Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying KT (Konkurencijos taryba — Competition Council
 * of Lithuania) decisions, merger control cases, and sector enforcement activity
 * under Lithuanian competition law (KĮ — Konkurencijos įstatymas).
 *
 * Tool prefix: lt_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { CallToolRequestSchema, ListToolsRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { searchDecisions, getDecision, searchMergers, getMerger, listSectors } from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(readFileSync(join(__dirname, "..", "package.json"), "utf8")) as { version: string };
  pkgVersion = pkg.version;
} catch { /* fallback */ }

const SERVER_NAME = "lithuanian-competition-mcp";

const TOOLS = [
  {
    name: "lt_comp_search_decisions",
    description: "Full-text search across KT enforcement decisions (abuse of dominance, cartels, sector inquiries) under Lithuanian competition law (KĮ). Returns matching decisions with case number, parties, outcome, fine amount, and KĮ articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'piktnaudžiavimas dominuojančia padėtimi', 'kartelis', 'koncentracija')" },
        type: { type: "string", enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"], description: "Filter by decision type. Optional." },
        sector: { type: "string", description: "Filter by sector ID (e.g., 'telecommunications', 'energy', 'retail'). Optional." },
        outcome: { type: "string", enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"], description: "Filter by outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "lt_comp_get_decision",
    description: "Get a specific KT decision by case number (e.g., '2023/11/03-3').",
    inputSchema: {
      type: "object" as const,
      properties: { case_number: { type: "string", description: "KT case number" } },
      required: ["case_number"],
    },
  },
  {
    name: "lt_comp_search_mergers",
    description: "Search KT merger control decisions (concentrations). Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'koncentracija', 'įsigijimas', 'telekomunikacijos')" },
        sector: { type: "string", description: "Filter by sector ID. Optional." },
        outcome: { type: "string", enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"], description: "Filter by merger outcome. Optional." },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "lt_comp_get_merger",
    description: "Get a specific KT merger control decision by case number.",
    inputSchema: {
      type: "object" as const,
      properties: { case_number: { type: "string", description: "KT merger case number" } },
      required: ["case_number"],
    },
  },
  {
    name: "lt_comp_list_sectors",
    description: "List all sectors with KT enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "lt_comp_about",
    description: "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
];

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetDecisionArgs = z.object({ case_number: z.string().min(1) });
const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});
const GetMergerArgs = z.object({ case_number: z.string().min(1) });

function textContent(data: unknown) { return { content: [{ type: "text" as const, text: JSON.stringify(data, null, 2) }] }; }
function errorContent(message: string) { return { content: [{ type: "text" as const, text: message }], isError: true as const }; }

const server = new Server({ name: SERVER_NAME, version: pkgVersion }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;
  try {
    switch (name) {
      case "lt_comp_search_decisions": { const p = SearchDecisionsArgs.parse(args); const r = searchDecisions({ query: p.query, type: p.type, sector: p.sector, outcome: p.outcome, limit: p.limit }); return textContent({ results: r, count: r.length }); }
      case "lt_comp_get_decision": { const p = GetDecisionArgs.parse(args); const d = getDecision(p.case_number); if (!d) return errorContent(`Decision not found: ${p.case_number}`); const dr = d as Record<string, unknown>; return textContent({ ...d, _citation: buildCitation(String(dr.case_number || p.case_number), String(dr.title || dr.case_number || p.case_number), "lt_comp_get_decision", { case_number: p.case_number }, dr.source_url as string | undefined) }); }
      case "lt_comp_search_mergers": { const p = SearchMergersArgs.parse(args); const r = searchMergers({ query: p.query, sector: p.sector, outcome: p.outcome, limit: p.limit }); return textContent({ results: r, count: r.length }); }
      case "lt_comp_get_merger": { const p = GetMergerArgs.parse(args); const m = getMerger(p.case_number); if (!m) return errorContent(`Merger case not found: ${p.case_number}`); const mr = m as Record<string, unknown>; return textContent({ ...m, _citation: buildCitation(String(mr.case_number || p.case_number), String(mr.title || mr.case_number || p.case_number), "lt_comp_get_merger", { case_number: p.case_number }, mr.source_url as string | undefined) }); }
      case "lt_comp_list_sectors": { const s = listSectors(); return textContent({ sectors: s, count: s.length }); }
      case "lt_comp_about": return textContent({ name: SERVER_NAME, version: pkgVersion, description: "KT (Konkurencijos taryba — Competition Council of Lithuania) MCP server. Provides access to Lithuanian competition law enforcement decisions, merger control cases, and sector enforcement data under the KĮ (Konkurencijos įstatymas).", data_source: "Konkurencijos taryba (https://kt.gov.lt/)", coverage: { decisions: "Abuse of dominance, cartel enforcement, and sector inquiries under KĮ", mergers: "Merger control decisions (concentrations) — Phase I and Phase II", sectors: "Telecommunications, energy, retail, financial services, digital economy, pharmaceutical" }, tools: TOOLS.map(t => ({ name: t.name, description: t.description })) });
      default: return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) { return errorContent(`Error executing ${name}: ${err instanceof Error ? err.message : String(err)}`); }
});

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}
main().catch(err => { process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`); process.exit(1); });
