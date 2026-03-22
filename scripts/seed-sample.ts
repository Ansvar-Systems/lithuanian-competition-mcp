/**
 * Seed the KT (Competition Council of Lithuania) database with sample decisions,
 * mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["KT_LT_DB_PATH"] ?? "data/kt-lt.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted existing database at ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

const sectors = [
  { id: "energy", name: "Energetika", name_en: "Energy", description: "Elektros energijos gamyba, perdavimas ir tiekimas, gamtinės dujos ir atsinaujinantys energijos šaltiniai.", decision_count: 3, merger_count: 1 },
  { id: "retail", name: "Mažmeninė prekyba", name_en: "Retail", description: "Maisto produktų mažmeninė prekyba, parduotuvių tinklai ir elektroninė prekyba.", decision_count: 2, merger_count: 2 },
  { id: "telecommunications", name: "Telekomunikacijos", name_en: "Telecommunications", description: "Mobiliojo ryšio tinklai, plačiajuostis internetas ir televizijos paslaugos.", decision_count: 2, merger_count: 1 },
  { id: "pharmaceutical", name: "Farmacija", name_en: "Pharmaceutical", description: "Vaistų gamyba, vaistinių tinklai ir farmacijos platinimas.", decision_count: 2, merger_count: 0 },
  { id: "transport", name: "Transportas", name_en: "Transport", description: "Krovinių vežimas, viešasis transportas ir logistika.", decision_count: 1, merger_count: 1 },
];

const is = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) is.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

const decisions = [
  { case_number: "2023/11/03-3", title: "Ignitis Grupė — piktnaudžiavimas dominuojančia padėtimi elektros energijos mažmeninės prekybos rinkoje", date: "2023-07-18", type: "abuse_of_dominance", sector: "energy", parties: JSON.stringify(["Ignitis UAB", "Ignitis Grupė AB"]), summary: "KT nustatė, kad Ignitis UAB piktnaudžiavo dominuojančia padėtimi elektros energijos mažmeninės prekybos rinkoje, taikant nesąžiningas sąlygas buitiniams vartotojams, keičiantiems tiekėją.", full_text: "KT pradėjo tyrimą prieš Ignitis UAB pagal Lietuvos Respublikos konkurencijos įstatymą. Ignitis užima dominuojančią padėtį elektros energijos mažmeninės prekybos rinkoje buitiniams vartotojams su rinkos dalimi virš 45%. KT nustatė, kad bendrovė taikė perteklines kliūtis vartotojams, norintiems keisti tiekėją: nepagrįstai ilgas perjungimo terminas, sudėtingos atsisakymo procedūros ir neaiški informacija apie kainas. Bendrovei skirta piniginė bauda ir įpareigojimas supaprastinti tiekėjo keitimo procesą.", outcome: "fine", fine_amount: 1_850_000, gwb_articles: JSON.stringify(["9", "36"]), status: "final" },
  { case_number: "2022/09/03-1", title: "Farmacijos kompanijos — koordinuota kainodara vaistinėse", date: "2022-11-30", type: "cartel", sector: "pharmaceutical", parties: JSON.stringify(["Eurovaistinė UAB", "Camelia vaistinės UAB", "Gintarinė vaistinė UAB"]), summary: "KT atskleidė ir nubaudė trijų pirmaujančių vaistinių tinklų koordinuotą kainų nustatymą receptinių vaistų segmente, pažeidžiant Konkurencijos įstatymo 5 straipsnį.", full_text: "KT atliko kratą ir tyrimą trijuose didžiausiuose vaistinių tinkluose dėl koordinuoto kainų nustatymo. Tyrimas atskleidė, kad įmonių atstovai reguliariai keitėsi komerciniu pobūdžiu jautria informacija apie būsimus kainos pokyčius dažniausiai parduodamų receptinių vaistų kategorijose. Koordinavimas vyko per neformaliuosius ryšius tarp pardavimų vadybininkų. KT skyrė baudas visiems trims dalyviams, atsižvelgdamas į pažeidimo trukmę ir poveikį vartotojams.", outcome: "fine", fine_amount: 4_200_000, gwb_articles: JSON.stringify(["5", "36"]), status: "final" },
  { case_number: "2023/06/03-7", title: "Mažmeninė prekyba — prekybos tinklų sąlygos maisto tiekėjams", date: "2023-04-12", type: "abuse_of_dominance", sector: "retail", parties: JSON.stringify(["Maxima LT UAB", "IKI Retail UAB"]), summary: "KT tyrė nesąžiningą prekybos praktiką, kurią didžiausi mažmeninės prekybos tinklai taikė maisto produktų tiekėjams, ypač mažoms ir vidutinėms įmonėms.", full_text: "KT pradėjo tyrimą pagal Nesąžiningos prekybos praktikos maisto grandinėje įstatymą ir Konkurencijos įstatymą. Tyrimo metu nustatyta, kad du didžiausi prekybos tinklai sistematiškai taikė sąlygas, pažeidžiančias tiekimo sutarčių skaidrumą: vienašaliai sąlygų pakeitimai be pakankamo įspėjimo termino, nepagrįsti mokesčiai už prekyvietes ir vidinė informacija apie tiekėjų komercines paslaptis naudota derybose. KT skyrė baudas ir įpareigojo pakeisti tipines sutarties sąlygas.", outcome: "fine", fine_amount: 2_900_000, gwb_articles: JSON.stringify(["9", "NSP"]), status: "final" },
  { case_number: "2022/04/03-2", title: "Telekomunikacijų sektorius — sektorinis tyrimas", date: "2022-08-25", type: "sector_inquiry", sector: "telecommunications", parties: JSON.stringify(["Telia Lietuva AB", "Bite Lietuva UAB", "Tele2 UAB"]), summary: "KT paskelbė sektorinio tyrimo telekomunikacijų rinkoje rezultatus, nustatydamas struktūrines kliūtis veiksmingai konkurencijai mobiliojo ryšio paslaugų rinkoje.", full_text: "KT baigė sektorinį tyrimą telekomunikacijų rinkoje. Nustatytos šios problemos: (1) didelė rinkos koncentracija — trys operatoriai kontroliuoja 98% rinkos; (2) nepakankama numerio perkėlimo paslauga dėl ilgų terminų ir administracinės naštos; (3) skirtingos didmeninės sąlygos MVNO operatoriams. KT rekomendavo RRT (Ryšių reguliavimo tarnybai) stiprinti reguliavimą ir supaprastinti numerio perkėlimo procedūras.", outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["23"]), status: "final" },
  { case_number: "2024/02/03-4", title: "Farmacijos platintojas — atsisakymas patiekti vaistus mažesnėms vaistinėms", date: "2024-03-05", type: "abuse_of_dominance", sector: "pharmaceutical", parties: JSON.stringify(["Tamro UAB"]), summary: "KT tyrė vaistų platintojo Tamro UAB atsisakymą tiekti vaistus mažesnėms nepriklausomoms vaistinėms, vertindamas ar tai sudaro piktnaudžiavimą dominuojančia padėtimi.", full_text: "KT pradėjo tyrimą dėl galimo Tamro UAB piktnaudžiavimo dominuojančia padėtimi farmacijos platinimo rinkoje. Tamro UAB yra vienas didžiausių farmacijos platintojų Lietuvoje. Mažesnės nepriklausomos vaistinės skundėsi, kad Tamro atsisakė tiekti vaistus, arba taikė diskriminacines tiekimo sąlygas lyginant su didesniais vaistinių tinklais. KT tyrimo metu Tamro prisiėmė įsipareigojimus nediskriminuoti mažesnių vaistinių ir tiekti vaistus nediskriminacinėmis sąlygomis.", outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["9", "10"]), status: "final" },
];

const id = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const d of decisions) id.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); })();
console.log(`Inserted ${decisions.length} decisions`);

const mergers = [
  { case_number: "KT-M-2023-0045", title: "Mažmeninės prekybos tinklo įsigijimas iš skandinavų investuotojų", date: "2023-10-16", sector: "retail", acquiring_party: "Maxima LT UAB", target: "Norfa Distribution UAB", summary: "KT patvirtino koncentraciją I etapu be sąlygų, nustačius, kad šalys turi ribotą geografinį prekiavimosi vietų persidengimą.", full_text: "KT nagrinėjo koncentraciją, kur Maxima LT įsigijo Norfa Distribution tinklo parduotuves. Maxima veikia daugiau nei 230 mažmeninės prekybos vietose Lietuvoje. Norfa turi 26 parduotuves regionuose. KT atliko lokalinius rinkų tyrimus ir nustatė, kad šalių parduotuvės perkasi tik keliose vietovėse, nesukuriant horizontalių problemų. Koncentracija patvirtinta be sąlygų I etapu.", outcome: "cleared_phase1", turnover: 1_200_000_000 },
  { case_number: "KT-M-2023-0028", title: "Telekomunikacijų infrastruktūros sandoris", date: "2023-06-30", sector: "telecommunications", acquiring_party: "Telia Lietuva AB", target: "Baltik Bitė antenų tinklas", summary: "KT patvirtino su sąlygomis telekomunikacijų bokštų infrastruktūros įsigijimą, reikalaudamas neprasmingumo prieigą alternatyvių operatorių.", full_text: "KT nagrinėjo sandorį, kuriuo Telia Lietuva perima telekomunikacijų bokštų infrastruktūrą iš Bite Lietuva. Telia jau valdo didelę dalį telekomunikacijų bokštų infrastruktūros Lietuvoje. KT identifikavo rinkos galios problemas — Telia galėtų atsisakyti suteikti prieigą konkurencinėms operatoriams prie esminės infrastruktūros. KT patvirtino sandorį su sąlyga, kad Telia užtikrins nediskriminuojamą prieigą alternatyviniams operatoriams prie visų perimtų bokštų.", outcome: "cleared_with_conditions", turnover: 850_000_000 },
  { case_number: "KT-M-2022-0061", title: "Transporto logistikos grupė — krovinių vežimo įmonės įsigijimas", date: "2022-12-14", sector: "transport", acquiring_party: "Girteka Logistics UAB", target: "Kelprojektas Logistics UAB", summary: "KT patvirtino I etapu krovinių vežimo įmonės įsigijimą, nenustatęs reikšmingo rinkos dalių persidengimo krovinių vežimo paslaugų rinkose.", full_text: "KT nagrinėjo koncentraciją Girteka Logistics — vienas didžiausių krovinių vežėjų Europoje — įsigijant Kelprojektas Logistics. Abi bendrovės veikia krovinių vežimo tarptautinėmis keliais rinkose. KT atliko analizę pagal produktų ir geografines rinkas ir nustatė, kad šalys naudoja skirtingus maršrutus ir serviso segmentus. Koncentracija patvirtinta be sąlygų I etapu.", outcome: "cleared_phase1", turnover: 1_800_000_000 },
];

const im = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const m of mergers) im.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); })();
console.log(`Inserted ${mergers.length} mergers`);

const dCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nDatabase summary:\n  Sectors:   ${sCount}\n  Decisions: ${dCount}\n  Mergers:   ${mCount}\n\nDone. Database ready at ${DB_PATH}`);
db.close();
