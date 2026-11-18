/*
 * What the Databases screens say about a finding, kept apart from React.
 *
 * Every word here comes from docs/db-detection.md: the engine titles from
 * section 3, the coverage levels from section 5 and the trade-offs from the
 * section 6 table. They live in one module so the card, the list and the
 * badges cannot describe the same finding three ways, and so the parts that
 * decide something (which coverage is alarming, how a scope splits into a
 * provider and a unit, how a page groups) are tested without a DOM.
 */

import type { DbCoverage, DbSuggestion, WireDbFinding } from './wire'

/* ---------------------------------------------------------------- */
/* Engines                                                           */
/* ---------------------------------------------------------------- */

export const ENGINE_TITLES: Record<string, string> = {
  postgres: 'PostgreSQL',
  mysql: 'MySQL/MariaDB',
  sqlite: 'SQLite',
  redis: 'Redis/Valkey',
  prometheus: 'Prometheus',
  influxdb1: 'InfluxDB 1',
  influxdb2: 'InfluxDB 2',
  mongodb: 'MongoDB',
  elasticsearch: 'Elasticsearch/OpenSearch',
  leveldb: 'LevelDB',
  rocksdb: 'RocksDB',
  lmdb: 'LMDB',
  bbolt: 'bbolt',
  clickhouse: 'ClickHouse',
  nextcloud: 'Nextcloud',
}

/* The filter offers these in the daemon's order. An engine a newer daemon
 * adds still renders, under its own name, rather than vanishing. */
export const ENGINES = Object.keys(ENGINE_TITLES)

export function engineTitle(engine: string): string {
  return ENGINE_TITLES[engine] ?? engine
}

/* ---------------------------------------------------------------- */
/* Coverage                                                          */
/* ---------------------------------------------------------------- */

export const COVERAGES: DbCoverage[] = [
  'uncovered', 'split', 'engine-unsafe',
  'covered-crash', 'covered-quiesced', 'covered-application',
]

/*
 * How loudly to show a coverage level.
 *
 * alarm is the question section 12 says the screen exists to answer: a live
 * database that is not consistently captured. caution is consistent but
 * with a caveat worth reading; ok needs nothing; none is a finding nobody
 * assessed (dormant files are consistent by definition, and a hint has no
 * files to assess).
 */
export type Severity = 'alarm' | 'caution' | 'ok' | 'none'

export function coverageSeverity(c: DbCoverage | null | undefined): Severity {
  switch (c) {
    case 'uncovered':
    case 'split':
      return 'alarm'
    case 'engine-unsafe':
      return 'caution'
    case 'covered-crash':
    case 'covered-quiesced':
    case 'covered-application':
      return 'ok'
    default:
      return 'none'
  }
}

/* engine-unsafe counts as not covered: the snapshot is atomic but the
 * engine cannot be trusted to recover from it (MyISAM, Elasticsearch), so
 * it belongs with the other two when somebody asks "what is at risk". */
export function needsAttention(c: DbCoverage | null | undefined): boolean {
  return c === 'uncovered' || c === 'split' || c === 'engine-unsafe'
}

export const COVERAGE_LABELS: Record<DbCoverage, string> = {
  'covered-crash': 'crash-consistent',
  'covered-quiesced': 'quiesced',
  'covered-application': 'application-consistent',
  'engine-unsafe': 'engine-unsafe',
  split: 'split capture',
  uncovered: 'uncovered',
}

export function coverageLabel(c: DbCoverage | null | undefined): string {
  if (!c) return 'not assessed'
  return COVERAGE_LABELS[c] ?? c
}

/* Section 5, in a line each, for a finding whose coverage_why is absent. */
const COVERAGE_MEANING: Record<DbCoverage, string> = {
  'covered-crash': 'captured at one instant: restores as after a power cut, and the engine recovers',
  'covered-quiesced': 'captured at one instant with the guest frozen',
  'covered-application': 'captured at one instant after a hook quiesced or dumped it',
  'engine-unsafe': 'captured at one instant, but the engine is not crash-safe for all of its data',
  split: "the engine's files span captures taken at different instants",
  uncovered: 'read live: the copy may be torn',
}

export function coverageWords(f: Pick<WireDbFinding, 'coverage' | 'coverage_why' | 'confidence'>): string {
  if (f.coverage_why) return f.coverage_why
  if (f.coverage) return COVERAGE_MEANING[f.coverage] ?? ''
  if (f.confidence === 'dormant') return 'the files were at rest: any capture of them is consistent'
  if (f.confidence === 'hint') return 'no data directory was found in the backed-up paths'
  return ''
}

export const METHOD_LABELS: Record<string, string> = {
  snapshot: 'atomic snapshot',
  'paused-read': 'read while paused',
  'live-read': 'read live',
}

export function methodLabel(m: string | null | undefined): string {
  if (!m) return ''
  return METHOD_LABELS[m] ?? m
}

/* ---------------------------------------------------------------- */
/* Confidence                                                        */
/* ---------------------------------------------------------------- */

export const CONFIDENCE_MEANING: Record<string, string> = {
  live: 'data files and a sign the engine is running',
  dormant: 'data files, nothing running',
  hint: 'an image or port suggests it; no data files found',
}

/* ---------------------------------------------------------------- */
/* Suggestions                                                       */
/* ---------------------------------------------------------------- */

/*
 * The one-line trade-off for each suggestion, against the atomic snapshot
 * the section 6 table compares everything with. Short enough for a card;
 * the Manual's Databases chapter carries the full table.
 */
export const SUGGESTIONS: Record<DbSuggestion, { title: string; tradeoff: string }> = {
  'pg-dumpall': {
    title: 'pg_dumpall dump hook',
    tradeoff: 'transactional and restores into any later version; minutes per GB, dedups poorly',
  },
  'mariadb-dump': {
    title: 'mariadb-dump dump hook',
    tradeoff: 'transactional for InnoDB and portable across versions; minutes per GB, dedups poorly',
  },
  'redis-rdb': {
    title: 'Redis RDB dump',
    tradeoff: 'the dataset as of the dump, in seconds; an RDB dedups poorly',
  },
  mongodump: {
    title: 'mongodump dump hook',
    tradeoff: 'per-collection logical copy, portable; minutes per GB, dedups poorly',
  },
  'sqlite-backup': {
    title: 'SQLite online backup',
    tradeoff: 'a transactional standalone copy, about a second per 100 MB; dedups well',
  },
  'influx2-backup': {
    title: 'influx backup',
    tradeoff: 'consistent and restorable with influx restore; minutes, needs the operator token',
  },
  'prometheus-snapshot': {
    title: 'Prometheus snapshot API',
    tradeoff: 'consistent hard-linked blocks, no extra space; needs --web.enable-admin-api',
  },
  'exclude-rebuild': {
    title: 'Exclude and rebuild',
    tradeoff: 'nothing stored; the index has to be rebuilt after a restore',
  },
  'pause-always': {
    title: 'Pause the unit for every capture',
    tradeoff: 'one consistent capture of every file; the service stops for its duration',
  },
  'source-snapshot': {
    title: 'Source snapshot mode',
    tradeoff: 'one atomic capture of the source where the filesystem allows it',
  },
  'group-units': {
    title: 'Group the units',
    tradeoff: 'captured together as one instant; every grouped unit pauses together',
  },
}

export function suggestionTitle(s: string): string {
  return (SUGGESTIONS as Record<string, { title: string }>)[s]?.title ?? s
}

export function suggestionTradeoff(s: string): string {
  return (SUGGESTIONS as Record<string, { tradeoff: string }>)[s]?.tradeoff ?? ''
}

/* ---------------------------------------------------------------- */
/* Scopes                                                            */
/* ---------------------------------------------------------------- */

export type Scope =
  | { kind: 'unit'; key: string; provider: string; unit: string }
  | { kind: 'source'; path: string }
  | { kind: 'other'; raw: string }

/*
 * "unit:docker:compose:immich" is provider docker, unit compose:immich: the
 * unit part may itself hold colons, so only the first one after the prefix
 * separates. The detect route takes the two apart, which is why this has to
 * be exact.
 */
export function parseScope(scope: string): Scope {
  if (scope.startsWith('unit:')) {
    const key = scope.slice(5)
    const i = key.indexOf(':')
    if (i > 0 && i < key.length - 1) {
      return { kind: 'unit', key, provider: key.slice(0, i), unit: key.slice(i + 1) }
    }
    return { kind: 'other', raw: scope }
  }
  if (scope.startsWith('source:')) return { kind: 'source', path: scope.slice(7) }
  return { kind: 'other', raw: scope }
}

export function scopeTitle(s: Scope): string {
  if (s.kind === 'unit') return s.key
  if (s.kind === 'source') return s.path
  return s.raw
}

export interface ScopeGroup {
  scope: string
  parsed: Scope
  findings: WireDbFinding[]
}

/*
 * Findings grouped under their scope, in the order they arrived.
 *
 * The daemon orders by scope first, so on one page a scope's findings are
 * already together and this only cuts the list where the scope changes. It
 * still collects by key rather than by adjacency, so a page from a daemon
 * that ordered differently groups correctly instead of showing one scope
 * under two headings.
 */
export function groupByScope(findings: WireDbFinding[]): ScopeGroup[] {
  const groups = new Map<string, ScopeGroup>()
  for (const f of findings) {
    let g = groups.get(f.scope)
    if (!g) {
      g = { scope: f.scope, parsed: parseScope(f.scope), findings: [] }
      groups.set(f.scope, g)
    }
    g.findings.push(f)
  }
  return [...groups.values()]
}

/* Uncovered findings per scope, for the "uncovered" badge section 7.3 puts
 * on a unit or source. Dismissed ones are left out: the operator has
 * already decided, and the badge is what is still waiting on a decision. */
export function uncoveredByScope(findings: WireDbFinding[]): Map<string, number> {
  const out = new Map<string, number>()
  for (const f of findings) {
    if (f.state === 'dismissed' || f.coverage !== 'uncovered') continue
    out.set(f.scope, (out.get(f.scope) ?? 0) + 1)
  }
  return out
}

/* ---------------------------------------------------------------- */
/* Evidence                                                          */
/* ---------------------------------------------------------------- */

export interface EvidenceLine {
  label: string
  value: string
}

const isObj = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v)

const str = (v: unknown): string => {
  if (v === null || v === undefined) return ''
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  return JSON.stringify(v)
}

/* An array of objects, each rendered by fmt; anything else in it, or an
 * array of something else, falls back to the compact dump. */
function listOf(v: unknown, fmt: (o: Record<string, unknown>) => string): string {
  if (!Array.isArray(v)) return compact(v)
  return v.map(x => (isObj(x) ? fmt(x) : str(x))).join(', ')
}

/* key=value pairs, one level deep, for keys nothing here knows. */
export function compact(v: unknown): string {
  if (Array.isArray(v)) return v.map(x => (isObj(x) ? compact(x) : str(x))).join(', ')
  if (isObj(v)) {
    return Object.entries(v).map(([k, x]) => `${k}=${isObj(x) || Array.isArray(x) ? JSON.stringify(x) : str(x)}`).join(' ')
  }
  return str(v)
}

/*
 * The known evidence keys, each with a label and a formatter. Their order
 * is the order on the card: what was found on disk first, then what shows
 * the engine running, then what merely hints.
 */
const KNOWN: [string, string, (v: unknown) => string][] = [
  ['markers', 'Markers', v => (Array.isArray(v) ? v.map(str).join(', ') : compact(v))],
  ['flavour', 'Flavour', str],
  ['journal_mode', 'Journal mode', str],
  ['unsettled', 'Changed during read', v => `${str(v)} file${v === 1 ? '' : 's'}`],
  ['open_write', 'Open for writing', v => listOf(v, o => `${str(o.path)} (${str(o.comm)})`)],
  ['processes', 'Processes', v => listOf(v, o => `${str(o.comm)} in ${str(o.container)}`)],
  ['links', 'Links', v => listOf(v, o =>
    `${str(o.kind)} -> ${str(o.target)}${o.found === false ? ' (not found)' : ''}`)],
  ['link', 'Database link', v => {
    if (!isObj(v)) return compact(v)
    const { dbhost, resolved, consistent, ...rest } = v
    const parts = [`dbhost ${str(dbhost)}`]
    if (resolved !== undefined) parts.push(`resolved ${str(resolved)}`)
    if (consistent !== undefined) parts.push(consistent ? 'captured consistently' : 'not captured consistently')
    const more = compact(rest)
    return more ? `${parts.join(', ')}; ${more}` : parts.join(', ')
  }],
  ['ports', 'Ports', v => listOf(v, o => `${str(o.port)} in ${str(o.container)}`)],
  ['image', 'Image', v => (isObj(v) ? `${str(v.image)} (${str(v.container)})` : str(v))],
  ['images', 'Images', v => listOf(v, o => `${str(o.image)} (${str(o.container)})`)],
  ['also', 'Also', v => (Array.isArray(v) ? v.map(str).join(', ') : compact(v))],
]

const KNOWN_KEYS = new Set(KNOWN.map(([k]) => k))

export function evidenceLines(evidence: Record<string, unknown> | null | undefined): EvidenceLine[] {
  if (!isObj(evidence)) return []
  const out: EvidenceLine[] = []
  for (const [key, label, fmt] of KNOWN) {
    if (!(key in evidence)) continue
    const value = fmt(evidence[key])
    if (value) out.push({ label, value })
  }
  for (const [key, v] of Object.entries(evidence)) {
    if (KNOWN_KEYS.has(key)) continue
    const value = compact(v)
    if (value) out.push({ label: key, value })
  }
  return out
}
