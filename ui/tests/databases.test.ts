/*
 * The words and groupings the Databases screens put on a finding.
 *
 * Pure functions from shared/databases.ts, so node:test for the reason
 * contract.test.ts gives. What matters most here is what decides something:
 * a unit key splitting into the provider and unit the detect route takes,
 * which coverage levels are shown as alarming, and evidence the UI does not
 * know still reaching the screen.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  parseScope, scopeTitle, groupByScope, uncoveredByScope, coverageSeverity, needsAttention,
  coverageLabel, coverageWords, engineTitle, suggestionTitle, suggestionTradeoff,
  evidenceLines, compact, methodLabel, SUGGESTIONS, ENGINES,
} from '../src/shared/databases.ts'
import type { WireDbFinding } from '../src/shared/wire.ts'

function finding(over: Partial<WireDbFinding>): WireDbFinding {
  return {
    id: 1, scope: 'unit:docker:compose:immich', engine: 'postgres', version: '16',
    path: '/mnt/cache/appdata/immich/pgdata', confidence: 'live', coverage: 'covered-crash',
    coverage_why: null, method: 'snapshot', suggestion: null, optional: null, state: 'new',
    evidence: null, first_seen: 1, last_seen: 2, last_snap_id: null, alerted_at: null,
    template: null, params: null, decided_by: null, decided_at: null, reason: null,
    ...over,
  }
}

test('a unit scope splits at the first colon only', () => {
  assert.deepEqual(parseScope('unit:docker:compose:immich'),
    { kind: 'unit', key: 'docker:compose:immich', provider: 'docker', unit: 'compose:immich' })
  assert.deepEqual(parseScope('unit:lxc:web'),
    { kind: 'unit', key: 'lxc:web', provider: 'lxc', unit: 'web' })
})

test('a source scope keeps its whole path, colons included', () => {
  assert.deepEqual(parseScope('source:/mnt/user/appdata'), { kind: 'source', path: '/mnt/user/appdata' })
  assert.deepEqual(parseScope('source:/odd:name'), { kind: 'source', path: '/odd:name' })
})

test('a scope that cannot be split is kept whole rather than guessed at', () => {
  for (const s of ['unit:docker', 'unit::x', 'unit:docker:', 'vm:thing', '']) {
    const p = parseScope(s)
    assert.equal(p.kind, 'other', s)
    assert.equal(scopeTitle(p), s)
  }
  assert.equal(scopeTitle(parseScope('unit:docker:compose:immich')), 'docker:compose:immich')
  assert.equal(scopeTitle(parseScope('source:/srv')), '/srv')
})

test('grouping keeps arrival order and never splits one scope in two', () => {
  const page = [
    finding({ id: 1, scope: 'unit:docker:a' }),
    finding({ id: 2, scope: 'unit:docker:a' }),
    finding({ id: 3, scope: 'source:/srv' }),
    finding({ id: 4, scope: 'unit:docker:a' }),
  ]
  const g = groupByScope(page)
  assert.deepEqual(g.map(x => x.scope), ['unit:docker:a', 'source:/srv'])
  assert.deepEqual(g[0].findings.map(f => f.id), [1, 2, 4])
  assert.equal(g[0].parsed.kind, 'unit')
  assert.deepEqual(groupByScope([]), [])
})

test('the uncovered badge counts only undismissed uncovered findings', () => {
  const m = uncoveredByScope([
    finding({ scope: 'source:/a', coverage: 'uncovered' }),
    finding({ scope: 'source:/a', coverage: 'uncovered' }),
    finding({ scope: 'source:/a', coverage: 'uncovered', state: 'dismissed' }),
    finding({ scope: 'source:/a', coverage: 'split' }),
    finding({ scope: 'source:/b', coverage: 'covered-crash' }),
  ])
  assert.equal(m.get('source:/a'), 2)
  assert.equal(m.has('source:/b'), false)
})

test('uncovered and split are alarms; engine-unsafe is a caution; covered is fine', () => {
  assert.equal(coverageSeverity('uncovered'), 'alarm')
  assert.equal(coverageSeverity('split'), 'alarm')
  assert.equal(coverageSeverity('engine-unsafe'), 'caution')
  for (const c of ['covered-crash', 'covered-quiesced', 'covered-application'] as const) {
    assert.equal(coverageSeverity(c), 'ok')
    assert.equal(needsAttention(c), false)
  }
  assert.equal(coverageSeverity(null), 'none')
  assert.equal(needsAttention('engine-unsafe'), true)
  assert.equal(needsAttention(null), false)
})

test('coverage in words prefers the daemon, then section 5, then the confidence', () => {
  assert.equal(coverageWords(finding({ coverage_why: 'captured at one instant: x' })), 'captured at one instant: x')
  assert.match(coverageWords(finding({ coverage: 'uncovered' })), /torn/)
  assert.match(coverageWords(finding({ coverage: null, confidence: 'dormant' })), /at rest/)
  assert.match(coverageWords(finding({ coverage: null, confidence: 'hint' })), /no data directory/)
  assert.equal(coverageLabel(null), 'not assessed')
  assert.equal(coverageLabel('covered-crash'), 'crash-consistent')
})

test('every engine has a title, and an unknown one shows its own name', () => {
  assert.equal(ENGINES.length, 15)
  assert.equal(engineTitle('mysql'), 'MySQL/MariaDB')
  assert.equal(engineTitle('elasticsearch'), 'Elasticsearch/OpenSearch')
  assert.equal(engineTitle('duckdb'), 'duckdb')
  assert.equal(methodLabel('live-read'), 'read live')
  assert.equal(methodLabel(null), '')
})

test('every suggestion carries a title and a one-line trade-off', () => {
  for (const [k, v] of Object.entries(SUGGESTIONS)) {
    assert.ok(v.title.length > 0, k)
    assert.ok(v.tradeoff.length > 0 && !v.tradeoff.includes('\n'), k)
    assert.equal(suggestionTitle(k), v.title)
  }
  assert.equal(suggestionTitle('something-new'), 'something-new')
  assert.equal(suggestionTradeoff('something-new'), '')
})

test('known evidence keys render readably, in card order', () => {
  const lines = evidenceLines({
    ports: [{ container: 'immich_postgres', port: 5432 }],
    markers: ['PG_VERSION', 'global/pg_control', 'postmaster.pid'],
    processes: [{ container: 'immich_postgres', comm: 'postgres' }],
    open_write: [{ path: 'base/16384/2619', comm: 'postgres' }],
    unsettled: 1,
    links: [{ kind: 'pg_wal', target: '/mnt/disk2/wal', found: false }],
    image: { container: 'immich_postgres', image: 'postgres:16' },
  })
  assert.deepEqual(lines.map(l => l.label),
    ['Markers', 'Changed during read', 'Open for writing', 'Processes', 'Links', 'Ports', 'Image'])
  assert.equal(lines[0].value, 'PG_VERSION, global/pg_control, postmaster.pid')
  assert.equal(lines[1].value, '1 file')
  assert.equal(lines[2].value, 'base/16384/2619 (postgres)')
  assert.equal(lines[3].value, 'postgres in immich_postgres')
  assert.equal(lines[4].value, 'pg_wal -> /mnt/disk2/wal (not found)')
  assert.equal(lines[5].value, '5432 in immich_postgres')
  assert.equal(lines[6].value, 'postgres:16 (immich_postgres)')
})

test('the Nextcloud link says whether both halves were captured together', () => {
  const [l] = evidenceLines({ link: { dbhost: 'db', resolved: 'unit:docker:nc-db', consistent: false, via: 'config.php' } })
  assert.equal(l.label, 'Database link')
  assert.equal(l.value, 'dbhost db, resolved unit:docker:nc-db, not captured consistently; via=config.php')
})

test('unknown evidence keys still reach the screen, compactly', () => {
  const lines = evidenceLines({ journal_mode: 'wal', wal_bytes: 4096, extra: { a: 1, b: [2] } })
  assert.deepEqual(lines, [
    { label: 'Journal mode', value: 'wal' },
    { label: 'wal_bytes', value: '4096' },
    { label: 'extra', value: 'a=1 b=[2]' },
  ])
  assert.deepEqual(evidenceLines(null), [])
  assert.deepEqual(evidenceLines(undefined), [])
  /* A known key in a shape it was not expected in falls back, never throws. */
  assert.equal(evidenceLines({ processes: 'postgres' })[0].value, 'postgres')
  assert.equal(compact([{ a: 1 }, 'x']), 'a=1, x')
})
