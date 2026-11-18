/*
 * What the unit screens decide, without a DOM.
 *
 * Pure functions from shared/units.ts, under node:test for the reason
 * contract.test.ts gives. The merge is what matters most: a unit the
 * provider no longer lists must still show, with its history, or its
 * snapshots become unreachable from the app exactly when they are needed.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  mergeUnits, parseGlobs, unitJournal, mentions, isDisk, itemKindLabel,
  UNIT_LEVELS, DEFAULT_LEVEL, PAUSE_SCOPES,
} from '../src/shared/units.ts'
import type { WireJournalEntry, WireRepoUnit } from '../src/shared/wire.ts'

function hist(over: Partial<WireRepoUnit>): WireRepoUnit {
  return {
    unit_key: 'fake:a', provider: 'fake', unit_id: 'a', name: 'cbk-a', last_snapshot: 3,
    consistency: 'crash', snapshots: 2, last_ts: 100, ...over,
  }
}

function entry(over: Partial<WireJournalEntry>): WireJournalEntry {
  return { id: 1, ts: 1, source: 'api', operation: 'units', result: 'success',
    duration_ms: 5, message: null, ...over }
}

test('live units come first, matched to their history by key', () => {
  const rows = mergeUnits('fake', [
    { unit_key: 'fake:b', unit_id: 'b', name: 'cbk-b', state: 'running' },
    { unit_key: 'fake:a', unit_id: 'a', name: 'renamed', state: 'stopped' },
  ], [hist({})])
  assert.deepEqual(rows.map(r => [r.key, r.name, r.state]), [
    ['fake:b', 'cbk-b', 'running'],
    ['fake:a', 'renamed', 'stopped'],
  ])
  assert.equal(rows[0].history, null)
  assert.equal(rows[1].history?.last_snapshot, 3, 'matched by key, not by the name it had')
})

test('a unit with history the provider no longer lists still shows, without a state', () => {
  const rows = mergeUnits('fake', [
    { unit_key: 'fake:a', unit_id: 'a', name: 'cbk-a', state: 'running' },
  ], [hist({}), hist({ unit_key: 'fake:z', unit_id: 'z', name: 'b-gone' }),
      hist({ unit_key: 'fake:y', unit_id: 'y', name: 'a-gone' })])
  assert.deepEqual(rows.map(r => [r.name, r.state]), [
    ['cbk-a', 'running'], ['a-gone', null], ['b-gone', null],
  ])
})

test('a later page of the live list matches history but does not repeat the gone units', () => {
  const rows = mergeUnits('fake', [
    { unit_key: 'fake:a', unit_id: 'a', name: 'cbk-a', state: 'running' },
  ], [hist({}), hist({ unit_key: 'fake:z', unit_id: 'z', name: 'gone' })], false)
  assert.deepEqual(rows.map(r => r.name), ['cbk-a'])
  assert.equal(rows[0].history?.unit_key, 'fake:a')
})

test('history of another provider stays under its own provider', () => {
  const rows = mergeUnits('fake', [], [hist({ unit_key: 'docker:a', provider: 'docker' })])
  assert.deepEqual(rows, [])
})

test('exclude globs split on commas and lines, blanks dropped', () => {
  assert.deepEqual(parseGlobs('cbk-*, test-?\n\n  web '), ['cbk-*', 'test-?', 'web'])
  assert.deepEqual(parseGlobs('  ,\n'), [])
})

test('the levels are the daemon\'s, lowest first, and crash is the default', () => {
  assert.deepEqual(UNIT_LEVELS, ['inconsistent', 'crash', 'quiesced', 'application', 'offline'])
  assert.equal(DEFAULT_LEVEL, 'crash')
  assert.deepEqual(PAUSE_SCOPES, ['unit', 'group', 'job'])
})

test('a block item is a disk; the other kinds keep plain names', () => {
  assert.equal(isDisk('block'), true)
  for (const k of ['blob', 'tree', 'stream']) assert.equal(isDisk(k), false)
  assert.equal(itemKindLabel('block'), 'disk')
  assert.equal(itemKindLabel('tree'), 'data')
  assert.equal(itemKindLabel('blob'), 'config')
  assert.equal(itemKindLabel('novel'), 'novel', 'a kind a newer daemon adds still shows')
})

test('the unit journal keeps unit operations and marks the ones naming the unit', () => {
  const j = unitJournal([
    entry({ id: 5, operation: 'backup', message: 'snapshot 4: cbk-a' }),
    entry({ id: 4, operation: 'units', message: 'unit cbk-a: paused for 30 ms' }),
    entry({ id: 3, operation: 'units', message: 'fake: 2 unit(s) backed up, 0 failed' }),
    entry({ id: 2, operation: 'unit-restore', message: null }),
    entry({ id: 1, operation: 'nbd-export', message: 'export 1 of snapshot 3 item vda closed (revoked)' }),
    entry({ id: 0, operation: 'nbd-export', message: 'export 2 of snapshot 9 item vda closed (idle)' }),
  ], 'cbk-a', 'fake:a', new Set([3]))
  assert.deepEqual(j.map(e => [e.id, e.names]),
    [[4, true], [3, false], [2, false], [1, true], [0, false]],
    'an export names its snapshot, and only this unit\'s snapshots count')
})

test('a name is matched whole, never as part of a longer one', () => {
  assert.equal(mentions('unit cbk-uat-vm2: done', 'cbk-uat-vm'), false)
  assert.equal(mentions('unit cbk-uat-vm2 and cbk-uat-vm: done', 'cbk-uat-vm'), true)
  assert.equal(mentions('xcbk-uat-vm', 'cbk-uat-vm'), false)
  assert.equal(mentions('(cbk-uat-vm)', 'cbk-uat-vm'), true)
  assert.equal(mentions('anything', ''), false)
})
