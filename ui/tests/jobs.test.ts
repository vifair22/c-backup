/*
 * A finished job's warnings, as the task list shows them (B241). Pure
 * functions from shared/jobs.ts, under node:test like the others.
 */

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { jobWarnings, jobDisplayState } from '../src/shared/jobs.ts'

test('the warnings list, each one as raised', () => {
  assert.deepEqual(jobWarnings({ result: { warning: 'a; b', warnings: ['a', 'b'] } }), ['a', 'b'])
})

test('the joined line when a result has no list', () => {
  assert.deepEqual(jobWarnings({ result: { warning: 'only this' } }), ['only this'])
  assert.deepEqual(jobWarnings({ result: { warning: 'joined', warnings: [] } }), ['joined'])
})

test('none, and never anything that is not text', () => {
  assert.deepEqual(jobWarnings({ result: null }), [])
  assert.deepEqual(jobWarnings({}), [])
  assert.deepEqual(jobWarnings({ result: { warning: '', warnings: [3, null, ''] } }), [])
})

test('a completed job with warnings is its own state', () => {
  assert.equal(jobDisplayState({ state: 'completed', result: { warnings: ['x'] } }), 'warning')
  assert.equal(jobDisplayState({ state: 'completed', result: { files: 3 } }), 'completed')
  /* A failed job's warnings do not change what it is. */
  assert.equal(jobDisplayState({ state: 'failed', result: { warnings: ['x'] } }), 'failed')
  assert.equal(jobDisplayState({ state: 'running', result: null }), 'running')
})

/* B283: the per-repository task list asks the daemon for that repository's
 * jobs instead of cutting the daemon-wide newest 50 itself. */
test('a repository-scoped job list carries ?repo=', async () => {
  const { toEndpoint } = await import('../src/main/client.ts')
  assert.equal(toEndpoint({ kind: 'jobList', repoId: 7 }).path, '/jobs?repo=7')
  assert.equal(toEndpoint({ kind: 'jobList' }).path, '/jobs')
})
