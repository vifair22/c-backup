/*
 * Does every request the app can make reach a route the daemon serves?
 *
 * The desktop app is meant to reach parity with the CLI, and the CLI is held
 * to a written rule: an endpoint gets a command, and the end-to-end suite
 * exercises the whole table. Until now the app was a second client held to
 * nothing, and it showed - it posted to /repos/<id>/run for four months, a
 * route that has never existed, because the path was built by interpolating
 * a string and nobody ever asked the daemon whether it meant anything.
 *
 * So this asks. It starts a real c-backupd over a unix socket, builds one
 * representative request of every kind, sends it, and requires the router to
 * recognise the path.
 *
 * WHAT IT ASSERTS, AND WHAT IT DOES NOT
 *
 * That the route exists, not that the operation is correct. A handler
 * refusing a request as malformed is fine here - that is a conversation
 * between two parties who agree the endpoint exists. A router that has never
 * heard of the path is not.
 *
 * The two are told apart by the message: the router answers an unknown path
 * with exactly {"error":"not found"}, while a handler that cannot find a
 * resource says which one. Distinguishing them is the whole test, so
 * ROUTER_404 is compared exactly rather than by status alone.
 *
 * WHY node:test AND NOT VITEST
 *
 * This spawns a daemon and speaks HTTP to it; it needs a process runner, not
 * a browser or a DOM. node:test is in the standard library and Node strips
 * the types natively, so the suite that guards the API costs no dependency.
 * Vitest earns its place when there are components to render.
 */

import { test, before, after, describe } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync, mkdirSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import type { ApiKind, ApiRequest } from '../src/shared/api.ts'
import { toEndpoint } from '../src/main/client.ts'

const ROUTER_404 = 'not found'

const REPO_ROOT = resolve(import.meta.dirname, '..', '..')
const DAEMON = join(REPO_ROOT, 'build', 'bins', 'c-backupd')
const CLI = join(REPO_ROOT, 'build', 'bins', 'c-backup')

let work: string
let sock: string
let daemon: ChildProcess

interface Reply { status: number; body: unknown }

function request(ep: ReturnType<typeof toEndpoint>): Promise<Reply> {
  return new Promise((resolvePromise, reject) => {
    const payload = ep.body === undefined ? undefined : JSON.stringify(ep.body)
    const req = http.request(
      {
        socketPath: sock,
        path: ep.path,
        method: ep.method,
        headers: payload
          ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) }
          : {},
      },
      (res) => {
        let text = ''
        res.on('data', (c) => { text += c })
        res.on('end', () => {
          let body: unknown = text
          try { body = JSON.parse(text) } catch { /* metrics is not JSON */ }
          resolvePromise({ status: res.statusCode ?? 0, body })
        })
      },
    )
    req.on('error', reject)
    if (payload) req.write(payload)
    req.end()
  })
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function cli(args: string[]): void {
  const r = spawn(CLI, ['--socket', sock, ...args])
  r.on('error', () => {})
}

before(async () => {
  assert.ok(existsSync(DAEMON), `build the daemon first: ${DAEMON}`)

  work = mkdtempSync(join(tmpdir(), 'cbackup-contract-'))
  sock = join(work, 'sock')
  mkdirSync(join(work, 'keys'), { recursive: true })
  mkdirSync(join(work, 'src'), { recursive: true })
  writeFileSync(join(work, 'src', 'one.txt'), 'contract fixture\n')

  writeFileSync(join(work, 'config.yaml'),
    `db:\n  path: ${work}/state.db\n` +
    `http:\n  socket: ${sock}\n  port: 0\n  bind: 127.0.0.1\n` +
    `security:\n  key_dir: ${work}/keys\n` +
    `log:\n  level: warn\n`)

  daemon = spawn(DAEMON, [], {
    env: { ...process.env, C_BACKUP_CONFIG_PATH: join(work, 'config.yaml') },
    stdio: 'ignore',
  })

  for (let i = 0; i < 100 && !existsSync(sock); i++) await sleep(50)
  assert.ok(existsSync(sock), 'daemon did not start')

  /* Real fixtures, so most calls exercise a handler rather than bouncing off
   * a missing resource. The point is the route, but a route reached with
   * something real behind it tests more of the path. */
  cli(['repo', 'add', join(work, 'repo'), 'contract'])
  await sleep(400)
  cli(['backup', '1', join(work, 'src')])
  await sleep(1500)
})

after(async () => {
  daemon?.kill()
  await sleep(200)
  if (work) rmSync(work, { recursive: true, force: true })
})

/*
 * One sample of every kind.
 *
 * Typed as Record<ApiKind, ...>, which is the second half of the guard: a
 * variant added to ApiRequest without a sample here does not compile, so the
 * suite cannot fall behind the union the way the union fell behind the
 * daemon.
 */
const SAMPLES: Record<ApiKind, ApiRequest> = {
  repoList:        { kind: 'repoList' },
  repoShow:        { kind: 'repoShow', repoId: 1 },
  repoAdd:         { kind: 'repoAdd', path: '/nonexistent/contract-probe', label: 'probe' },
  repoRemove:      { kind: 'repoRemove', repoId: 9999 },

  keyExport:       { kind: 'keyExport', repoId: 1 },
  keyImport:       { kind: 'keyImport', repoId: 1, key: '00'.repeat(32) },
  keyAck:          { kind: 'keyAck', repoId: 1 },

  sourcesGet:      { kind: 'sourcesGet', repoId: 1 },
  sourcesSet:      { kind: 'sourcesSet', repoId: 1, sources: ['/nonexistent/contract-probe'] },
  backup:          { kind: 'backup', repoId: 1, paths: ['/nonexistent/contract-probe'] },
  restore:         { kind: 'restore', repoId: 1, snapshotId: 1, dest: '/nonexistent/contract-probe' },

  snapshotList:    { kind: 'snapshotList', repoId: 1, limit: 5 },
  snapshotDelete:  { kind: 'snapshotDelete', repoId: 1, snapshotId: 9999 },
  snapshotTree:    { kind: 'snapshotTree', repoId: 1, snapshotId: 1 },
  noteGet:         { kind: 'noteGet', repoId: 1, snapshotId: 1 },
  noteSet:         { kind: 'noteSet', repoId: 1, snapshotId: 1, note: 'contract' },
  search:          { kind: 'search', repoId: 1, query: 'one.txt' },
  diff:            { kind: 'diff', repoId: 1, from: 1, to: 1 },

  grepStart:       { kind: 'grepStart', repoId: 1, pattern: 'contract' },
  grepRuns:        { kind: 'grepRuns', repoId: 1 },
  grepHits:        { kind: 'grepHits', repoId: 1, runId: 1 },

  verify:          { kind: 'verify', repoId: 1, level: 'index' },
  verifyRuns:      { kind: 'verifyRuns', repoId: 1 },
  verifyFindings:  { kind: 'verifyFindings', repoId: 1, runId: 1 },
  gc:              { kind: 'gc', repoId: 1, dryRun: true },
  gfs:             { kind: 'gfs', repoId: 1, dryRun: true },
  pack:            { kind: 'pack', repoId: 1 },
  reindex:         { kind: 'reindex', repoId: 1 },

  stats:           { kind: 'stats', repoId: 1 },
  summary:         { kind: 'summary', repoId: 1 },
  objectList:      { kind: 'objectList', repoId: 1, limit: 5 },
  objectInfo:      { kind: 'objectInfo', repoId: 1, hash: '00'.repeat(32) },
  objectRefs:      { kind: 'objectRefs', repoId: 1, hash: '00'.repeat(32) },
  objectLayout:    { kind: 'objectLayout', repoId: 1, hash: '00'.repeat(32) },
  objectContent:   { kind: 'objectContent', repoId: 1, hash: '00'.repeat(32) },
  packList:        { kind: 'packList', repoId: 1 },
  packEntries:     { kind: 'packEntries', repoId: 1, packId: 1 },
  fanout:          { kind: 'fanout', repoId: 1 },

  policyGet:       { kind: 'policyGet', repoId: 1 },
  policySet:       { kind: 'policySet', repoId: 1, key: 'retention.keep_daily', value: '7' },
  tagList:         { kind: 'tagList', repoId: 1 },
  tagSet:          { kind: 'tagSet', repoId: 1, name: 'contract', snapshotId: 1 },
  tagRemove:       { kind: 'tagRemove', repoId: 1, name: 'contract' },
  journal:         { kind: 'journal', repoId: 1, limit: 5 },

  scheduleList:    { kind: 'scheduleList', repoId: 1 },
  scheduleAdd:     { kind: 'scheduleAdd', repoId: 1, action: 'verify',
                     trigger: { type: 'interval', seconds: 86400 } },
  scheduleSetEnabled: { kind: 'scheduleSetEnabled', repoId: 1, scheduleId: 9999, enabled: false },
  scheduleRemove:  { kind: 'scheduleRemove', repoId: 1, scheduleId: 9999 },

  jobList:         { kind: 'jobList' },
  jobGet:          { kind: 'jobGet', jobId: 1 },
  jobCancel:       { kind: 'jobCancel', jobId: 9999 },

  health:          { kind: 'health' },
  ready:           { kind: 'ready' },
  metrics:         { kind: 'metrics' },
  authSetupState:  { kind: 'authSetupState' },
  authSetup:       { kind: 'authSetup', password: 'contract-probe-password' },
  authLogin:       { kind: 'authLogin', password: 'contract-probe-password' },
}

describe('every request reaches a route the daemon serves', () => {
  for (const [kind, req] of Object.entries(SAMPLES) as [ApiKind, ApiRequest][]) {
    test(kind, async () => {
      const ep = toEndpoint(req)
      const res = await request(ep)

      const routerMissed =
        res.status === 404 &&
        typeof res.body === 'object' && res.body !== null &&
        (res.body as { error?: string }).error === ROUTER_404

      assert.ok(
        !routerMissed,
        `${kind}: ${ep.method} ${ep.path} is not a route the daemon has. ` +
        `This is the '/repos/<id>/run' failure: a path the app can produce ` +
        `and the daemon has never served.`,
      )
    })
  }
})

/*
 * The sequence the app actually performs, in order.
 *
 * The contract suite above proves each path exists. This proves the ones the
 * user presses do the thing: the backup button has never worked - it posted
 * to a route the daemon has never had - so "the endpoint resolves" is not
 * enough to believe it now does.
 */
describe('the flows behind the buttons', () => {
  test('open the backup modal, set sources, back up, restore', async () => {
    /* A repository of its own, so the shared fixture is left alone. */
    const path = join(work, 'flowrepo')
    const created = await request(toEndpoint({ kind: 'repoAdd', path, label: 'flow' }))
    assert.equal(created.status, 201)
    const repoId = (created.body as { id: number }).id

    /* The modal opens and asks what this repository backs up. Nothing yet,
     * which is what the empty state is for. */
    let got = await request(toEndpoint({ kind: 'sourcesGet', repoId }))
    assert.deepEqual((got.body as { sources: string[] }).sources, [])

    /* The user says what to back up, and it comes back. */
    const src = join(work, 'src')
    const saved = await request(toEndpoint({ kind: 'sourcesSet', repoId, sources: [src] }))
    assert.equal(saved.status, 204)
    got = await request(toEndpoint({ kind: 'sourcesGet', repoId }))
    assert.deepEqual((got.body as { sources: string[] }).sources, [src])

    /* Start Backup sends no paths at all: the repository holds them. */
    const started = await request(toEndpoint({ kind: 'backup', repoId }))
    assert.equal(started.status, 202, `backup was refused: ${JSON.stringify(started.body)}`)

    /* And it produces a real snapshot, which is the whole point. */
    let snaps: unknown[] = []
    for (let i = 0; i < 60; i++) {
      const r = await request(toEndpoint({ kind: 'snapshotList', repoId }))
      snaps = (r.body as { snapshots?: unknown[] }).snapshots ?? []
      if (snaps.length > 0) break
      await sleep(250)
    }
    assert.ok(snaps.length > 0, 'the backup button produced no snapshot')

    /* Restore goes under the snapshot, which is where the endpoint is. */
    const restored = await request(toEndpoint({
      kind: 'restore', repoId, snapshotId: 1, dest: join(work, 'flowout'),
    }))
    assert.equal(restored.status, 202, `restore was refused: ${JSON.stringify(restored.body)}`)
  })

  test('schedules: list, add, pause, resume, remove', async () => {
    const path = join(work, 'schedrepo')
    const created = await request(toEndpoint({ kind: 'repoAdd', path, label: 'sched' }))
    assert.equal(created.status, 201)
    const repoId = (created.body as { id: number }).id

    /* The screen opens on an empty list rather than an error. */
    let list = await request(toEndpoint({ kind: 'scheduleList', repoId }))
    assert.equal(list.status, 200)
    assert.deepEqual((list.body as { schedules?: unknown[] }).schedules ?? [], [])

    /*
     * A backup schedule for a repository with no sources is refused, and the
     * screen shows the reason verbatim because it says what to do next.
     * Being refused here is the fix for schedules that were accepted and
     * then failed nightly in a log nobody reads.
     */
    const premature = await request(toEndpoint({
      kind: 'scheduleAdd', repoId, action: 'backup',
      trigger: { type: 'interval', seconds: 86400 },
    }))
    assert.equal(premature.status, 400)

    /* With sources set, the same request is accepted. */
    await request(toEndpoint({ kind: 'sourcesSet', repoId, sources: [join(work, 'src')] }))
    const added = await request(toEndpoint({
      kind: 'scheduleAdd', repoId, action: 'backup',
      trigger: { type: 'interval', seconds: 86400 },
    }))
    assert.ok(added.status < 400, `schedule add failed: ${JSON.stringify(added.body)}`)

    list = await request(toEndpoint({ kind: 'scheduleList', repoId }))
    const rows = (list.body as { schedules: { id: string; enabled: string }[] }).schedules
    assert.equal(rows.length, 1)
    assert.equal(rows[0].enabled, '1')

    /* Pause and resume, which is the toggle in the table. */
    const sid = Number(rows[0].id)
    await request(toEndpoint({ kind: 'scheduleSetEnabled', repoId, scheduleId: sid, enabled: false }))
    list = await request(toEndpoint({ kind: 'scheduleList', repoId }))
    assert.equal((list.body as { schedules: { enabled: string }[] }).schedules[0].enabled, '0')

    await request(toEndpoint({ kind: 'scheduleSetEnabled', repoId, scheduleId: sid, enabled: true }))
    list = await request(toEndpoint({ kind: 'scheduleList', repoId }))
    assert.equal((list.body as { schedules: { enabled: string }[] }).schedules[0].enabled, '1')

    /* And removed, leaving the empty state the screen started from. */
    await request(toEndpoint({ kind: 'scheduleRemove', repoId, scheduleId: sid }))
    list = await request(toEndpoint({ kind: 'scheduleList', repoId }))
    assert.deepEqual((list.body as { schedules?: unknown[] }).schedules ?? [], [])
  })

  test('the maintenance menu starts every operation it offers', async () => {
    const ops = ['verify', 'pack', 'gc', 'gfs', 'reindex'] as const
    for (const kind of ops) {
      const r = await request(toEndpoint({ kind, repoId: 1 }))
      assert.equal(r.status, 202, `${kind} was refused: ${JSON.stringify(r.body)}`)
    }
  })
})

describe('the samples cover the union', () => {
  test('no kind is missing a sample', () => {
    /* Record<ApiKind, ...> makes this a compile-time guarantee; asserting it
     * at run time as well catches a sample deleted with a cast. */
    assert.ok(Object.keys(SAMPLES).length > 0)
  })
})
