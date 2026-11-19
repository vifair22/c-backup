/*
 * A real daemon, a real repository, and the app pointed at both.
 *
 * Nothing here is mocked. The screens under test read backup sources, verify
 * runs and encryption state, and every one of those is a thing the daemon
 * computes rather than a shape the app invents - a fixture that returned
 * plausible JSON would prove the components render, which was never the part
 * in doubt. The backup button rendered perfectly for four months while
 * posting to a route that did not exist.
 *
 * So: build the daemon, start it on a unix socket in a temp directory, put
 * real files through it, and seed the app's config so it connects to that
 * socket instead of whatever is on the developer's machine.
 */

import { _electron as electron, expect, type ElectronApplication, type Page } from '@playwright/test'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { createServer } from 'node:net'
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync, chmodSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

/* __dirname rather than import.meta.dirname: Playwright transpiles these to
 * CommonJS, where import.meta is a syntax error. */
export const REPO_ROOT = resolve(__dirname, '..', '..')
const DAEMON = join(REPO_ROOT, 'build', 'bins', 'c-backupd')
const CLI = join(REPO_ROOT, 'build', 'bins', 'c-backup')
const HELPER = join(REPO_ROOT, 'build', 'bins', 'c-backup-helper')
const APP_MAIN = join(REPO_ROOT, 'ui', 'out', 'main', 'index.js')

export interface HarnessOptions {
  /*
   * Files to write into one directory before the first backup.
   *
   * The default fixture is three files, which proves a screen renders and
   * proves nothing about what it does when a directory is bigger than a
   * page. Passing a number here builds a wide directory instead, so the
   * paging can be tested rather than assumed.
   */
  wideDir?: number

  /*
   * Serve the daemon over TCP as well, and point the app at that instead of
   * the socket.
   *
   * Every other spec connects over the unix socket, which needs no
   * credential, so none of them could notice that the remote path did not
   * work - and it did not, for the whole of its existence. This is what
   * makes the TCP path a tested one.
   *
   *   password  set as the admin password, over the socket, the only way a
   *             first password can be set. Omit it to leave the daemon
   *             unconfigured.
   *   seed      put the remote connection in the app's config, or leave the
   *             app with none so a spec can add it through the dialog.
   */
  remote?: { password?: string; seed: boolean }

  /*
   * Live databases for detection (docs/db-detection.md): a WAL-mode SQLite
   * with its -wal in the backed-up source, which the first backup finds
   * live and uncovered, and a fake-provider unit `cbk-uat-db` whose tree
   * holds another, so the unit scope and its Detect action have something
   * to find. Real SQLite files, so an engine copy can actually be made.
   */
  databases?: boolean

  /*
   * Units through the real helper (Manual 22.2), serving the fake provider
   * the way tests/test_helper_e2e.sh does: a VM-like unit with a disk, a
   * container-like one with a data tree on a snapshot domain (so an
   * in-place restore can stage and swap), and a unit outside units.allow
   * that must never show. Behind the helper rather than in-process because
   * a restore as a unit (define) only exists through the helper, and the
   * helper's fake backend refuses a define check with whatever a test
   * writes to its state directory, so a refusal travels the whole real
   * path. Paths: unitPaths(work).
   */
  units?: boolean

  /*
   * With units: a fake-provider unit `cbk-uat-pg` whose tree holds a live
   * PostgreSQL data directory, as tests/test_helper_e2e.sh makes one, and a
   * helper policy whose [db] leaves out pg-dumpall, the template its finding
   * suggests. Its finding is
   * enabled through the helper, so enabling it is refused with the policy
   * line to add (db-detection.md 7.2). The engine's tools are fakes on the
   * helper's PATH: the fake provider has no container to run them in.
   */
  unitDb?: boolean
}

/* Where the units option puts things, for specs to look at. */
export function unitPaths(work: string) {
  return {
    /* The container's data as the fake provider reads it (its snapshot). */
    lxcRead: join(work, 'lxc-read'),
    /* The container's data where it lives, on the dir snapshot domain. */
    lxcLive: join(work, 'pool', 'lxc', 'web', 'rootfs'),
    pool: join(work, 'pool'),
    helperState: join(work, 'hstate'),
    /* Written there, the helper's fake backend refuses a define check with
     * the file's text, as a real backend refuses. */
    refuse: join(work, 'hstate', 'fake-define-refuse'),
    exports: join(work, 'exports'),
  }
}

export interface Harness {
  app: ElectronApplication
  page: Page
  work: string
  sock: string
  /** Ids of the repositories seeded below, by label. */
  repos: { plain: number; encrypted: number }
  /** The daemon's TCP port, when the harness was started remote. */
  port: number
  /** The app's userData directory: config.json and credentials.json. */
  userData: string
  /** Kill the daemon, to see what the app does without one. */
  killDaemon(): void
  /** Close the app and start it again on the same userData, as a user
   * quitting and reopening it would. The daemon keeps running. */
  relaunch(): Promise<Page>
  close(): Promise<void>
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/*
 * An empty SQLite database in WAL mode, byte for byte: the 100-byte header
 * and an empty sqlite_master leaf page. Written rather than made with the
 * sqlite3 tool, which the CI image does not carry. integrity_check passes
 * on it and the online backup copies it.
 */
export function emptyWalSqlite(): Buffer {
  const b = Buffer.alloc(4096)
  b.write('SQLite format 3\0', 0, 'latin1')
  b.writeUInt16BE(4096, 16)
  b[18] = 2; b[19] = 2          /* WAL */
  b[21] = 64; b[22] = 32; b[23] = 32
  b.writeUInt32BE(1, 24)        /* change counter */
  b.writeUInt32BE(1, 28)        /* pages */
  b.writeUInt32BE(4, 44)        /* schema format */
  b.writeUInt32BE(1, 56)        /* UTF-8 */
  b.writeUInt32BE(1, 92)
  b.writeUInt32BE(3046000, 96)
  b[100] = 0x0d                 /* leaf table page, no cells */
  b.writeUInt16BE(4096, 105)
  return b
}

export function cli(sock: string, args: string[]): string {
  const r = spawnSync(CLI, ['--socket', sock, ...args], { encoding: 'utf-8' })
  return (r.stdout ?? '') + (r.stderr ?? '')
}

/* A port nothing is listening on, by asking the kernel for one. */
function freePort(): Promise<number> {
  return new Promise((done, fail) => {
    const srv = createServer()
    srv.once('error', fail)
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address()
      const port = typeof addr === 'object' && addr ? addr.port : 0
      srv.close(() => done(port))
    })
  })
}

function idFrom(out: string): number {
  const m = out.match(/"id":(\d+)/)
  return m ? Number(m[1]) : 0
}

export async function startHarness(opts: HarnessOptions = {}): Promise<Harness> {
  if (!existsSync(DAEMON)) throw new Error(`build the daemon first: ${DAEMON}`)
  if (!existsSync(APP_MAIN)) throw new Error(`run 'npm run build' first: ${APP_MAIN}`)

  const work = mkdtempSync(join(tmpdir(), 'cbackup-e2e-'))
  const sock = join(work, 'daemon.sock')
  const userData = join(work, 'userData')
  mkdirSync(join(work, 'keys'), { recursive: true })
  mkdirSync(userData, { recursive: true })

  /* Something worth looking at in a snapshot browser. */
  mkdirSync(join(work, 'src', 'docs'), { recursive: true })
  writeFileSync(join(work, 'src', 'notes.txt'), 'the quick brown fox\n'.repeat(40))
  writeFileSync(join(work, 'src', 'docs', 'readme.md'), '# demo\n\nA fixture file.\n')

  if (opts.wideDir) {
    mkdirSync(join(work, 'src', 'wide'), { recursive: true })
    for (let i = 0; i < opts.wideDir; i++) {
      writeFileSync(join(work, 'src', 'wide', `f${String(i).padStart(5, '0')}.txt`),
                    `entry ${i}\n`)
    }
  }

  /*
   * The helper refuses daemon_uid 0 by design and the CI job runs as root,
   * so there the daemon runs as a fixed non-root uid that owns the work
   * tree, and the policy stays root's, as the helper requires.
   */
  const asRoot = typeof process.getuid === 'function' && process.getuid() === 0
  const duid = asRoot ? 990 : (process.getuid?.() ?? 0)
  const dgid = asRoot ? 990 : (process.getgid?.() ?? 0)
  const hsock = join(work, 'h.sock')
  if (opts.units) {
    const u = unitPaths(work)
    mkdirSync(join(u.lxcRead, 'etc'), { recursive: true })
    writeFileSync(join(u.lxcRead, 'etc', 'motd'), 'restored from the snapshot\n')
    mkdirSync(join(u.lxcLive, 'etc'), { recursive: true })
    writeFileSync(join(u.lxcLive, 'etc', 'motd'), 'the live copy\n')
    mkdirSync(u.helperState, { recursive: true })
    writeFileSync(join(work, 'units.json'), JSON.stringify({ units: [
      { id: 'vm1', name: 'cbk-uat-vm', state: 'running', caps: ['quiesce', 'snapshot'],
        blobs: [{ id: 'definition', path: '.cbackup-unit/config/domain.xml',
                  text: '<domain><name>cbk-uat-vm</name></domain>\n' }],
        disks: [{ id: 'vda', size: 4194304, runs: [{ off: 0, len: 1048576, kind: 'data', seed: 3 }] }] },
      { id: 'web', name: 'cbk-uat-web', state: 'running', caps: ['pause', 'snapshot'],
        blobs: [{ id: 'config', path: '.cbackup-unit/config/lxc/config', text: 'lxc.uts.name = web\n' }],
        trees: [{ id: 'rootfs', read_root: u.lxcRead, path: u.lxcLive, source: u.lxcLive }] },
      { id: 'x', name: 'other-thing', state: 'stopped' },
      ...(opts.unitDb ? [{ id: 'pg', name: 'cbk-uat-pg', state: 'running', caps: ['quiesce', 'snapshot'],
        trees: [{ id: 'pg', read_root: join(work, 'pgsnap'), path: join(u.pool, 'app', 'pg'),
                  source: join(u.pool, 'app', 'pg') }] }] : []),
    ] }))
    if (opts.unitDb) {
      const pg = join(work, 'pgsnap')
      mkdirSync(join(pg, 'global'), { recursive: true })
      mkdirSync(join(pg, 'base', '1'), { recursive: true })
      mkdirSync(join(u.pool, 'app', 'pg'), { recursive: true })
      mkdirSync(join(work, 'bin'), { recursive: true })
      writeFileSync(join(pg, 'PG_VERSION'), '16\n')
      writeFileSync(join(pg, 'global', 'pg_control'), Buffer.alloc(8192))
      writeFileSync(join(pg, 'base', '1', '1259'), Buffer.alloc(8192))
      writeFileSync(join(pg, 'postmaster.pid'), `42\n${pg}\n`)
      writeFileSync(join(work, 'bin', 'psql'), '#!/bin/sh\necho "PostgreSQL 16.4 (fake) $*"\n', { mode: 0o755 })
      writeFileSync(join(work, 'bin', 'pg_dumpall'), '#!/bin/sh\necho "-- fake dump"\n', { mode: 0o755 })
    }
    writeFileSync(join(work, 'policy.ini'),
      `[helper]\ndaemon_uid = ${duid}\ndaemon_gid = ${dgid}\ndaemon_id = 0badf00d\n` +
      `max_pause_s = 30\ncapture_max_s = 3600\nallow_restore = 1\nallow_define = 1\n` +
      `state_dir = ${u.helperState}\nsocket = ${hsock}\nfake_spec = ${join(work, 'units.json')}\n\n` +
      `[units]\nallow = fake:cbk-uat-*\n\n[snapshot]\ndir_domains = ${u.pool}\n` +
      /* Without [db] the stdout dumps are allowed; this leaves out the
       * pg-dumpall the finding suggests, so enabling it is refused. */
      (opts.unitDb ? `\n[db]\nallow_templates = pg-dump\n` : ''))
    chmodSync(join(work, 'policy.ini'), 0o600)
  }

  if (opts.databases) {
    mkdirSync(join(work, 'src', 'app'), { recursive: true })
    writeFileSync(join(work, 'src', 'app', 'live.db'), emptyWalSqlite())
    writeFileSync(join(work, 'src', 'app', 'live.db-wal'), '')
    mkdirSync(join(work, 'unit-snap'), { recursive: true })
    writeFileSync(join(work, 'unit-snap', 'grafana.db'), emptyWalSqlite())
    writeFileSync(join(work, 'unit-snap', 'grafana.db-wal'), '')
    writeFileSync(join(work, 'units.json'), JSON.stringify({ units: [{
      id: 'db', name: 'cbk-uat-db', state: 'running', caps: ['snapshot'],
      trees: [{ id: 'vol', read_root: join(work, 'unit-snap'), path: join(work, 'unit-live') }],
    }] }))
  }

  const port = opts.remote ? await freePort() : 0
  writeFileSync(join(work, 'config.yaml'),
    `db:\n  path: ${work}/state.db\n` +
    `http:\n  socket: ${sock}\n  port: ${port}\n  bind: 127.0.0.1\n` +
    `security:\n  key_dir: ${work}/keys\n` +
    `log:\n  level: warn\n` +
    (opts.databases ? `units:\n  allow: cbk-uat-*\n  fake_spec: ${work}/units.json\n` : '') +
    (opts.units ? `units:\n  allow: cbk-uat-*\n  helper_socket: ${hsock}\n` +
                  `vm:\n  export_dir: ${unitPaths(work).exports}\n` : ''))

  let helper: ChildProcess | null = null
  if (opts.units) {
    if (!existsSync(HELPER)) throw new Error(`build the helper first: ${HELPER}`)
    if (asRoot) {
      spawnSync('chown', ['-R', `${duid}:${dgid}`, work])
      spawnSync('chown', ['0:0', join(work, 'policy.ini')])
    }
    helper = spawn(HELPER, ['--policy', join(work, 'policy.ini')], {
      stdio: 'ignore',
      env: opts.unitDb ? { ...process.env, PATH: `${join(work, 'bin')}:${process.env.PATH ?? ''}` }
                       : process.env,
    })
    for (let i = 0; i < 120 && !existsSync(hsock); i++) await sleep(50)
    if (!existsSync(hsock)) throw new Error('helper did not start')
  }

  const daemon: ChildProcess = opts.units && asRoot
    ? spawn('setpriv', [`--reuid=${duid}`, `--regid=${dgid}`, '--clear-groups', '--', DAEMON], {
        env: { ...process.env, C_BACKUP_CONFIG_PATH: join(work, 'config.yaml') },
        stdio: 'ignore',
      })
    : spawn(DAEMON, [], {
        env: { ...process.env, C_BACKUP_CONFIG_PATH: join(work, 'config.yaml') },
        stdio: 'ignore',
      })
  for (let i = 0; i < 120 && !existsSync(sock); i++) await sleep(50)
  if (!existsSync(sock)) throw new Error('daemon did not start')
  if (opts.units) {
    /* Connected to the helper before any spec asks for a unit. */
    for (let i = 0; i < 100 && !cli(sock, ['unit', 'providers']).includes('"state":"connected"'); i++) {
      await sleep(100)
    }
  }

  /*
   * Two repositories, because several screens differ by encryption state:
   * the key screen has three shapes and an unencrypted repository is one of
   * them.
   */
  const plain = idFrom(cli(sock, ['repo', 'add', join(work, 'plainrepo'), 'Documents']))
  const encrypted = idFrom(cli(sock, ['repo', 'add', join(work, 'encrepo'), 'Photos',
                                      '--encrypt', 'system']))

  /* Configured sources, so the backup dialog has something to show, and a
   * couple of real snapshots so the browser is not empty. */
  cli(sock, ['sources', 'set', String(plain), join(work, 'src')])
  cli(sock, ['backup', String(plain)])
  writeFileSync(join(work, 'src', 'docs', 'second.md'), '# added later\n')
  cli(sock, ['backup', String(plain)])
  /*
   * Apply retention once. gfs_flags is written by this pass and not by
   * backup, so without it every snapshot shows an empty retention column
   * and the tier filters have nothing to filter.
   */
  /*
   * The shipped defaults keep only the newest snapshot of a day that has not
   * closed, so with them this pass would prune the older of the two backups
   * above, and the screens below need both. Retention is applied for its
   * flags here, not for what it removes.
   */
  cli(sock, ['policy', 'set', String(plain), 'retention.keep_snaps', '100'])
  cli(sock, ['gfs', String(plain)])
  /* One scrub, so the integrity screen has a run to open on. */
  cli(sock, ['verify', String(plain), '--level', 'deep'])

  /*
   * Seed the app's own config rather than clicking through Add Connection.
   * The connection dialog is worth testing on its own; every other screen
   * needs a connection to exist and should not re-test it.
   */
  if (opts.remote?.password) {
    const r = spawnSync(CLI, ['--socket', sock, 'auth', 'setup'],
                        { input: `${opts.remote.password}\n`, encoding: 'utf-8' })
    if (r.status !== 0) throw new Error(`auth setup failed: ${r.stderr}`)
  }

  const connections = !opts.remote
    ? [{ type: 'local', name: 'This machine', socketPath: sock }]
    : opts.remote.seed
      ? [{ type: 'remote', name: 'nas', host: '127.0.0.1', port }]
      : []
  writeFileSync(join(userData, 'config.json'), JSON.stringify({ connections }, null, 2))

  const launch = async (): Promise<{ app: ElectronApplication; page: Page }> => {
    const a = await electron.launch({
      args: [APP_MAIN, `--user-data-dir=${userData}`, '--no-sandbox'],
      env: { ...process.env, NODE_ENV: 'production' },
    })
    const p = await a.firstWindow()
    await p.waitForLoadState('domcontentloaded')
    return { app: a, page: p }
  }
  const first = await launch()

  const h: Harness = {
    app: first.app, page: first.page, work, sock, port, userData,
    repos: { plain, encrypted },
    killDaemon() { daemon.kill('SIGKILL') },
    async relaunch() {
      await h.app.close().catch(() => {})
      const next = await launch()
      h.app = next.app
      h.page = next.page
      return next.page
    },
    async close() {
      await h.app.close().catch(() => {})
      daemon.kill()
      helper?.kill()
      await sleep(200)
      rmSync(work, { recursive: true, force: true })
    },
  }
  return h
}

/*
 * Open a repository and reach one of its screens.
 *
 * By visible text rather than by test id: the buttons are the operator's
 * route to these screens, so a rename that breaks the path a person takes
 * should break the test too.
 */
export async function openRepo(page: Page, label: string): Promise<void> {
  /* The connection is expanded already, so clicking it collapses the list
   * and hides the repository this is trying to reach. Go straight to the
   * repository. */
  await page.getByText(label, { exact: true }).first().click()
  await page.waitForTimeout(600)
}

/* ------------------------------------------------------------------ */
/* Timing                                                              */
/* ------------------------------------------------------------------ */

/*
 * How long an interaction takes, from the click to the answer being on
 * screen.
 *
 * Measured rather than judged. Everything here talks to a daemon over a unix
 * socket on the same machine, so a screen that feels slow is the app's doing:
 * a request that should have been parallel, a refetch that should have been
 * cached, or a render waiting on data it did not need. Those are invisible
 * until somebody puts a number on them.
 *
 * The budget is deliberately not per-screen. A user does not know which
 * endpoint a page uses; they know whether the app answered.
 */
export const BUDGET_MS = {
  /* Moving around inside a repository. Nothing here does real work. */
  navigation: 400,
  /* Opening a dialog that has to read something first. */
  dialog: 500,
  /* Starting a job. The work is asynchronous by design, so this measures
   * the handoff, not the backup. */
  submit: 600,
}

export interface Timing { label: string; ms: number; budget: number }

export const timings: Timing[] = []

export async function timed(
  label: string,
  budget: number,
  action: () => Promise<void>,
): Promise<number> {
  const ms = await clock(action)
  record(label, ms, budget)
  return ms
}

/* How long an action takes, without putting it in the report. For a figure
 * built from several runs, which is recorded once as the figure. */
export async function clock(action: () => Promise<void>): Promise<number> {
  const started = Date.now()
  await action()
  return Date.now() - started
}

export function record(label: string, ms: number, budget: number): void {
  timings.push({ label, ms, budget })
}

/*
 * The middle of several runs.
 *
 * One sample on a shared CI runner measures the runner as much as the app:
 * the ui-e2e job runs alongside other heavy jobs, and a single slow
 * scheduling slice turned a 250 ms search into 631 ms (FLAKE-11). The median
 * of five ignores up to two such slices, and the first, cold run with them.
 */
export function median(samples: number[]): number {
  const s = [...samples].sort((a, b) => a - b)
  const mid = s.length >> 1
  return s.length % 2 ? s[mid] : Math.round((s[mid - 1] + s[mid]) / 2)
}

/* Printed as a table at the end of a run, so a regression is visible as a
 * number moving rather than as a test that happens to still pass. */
export function reportTimings(): void {
  if (timings.length === 0) return
  const width = Math.max(...timings.map(t => t.label.length))
  process.stdout.write('\n  interaction timings\n')
  for (const t of timings) {
    const flag = t.ms > t.budget ? '  OVER BUDGET' : ''
    process.stdout.write(
      `    ${t.label.padEnd(width)}  ${String(t.ms).padStart(5)} ms  (budget ${t.budget})${flag}\n`)
  }
  process.stdout.write('\n')
}

/*
 * Walk the tree down to the fixture's source directory.
 *
 * scan_tree records absolute paths, so a snapshot of /tmp/cbackup-e2e-X/src
 * has directory entries for /tmp and for the work directory above it. The
 * first version of this clicked "the last row that looks like a directory"
 * and slept 100 ms between levels, which passed alone and flaked under a
 * full run: a slow level meant the next click landed on the wrong row.
 *
 * Named segments and a wait on each level's contents instead. Slower to
 * write, deterministic to run.
 */
export async function descendTo(page: Page, work: string, ...rest: string[]): Promise<void> {
  const segments = ['tmp', work.split('/').pop()!, 'src', ...rest]
  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i]
    await page.getByText(seg, { exact: true }).first().click()
    /* The level is open once the next segment appears, or - for the last
     * one - once any child row has rendered. */
    const next = segments[i + 1]
    if (next) await expect(page.getByText(next, { exact: true }).first()).toBeVisible()
  }
}
