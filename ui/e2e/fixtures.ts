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

import { _electron as electron, type ElectronApplication, type Page } from '@playwright/test'
import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { mkdtempSync, mkdirSync, writeFileSync, existsSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

/* __dirname rather than import.meta.dirname: Playwright transpiles these to
 * CommonJS, where import.meta is a syntax error. */
export const REPO_ROOT = resolve(__dirname, '..', '..')
const DAEMON = join(REPO_ROOT, 'build', 'bins', 'c-backupd')
const CLI = join(REPO_ROOT, 'build', 'bins', 'c-backup')
const APP_MAIN = join(REPO_ROOT, 'ui', 'out', 'main', 'index.js')

export interface Harness {
  app: ElectronApplication
  page: Page
  work: string
  sock: string
  /** Ids of the repositories seeded below, by label. */
  repos: { plain: number; encrypted: number }
  close(): Promise<void>
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function cli(sock: string, args: string[]): string {
  const r = spawnSync(CLI, ['--socket', sock, ...args], { encoding: 'utf-8' })
  return (r.stdout ?? '') + (r.stderr ?? '')
}

function idFrom(out: string): number {
  const m = out.match(/"id":"?(\d+)"?/)
  return m ? Number(m[1]) : 0
}

export async function startHarness(): Promise<Harness> {
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

  writeFileSync(join(work, 'config.yaml'),
    `db:\n  path: ${work}/state.db\n` +
    `http:\n  socket: ${sock}\n  port: 0\n  bind: 127.0.0.1\n` +
    `security:\n  key_dir: ${work}/keys\n` +
    `log:\n  level: warn\n`)

  const daemon: ChildProcess = spawn(DAEMON, [], {
    env: { ...process.env, C_BACKUP_CONFIG_PATH: join(work, 'config.yaml') },
    stdio: 'ignore',
  })
  for (let i = 0; i < 120 && !existsSync(sock); i++) await sleep(50)
  if (!existsSync(sock)) throw new Error('daemon did not start')

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
  /* One scrub, so the integrity screen has a run to open on. */
  cli(sock, ['verify', String(plain), '--level', 'deep'])

  /*
   * Seed the app's own config rather than clicking through Add Connection.
   * The connection dialog is worth testing on its own; every other screen
   * needs a connection to exist and should not re-test it.
   */
  writeFileSync(join(userData, 'config.json'), JSON.stringify({
    connections: [{ type: 'local', name: 'This machine', socketPath: sock }],
  }, null, 2))

  const app = await electron.launch({
    args: [APP_MAIN, `--user-data-dir=${userData}`, '--no-sandbox'],
    env: { ...process.env, NODE_ENV: 'production' },
  })
  const page = await app.firstWindow()
  await page.waitForLoadState('domcontentloaded')

  return {
    app, page, work, sock,
    repos: { plain, encrypted },
    async close() {
      await app.close().catch(() => {})
      daemon.kill()
      await sleep(200)
      rmSync(work, { recursive: true, force: true })
    },
  }
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
  const started = Date.now()
  await action()
  const ms = Date.now() - started
  timings.push({ label, ms, budget })
  return ms
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
