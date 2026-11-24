import { test, expect } from '@playwright/test'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * Queued and cancelled jobs in the Background Tasks view (B282).
 *
 * Both used to fall through to the blue "Running" card, and only a running
 * job had a Cancel button, though the daemon takes a queued job out of the
 * queue on cancel (api.md, POST /jobs/<id>/cancel).
 *
 * A queued job needs something to wait behind. Jobs for one repository
 * dispatch one at a time, so a unit backup held up by a stopped helper
 * (SIGSTOP: the daemon waits on it as on a stuck one) keeps Documents
 * busy, and the file backups submitted after it stay queued for as long
 * as the helper is stopped.
 */

interface WireJob { id: number; repo_id: number; action: string; state: string }

let h: Harness
let running: WireJob
let queued: WireJob
let cancelled: WireJob

function jobs(): WireJob[] {
  return JSON.parse(cli(h.sock, ['jobs', '--limit', '1000'])).jobs
}

async function until<T>(what: string, f: () => T | undefined, ms = 20_000): Promise<T> {
  const end = Date.now() + ms
  for (;;) {
    const v = f()
    if (v !== undefined) return v
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}: ${JSON.stringify(jobs())}`)
    await new Promise(r => setTimeout(r, 100))
  }
}

test.beforeAll(async () => {
  h = await startHarness({ units: true })
  const repo = String(h.repos.plain)
  const before = new Set(jobs().map(j => j.id))
  const fresh = () => jobs().filter(j => !before.has(j.id))

  h.pauseHelper()
  cli(h.sock, ['unit', 'backup', repo, '--provider', 'fake', 'cbk-uat-vm', '--no-wait'])
  running = await until('the unit backup to start', () =>
    fresh().find(j => j.state === 'running'))

  cli(h.sock, ['backup', repo, '--no-wait'])
  cli(h.sock, ['backup', repo, '--no-wait'])
  const waiting = await until('two queued backups', () => {
    const q = fresh().filter(j => j.state === 'queued').sort((a, b) => a.id - b.id)
    return q.length === 2 ? q : undefined
  })
  queued = waiting[0]
  cancelled = waiting[1]
  cli(h.sock, ['jobs', 'cancel', String(cancelled.id)])
  await until('the second backup to be cancelled', () =>
    jobs().find(j => j.id === cancelled.id && j.state === 'cancelled'))
})
test.afterAll(async () => { h?.resumeHelper(); await h?.close() })

test.describe.configure({ mode: 'serial' })

const card = (id: number) => h.page.locator(`[data-testid="job-card"][data-job-id="${id}"]`)

test('task list: queued and cancelled jobs have their own state, not "Running" (B282)', async () => {
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^tasks$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Background Tasks' })).toBeVisible({ timeout: 10_000 })

  await expect(card(running.id).getByTestId('job-state')).toHaveText('Running', { timeout: 10_000 })
  await expect(card(queued.id).getByTestId('job-state')).toHaveText('Queued')
  await expect(card(cancelled.id).getByTestId('job-state')).toHaveText('Cancelled')

  /* Not merely relabelled: the card's colour is not the running one. */
  const bg = (id: number) => card(id).evaluate(e => getComputedStyle(e).backgroundColor)
  const runningBg = await bg(running.id)
  expect(await bg(queued.id)).not.toBe(runningBg)
  expect(await bg(cancelled.id)).not.toBe(runningBg)

  /* Running and queued can be cancelled; a cancelled job is over. */
  await expect(card(running.id).getByRole('button', { name: 'Cancel' })).toBeVisible()
  await expect(card(queued.id).getByRole('button', { name: 'Cancel' })).toBeVisible()
  await expect(card(cancelled.id).getByRole('button', { name: 'Cancel' })).toHaveCount(0)
  await h.page.screenshot({ path: 'test-results/14-task-states.png', fullPage: true })
})

test('task list: a queued job is cancelled from its card (B282)', async () => {
  await card(queued.id).getByRole('button', { name: 'Cancel' }).click()
  await h.page.getByRole('button', { name: 'Cancel Task' }).click()
  await expect(card(queued.id).getByTestId('job-state')).toHaveText('Cancelled', { timeout: 10_000 })
  await expect(card(queued.id).getByRole('button', { name: 'Cancel' })).toHaveCount(0)
  expect(jobs().find(j => j.id === queued.id)?.state).toBe('cancelled')
  /* The job it waited behind is untouched. */
  await expect(card(running.id).getByTestId('job-state')).toHaveText('Running')
})
