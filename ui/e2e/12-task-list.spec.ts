import { test, expect } from '@playwright/test'
import { mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * The Background Tasks view, reached from a repository's Tasks button.
 *
 * The harness seeds jobs on Documents (two backups, a retention pass, a
 * verify). Photos gets one more here: a backup of a source in strict
 * snapshot mode, which fails before anything is read because the harness
 * has no helper to take a snapshot (Manual 6.6). That gives a failed job
 * whose error the daemon wrote, on a repository of its own.
 */

interface WireJob { id: number; repo_id: number; dest_repo_id?: number | null; action: string; state: string; error: string }

let h: Harness
let failed: WireJob

function jobs(): WireJob[] {
  return JSON.parse(cli(h.sock, ['jobs', '--limit', '1000'])).jobs
}

test.beforeAll(async () => {
  h = await startHarness()
  const src = join(h.work, 'photos-src')
  mkdirSync(src, { recursive: true })
  writeFileSync(join(src, 'a.jpg'), 'not really a picture\n')
  cli(h.sock, ['sources', 'set', String(h.repos.encrypted), src, '--snapshot', src])
  cli(h.sock, ['backup', String(h.repos.encrypted)])
  const f = jobs().find(j => j.repo_id === h.repos.encrypted && j.state === 'failed')
  if (!f || !f.error) throw new Error('the strict-mode backup on Photos did not fail with an error')
  failed = f
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

async function openTasks(label: string): Promise<void> {
  await openRepo(h.page, label)
  await h.page.getByRole('button', { name: /^tasks$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Background Tasks' })).toBeVisible({ timeout: 10_000 })
}

test('task list: a failed job shows its error once (NIT-47)', async () => {
  await openTasks('Photos')
  const card = h.page.locator(`[data-testid="job-card"][data-job-id="${failed.id}"]`)
  await expect(card).toBeVisible({ timeout: 10_000 })
  await expect(card.getByText('Failed', { exact: true })).toBeVisible()
  /* It used to be printed twice: in the card's body and again in the
   * detail row at its foot. */
  await expect(card.getByText(failed.error, { exact: true })).toHaveCount(1)
  await h.page.screenshot({ path: 'test-results/12-task-failed.png', fullPage: true })
})

test("task list: a repository's view lists only that repository's jobs (Demo L4)", async () => {
  const all = jobs()
  const mine = all.filter(j => j.repo_id === h.repos.plain)
  const theirs = all.filter(j => j.repo_id !== h.repos.plain)
  expect(mine.length).toBeGreaterThan(0)
  expect(theirs.map(j => j.id)).toContain(failed.id)

  await openTasks('Documents')
  const cards = h.page.locator('[data-testid="job-card"]')
  await expect(cards).toHaveCount(mine.length, { timeout: 10_000 })
  for (const j of mine) {
    await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${j.id}"]`)).toBeVisible()
  }
  /* Photos' failed backup is not Documents' business: neither its card
   * nor the task bar's failed-job notice. */
  for (const j of theirs) {
    await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${j.id}"]`)).toHaveCount(0)
  }
  await expect(h.page.getByText(/failed job.*click to view/)).toHaveCount(0)
  await h.page.screenshot({ path: 'test-results/12-task-list-scoped.png', fullPage: true })

  /* And the other way round. */
  await openTasks('Photos')
  await expect(cards).toHaveCount(theirs.filter(j => j.repo_id === h.repos.encrypted).length,
                                  { timeout: 10_000 })
  await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${failed.id}"]`)).toBeVisible()
  for (const j of mine) {
    await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${j.id}"]`)).toHaveCount(0)
  }
  await expect(h.page.getByText(/1 failed job - click to view/)).toBeVisible()
})

test('task list: a replicate is listed under the repository it writes into (B283)', async () => {
  /* The app cut the daemon-wide list to repo_id itself, so a replicate,
   * whose repo_id is its source, could never show under its destination.
   * It asks the daemon for ?repo=<id> now, which lists both. */
  const out = cli(h.sock, ['repo', 'add', join(h.work, 'replica'), 'Replica'])
  const dest = Number(/"id":(\d+)/.exec(out)?.[1])
  if (!dest) throw new Error(`repo add failed: ${out}`)
  cli(h.sock, ['replicate', String(h.repos.plain), join(h.work, 'replica'), '--reencrypt'])
  const rep = jobs().find(j => j.action === 'replicate' && j.dest_repo_id === dest)
  if (!rep || rep.state !== 'completed') throw new Error(`no completed replicate into ${dest}: ${JSON.stringify(rep)}`)

  /* The sidebar lists repositories as the app found them at start. */
  await h.relaunch()
  await openTasks('Replica')
  const card = h.page.locator(`[data-testid="job-card"][data-job-id="${rep.id}"]`)
  await expect(card).toBeVisible({ timeout: 10_000 })
  await expect(card.getByTestId('job-from')).toHaveText(`into this repository from repository ${h.repos.plain}`)
  /* Under its source too, without the destination's note. */
  await openTasks('Documents')
  await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${rep.id}"]`)).toBeVisible({ timeout: 10_000 })
  await expect(h.page.locator(`[data-testid="job-card"][data-job-id="${rep.id}"]`).getByTestId('job-from')).toHaveCount(0)
  await h.page.screenshot({ path: 'test-results/12-task-list-replicate.png', fullPage: true })
})
