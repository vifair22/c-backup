import { test, expect, type Locator } from '@playwright/test'
import { existsSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * Database detection in the app (docs/db-detection.md 7.3, 9.5), against a
 * real daemon: the repository's Databases view with a live SQLite found in
 * a file source and another in a unit, the unit's Detect now, a finding
 * previewed and enabled, another dismissed, and a restore that puts a
 * database back from its engine copy. Each step is checked against what
 * the daemon then says, not only against what the screen shows.
 */

let h: Harness

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

interface Finding { id: number; scope: string; engine: string; path: string; state: string
  template?: string | null; reason?: string | null }

function findings(): Finding[] {
  const out = cli(h.sock, ['db', 'list', String(h.repos.plain), '--limit', '100'])
  return (JSON.parse(out) as { findings: Finding[] }).findings
}

function finding(pathEnd: string): Finding {
  const f = findings().find(x => x.engine === 'sqlite' && x.path.endsWith(pathEnd))
  if (!f) throw new Error(`no sqlite finding for ${pathEnd}: ${JSON.stringify(findings())}`)
  return f
}

/* The daemon's job list, newest first, as the CLI prints it. */
function jobs(): Array<Record<string, unknown> & { id: number }> {
  const j = JSON.parse(cli(h.sock, ['jobs', '--limit', '50']))
  return (Array.isArray(j) ? j : j.jobs) as Array<Record<string, unknown> & { id: number }>
}

function actions(id: number): Locator {
  return h.page.locator(`.db-card-actions[data-finding-id="${id}"]`)
}

test.beforeAll(async () => {
  h = await startHarness({ databases: true })
  /* The unit's first backup: its findings, under its own scope. */
  const out = cli(h.sock, ['unit', 'backup', String(h.repos.plain), '--provider', 'fake', 'cbk-uat-db'])
  if (!out.includes('"status":"ok"')) throw new Error(`unit backup failed: ${out}`)
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('databases: the repository view lists a file source\'s live database, uncovered', async () => {
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^databases/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Databases' })).toBeVisible({ timeout: 10_000 })
  await expect(h.page.getByText('live.db', { exact: false }).first()).toBeVisible({ timeout: 10_000 })
  await expect(h.page.getByText(/uncovered/i).first()).toBeVisible()
  const f = finding('/app/live.db')
  expect(f.scope.startsWith('source:')).toBe(true)
  await h.page.screenshot({ path: 'test-results/07-databases.png', fullPage: true })
})

test('databases: a unit\'s findings, and Detect now runs detection without a backup', async () => {
  await expect(h.page.getByText('grafana.db', { exact: false }).first()).toBeVisible()
  const g = finding('/grafana.db')
  expect(g.scope).toBe('unit:fake:db')
  const seen = new Set(jobs().map(j => j.id))
  await h.page.getByRole('button', { name: 'Detect now' }).first().click()
  /* A detect job, run to its end, and the button back. */
  await expect(h.page.getByRole('button', { name: 'Detecting...' })).toHaveCount(0, { timeout: 30_000 })
  const fresh = jobs().filter(j => !seen.has(j.id))
  expect(fresh.length).toBeGreaterThan(0)
  expect(JSON.stringify(fresh)).toContain('detect')
  expect(finding('/grafana.db').id).toBe(g.id)
})

test('databases: preview and enable the engine copy for the file source\'s database', async () => {
  const f = finding('/app/live.db')
  const row = actions(f.id)
  await row.getByRole('button', { name: 'Preview' }).click()
  const enable = row.getByRole('button', { name: /^enable/i })
  await expect(enable).toBeEnabled({ timeout: 15_000 })
  await h.page.screenshot({ path: 'test-results/07-databases-preview.png', fullPage: true })
  await enable.click()
  for (let i = 0; i < 50 && finding('/app/live.db').state !== 'enabled'; i++) await sleep(200)
  const now = finding('/app/live.db')
  expect(now.state).toBe('enabled')
  expect(now.template).toBe('sqlite-backup')
  await expect(h.page.getByText('Enabled: sqlite-backup').first()).toBeVisible({ timeout: 10_000 })
})

test('databases: dismiss the unit\'s finding, with a reason', async () => {
  const g = finding('/grafana.db')
  await actions(g.id).getByRole('button', { name: 'Dismiss' }).click()
  await h.page.getByLabel('Reason').fill('a test fixture, not data')
  await h.page.getByRole('button', { name: 'Dismiss', exact: true }).last().click()
  for (let i = 0; i < 50 && finding('/grafana.db').state !== 'dismissed'; i++) await sleep(200)
  const now = finding('/grafana.db')
  expect(now.state).toBe('dismissed')
  expect(now.reason).toBe('a test fixture, not data')
  await expect(h.page.getByText('Dismissed: a test fixture, not data').first()).toBeVisible({ timeout: 10_000 })
})

test('restore: a database comes back from its engine copy, without its stale -wal', async () => {
  /* The enabled copy is made by the next backup of the source. */
  const out = cli(h.sock, ['backup', String(h.repos.plain)])
  expect(out).toContain('"method":"sqlite-online-backup"')
  expect(out).toContain('"outcome":"ok"')
  const snap = Number((out.match(/"snapshot_id":(\d+)/) ?? [])[1])
  expect(snap).toBeGreaterThan(0)

  const { page } = h
  await page.getByText('Documents', { exact: true }).first().click()
  await page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  await page.locator('tr', { hasText: `#${snap}` }).first().click()
  await expect(page.getByText(`Snapshot #${snap}`).first()).toBeVisible()
  await page.getByRole('button', { name: 'Restore', exact: true }).first().click()
  await expect(page.getByRole('heading', { name: 'Restore Snapshot' })).toBeVisible()
  const dest = join(h.work, 'restored-dbs')
  await page.getByPlaceholder('/path/to/restore/destination').fill(dest)
  await page.getByLabel('Restore databases from their engine copies').check()
  await page.screenshot({ path: 'test-results/07-restore-engine-copy.png', fullPage: true })
  await page.getByRole('button', { name: 'Start Restore' }).click()

  /* The snapshot records absolute paths, so the restore lands under them. */
  const db = resolve(dest + join(h.work, 'src', 'app', 'live.db'))
  const notes = resolve(dest + join(h.work, 'src', 'notes.txt'))
  for (let i = 0; i < 150 && !(existsSync(db) && existsSync(notes)); i++) await sleep(200)
  expect(existsSync(db)).toBe(true)
  /* From the copy: the stale -wal beside the raw file is left out, which a
   * plain restore would have brought back. */
  expect(existsSync(db + '-wal')).toBe(false)
  const restore = jobs().find(j => JSON.stringify(j).includes('restore'))
  expect(restore).toBeTruthy()
  expect(cli(h.sock, ['jobs', String(restore!.id)])).toContain('"engine_copies_used":1')
})
