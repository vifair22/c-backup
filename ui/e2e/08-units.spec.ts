import { test, expect, type Locator } from '@playwright/test'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * Units in the app (Manual 22), against a real daemon and the real helper
 * serving the fake provider: the repository's unit list with each unit's
 * state, a backup of the selected units and of all of them less the
 * excluded, a backup that fails a unit in the daemon's words, and a unit's
 * page with its snapshots and journal. Each step is checked against what
 * the daemon then says, not only against what the screen shows.
 */

let h: Harness

interface UnitSnap { id: number; consistency: string; items: Array<{ id: string; kind: string }> }

/* The unit's snapshots in the Documents repository, as the CLI prints them. */
function history(unit: string): UnitSnap[] {
  const out = cli(h.sock, ['unit', 'history', String(h.repos.plain), '--provider', 'fake', unit])
  return (JSON.parse(out) as { snapshots: UnitSnap[] }).snapshots
}

function row(name: string): Locator {
  return h.page.locator(`tr.unit-row[data-unit="${name}"]`)
}

const result = (): Locator => h.page.locator('.unit-backup-result').first()

test.beforeAll(async () => {
  h = await startHarness({ units: true })
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('units: the list shows each unit units.allow takes, with its state', async () => {
  const { page } = h
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: 'Units', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Units' })).toBeVisible({ timeout: 10_000 })
  await expect(row('cbk-uat-vm')).toContainText('running', { timeout: 10_000 })
  await expect(row('cbk-uat-web')).toContainText('running')
  await expect(row('cbk-uat-vm')).toContainText('never')
  /* Outside units.allow: the daemon does not list it, so neither does the app. */
  await expect(page.getByText('other-thing')).toHaveCount(0)
  await expect(page.getByText('units.allow:')).toBeVisible()
  await page.screenshot({ path: 'test-results/08-units-list.png', fullPage: true })
})

test('units: back up the selected unit', async () => {
  const { page } = h
  await page.getByLabel('Select cbk-uat-vm').check()
  await page.getByRole('button', { name: 'Back up selected (1)' }).click()
  await expect(result()).toContainText('completed', { timeout: 30_000 })
  await expect(result()).toContainText('cbk-uat-vm')
  const snaps = history('vm1')
  expect(snaps.length).toBe(1)
  await expect(result()).toContainText(`snapshot #${snaps[0].id}`)
  await expect(result()).toContainText(`level ${snaps[0].consistency}`)
  /* The list reloads with the new snapshot in place. */
  await expect(row('cbk-uat-vm')).toContainText(`#${snaps[0].id}`)
  await page.getByLabel('Select cbk-uat-vm').uncheck()
  await page.screenshot({ path: 'test-results/08-units-backup.png', fullPage: true })
})

test('units: back up all, less the excluded', async () => {
  const { page } = h
  await page.getByLabel('Exclude').fill('cbk-uat-vm')
  await page.getByRole('button', { name: 'Back up all' }).click()
  await expect(result()).toContainText('completed', { timeout: 30_000 })
  await expect(result()).toContainText('cbk-uat-web')
  expect(history('web').length).toBe(1)
  expect(history('vm1').length).toBe(1)
  await page.getByLabel('Exclude').fill('')
})

test('units: a level the capture cannot reach fails that unit, in the daemon\'s words', async () => {
  const { page } = h
  await page.getByLabel('Require').selectOption('quiesced')
  await page.getByLabel('Select cbk-uat-web').check()
  await page.getByRole('button', { name: 'Back up selected (1)' }).click()
  await expect(result()).toContainText('failed', { timeout: 30_000 })
  await expect(result()).toContainText('unit cbk-uat-web: achieved crash, below the required quiesced')
  expect(history('web').length).toBe(1)
  await page.screenshot({ path: 'test-results/08-units-refused-level.png', fullPage: true })
  await page.getByLabel('Require').selectOption('crash')
  await page.getByLabel('Select cbk-uat-web').uncheck()
})

test('unit history: a unit\'s page shows its snapshots, how each item was read, and its journal', async () => {
  const { page } = h
  await row('cbk-uat-vm').click()
  await expect(page.getByRole('heading', { name: 'cbk-uat-vm' })).toBeVisible()
  await expect(page.locator('.unit-state')).toHaveText('running')
  const [snap] = history('vm1')
  const tr = page.locator(`tr.unit-snapshot[data-snapshot="${snap.id}"]`)
  await expect(tr).toContainText(snap.consistency)
  await expect(tr).toContainText('vda')
  await expect(tr).toContainText('disk')
  await expect(tr).toContainText('full')
  /* The journal's unit entries: the three backups above, the failed one in
   * its own words. */
  await expect(page.locator('tr.unit-journal').filter({ hasText: 'units' }).first()).toBeVisible()
  await expect(page.locator('tr.unit-journal').filter({ hasText: 'below the required quiesced' })).toHaveCount(1)
  await page.screenshot({ path: 'test-results/08-unit-history.png', fullPage: true })
})

test('unit history: Back up now on the unit\'s page adds a snapshot to its history', async () => {
  const { page } = h
  await page.getByRole('button', { name: 'Back up now' }).click()
  await expect(result()).toContainText('completed', { timeout: 30_000 })
  const snaps = history('vm1')
  expect(snaps.length).toBe(2)
  await expect(page.locator(`tr.unit-snapshot[data-snapshot="${snaps[0].id}"]`)).toBeVisible()
  /* Browse opens the unit snapshot like any other. */
  await page.locator(`tr.unit-snapshot[data-snapshot="${snaps[0].id}"]`)
    .getByRole('button', { name: 'Browse' }).click()
  await expect(page.getByText(`Snapshot #${snaps[0].id}`).first()).toBeVisible({ timeout: 10_000 })
  await expect(page.getByText('.cbackup-unit', { exact: true }).first()).toBeVisible({ timeout: 10_000 })
})
