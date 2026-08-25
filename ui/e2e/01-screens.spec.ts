import { test, expect } from '@playwright/test'
import { startHarness, openRepo, type Harness } from './fixtures'

/*
 * The screens added this session, driven the way an operator reaches them.
 *
 * Each one screenshots into test-results/ on success as well as failure: a
 * layout is easier to judge from a picture than from a passing assertion,
 * and these are the pictures a review looks at.
 */

let h: Harness

test.beforeAll(async () => { h = await startHarness() })
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('dashboard: a repository with real snapshots', async () => {
  await openRepo(h.page, 'Documents')
  await expect(h.page.getByText('Snapshots', { exact: false }).first()).toBeVisible({ timeout: 10_000 })
  await h.page.screenshot({ path: 'test-results/01-dashboard.png', fullPage: true })
})

test('backup dialog: shows the configured sources', async () => {
  await h.page.getByRole('button', { name: /backup/i }).first().click()
  /* The sources the repository holds, not an empty form. This is the dialog
   * that had no way to say what to back up. */
  await expect(h.page.getByText('Sources', { exact: false }).first()).toBeVisible()
  await expect(h.page.getByText('/tmp/cbackup-e2e-', { exact: false }).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/02-backup-dialog.png', fullPage: true })
  await h.page.getByRole('button', { name: /^cancel$/i }).first().click()
})

test('schedules: empty state, then a real schedule', async () => {
  await h.page.getByRole('button', { name: /schedules/i }).first().click()
  await expect(h.page.getByText('Schedules', { exact: false }).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/03-schedules-empty.png', fullPage: true })

  await h.page.getByRole('button', { name: /add schedule/i }).click()
  await h.page.screenshot({ path: 'test-results/04-schedules-add.png', fullPage: true })
  await h.page.getByRole('button', { name: /^add$/i }).click()
  await expect(h.page.getByText('enabled', { exact: false }).first()).toBeVisible({ timeout: 10_000 })
  await h.page.screenshot({ path: 'test-results/05-schedules-listed.png', fullPage: true })
  await h.page.getByRole('button', { name: /^back$/i }).first().click()
})

test('integrity: the scrub that already ran', async () => {
  await h.page.getByRole('button', { name: /integrity/i }).first().click()
  await expect(h.page.getByText('Integrity checks').first()).toBeVisible()
  /* The fixture ran a deep verify, so there is a run to open on and it
   * should read clean. */
  await expect(h.page.getByText('clean', { exact: false }).first()).toBeVisible({ timeout: 10_000 })
  await expect(h.page.getByText('Objects checked').first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/06-integrity.png', fullPage: true })
  await h.page.getByRole('button', { name: /^back$/i }).first().click()
})

test('encryption: an unencrypted repository says so', async () => {
  await h.page.getByRole('button', { name: /encryption/i }).first().click()
  await expect(h.page.getByText('not encrypted', { exact: false }).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/07-encryption-none.png', fullPage: true })
  await h.page.getByRole('button', { name: /^back$/i }).first().click()
})

test('encryption: an unsaved key is called out', async () => {
  await openRepo(h.page, 'Photos')
  await h.page.getByRole('button', { name: /encryption/i }).first().click()
  /* The warning that was only ever visible in Prometheus. */
  await expect(h.page.getByText('not confirmed').first()).toBeVisible({ timeout: 10_000 })
  await expect(h.page.getByText('exists in one file', { exact: false }).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/08-encryption-unsaved.png', fullPage: true })

  await h.page.getByRole('button', { name: /show key/i }).click()
  await expect(h.page.getByText(/^[0-9a-f]{64}$/).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/09-encryption-key.png', fullPage: true })
})
