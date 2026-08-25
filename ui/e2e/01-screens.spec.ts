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

test('find in files: searches contents and shows hits', async () => {
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /find in files/i }).first().click()
  await expect(h.page.getByText('Search file contents').first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/10-grep-empty.png', fullPage: true })

  /* The fixture writes "the quick brown fox" into notes.txt. */
  await h.page.getByPlaceholder(/text to look for/i).fill('quick brown')
  await h.page.getByRole('button', { name: /^search$/i }).click()

  /* A search is a job, so the result appears when the daemon finishes
   * reading the snapshot rather than when the click returns. */
  await expect(h.page.getByText('quick brown', { exact: false }).first())
    .toBeVisible({ timeout: 20_000 })
  await expect(h.page.getByText('notes.txt', { exact: false }).first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/11-grep-hits.png', fullPage: true })
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

test('snapshots: the list, with retention and sizes', async () => {
  /*
   * This screen read four fields the daemon does not send and rendered a
   * dash in every column. The assertion that matters is that a real value
   * appears - a passing "the table is there" test is exactly what let the
   * empty version survive.
   */
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()

  await expect(h.page.getByText('#1', { exact: true })).toBeVisible()
  await expect(h.page.getByText('#2', { exact: true })).toBeVisible()
  const row = h.page.locator('tr', { hasText: '#2' }).first()
  await expect(row).not.toContainText('NaN')
  /* Files and size both came back as dashes before, because the component
   * read field names the daemon does not send. */
  await expect(row.locator('td').nth(3)).not.toHaveText('-')
  await expect(row.locator('td').nth(4)).not.toHaveText('-')
  /*
   * Retention is empty here and that is correct: tier election only
   * considers closed calendar days, so a snapshot taken minutes ago belongs
   * to no tier yet. The tier algorithm itself is covered by tests/test_gfs.c,
   * which can pin dates; what this checks is that the column renders its
   * placeholder rather than throwing on a snapshot with no flags.
   */
  await expect(row.locator('td').last()).toHaveText('-')

  await h.page.screenshot({ path: 'test-results/snapshot-list.png', fullPage: true })
})

test('browse: walking into a directory and opening a file', async () => {
  /*
   * The browser asked for `children` from a response carrying `entries`, so
   * every directory was empty. Walking two levels down and finding a known
   * fixture file is the check that could not have passed before.
   */
  const row = h.page.locator('tr', { hasText: '#2' }).first()
  await row.click()
  await expect(h.page.getByText('Snapshot #2').first()).toBeVisible()

  /* scan_tree records absolute paths, so the root holds the leading
   * directories of the source root, not the source root's contents. */
  await expect(h.page.locator('tbody tr').first()).toBeVisible()

  /* Descend until the fixture directory shows up. */
  for (let i = 0; i < 8; i++) {
    if (await h.page.getByText('notes.txt', { exact: true }).count() > 0) break
    const dir = h.page.locator('tbody tr').filter({ hasText: '/' }).last()
    if (await dir.count() === 0) break
    await dir.click()
    await h.page.waitForTimeout(120)
  }
  await expect(h.page.getByText('notes.txt', { exact: true })).toBeVisible()
  await expect(h.page.getByText('docs', { exact: true }).first()).toBeVisible()

  await h.page.screenshot({ path: 'test-results/snapshot-browser.png', fullPage: true })

  /* A file row opens a detail card, and the hash is the field the tree
   * route did not used to serve - without it "View Content" cannot exist. */
  await h.page.getByText('notes.txt', { exact: true }).click()
  await expect(h.page.getByText('View Content')).toBeVisible()
  await h.page.screenshot({ path: 'test-results/snapshot-file-detail.png' })
  await h.page.getByRole('button', { name: 'Close' }).click()
})

test('search: a name, with the path it lives at', async () => {
  /*
   * The result rows carried no path, type, size or hash - the route did not
   * send them and the component read them from a nested `node` object that
   * does not exist. A path is the whole answer to "where is this file", so
   * that is what gets asserted.
   */
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^search$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'File Search' })).toBeVisible()

  await h.page.getByPlaceholder(/filename contains/i).fill('notes')
  await h.page.getByRole('button', { name: /^search$/i }).last().click()

  const row = h.page.locator('tbody tr').first()
  await expect(row).toBeVisible()
  await expect(row).toContainText('/src/notes.txt')
  await expect(row).toContainText('file')
  await expect(row.locator('td').last()).not.toHaveText('-')

  await h.page.screenshot({ path: 'test-results/file-search.png', fullPage: true })

  /* Under three characters is refused by the daemon, because the name index
   * is trigram-based and an empty list would read as "no such file". */
  await h.page.getByPlaceholder(/filename contains/i).fill('no')
  await h.page.getByRole('button', { name: /^search$/i }).last().click()
  await expect(h.page.getByText(/at least 3 characters/i)).toBeVisible()

  await h.page.getByRole('button', { name: /dashboard/i }).first().click()
})

test('activity: operations, results and how long they took', async () => {
  /*
   * The journal is one row per finished operation now. The old screen paired
   * "started" and "completed" rows on an op_id that no longer exists, so
   * every field it keyed on was undefined.
   */
  await h.page.getByRole('button', { name: /journal|activity/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Activity' })).toBeVisible()

  const row = h.page.locator('tbody tr').first()
  await expect(row).toBeVisible()
  /* The fixture ran backups, a retention pass and a scrub. */
  await expect(h.page.locator('tbody')).toContainText('backup')
  /* duration_ms was selected by the route and never sent; without it every
   * row's "Took" is a dash. */
  await expect(row.locator('td').nth(3)).not.toHaveText('-')

  await h.page.screenshot({ path: 'test-results/journal.png', fullPage: true })
  await h.page.getByRole('button', { name: /back/i }).first().click()
})

test('tags: naming a snapshot protects it', async () => {
  await h.page.getByRole('button', { name: /^tags$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Tags' })).toBeVisible()
  await expect(h.page.getByText(/never dropped by retention/i)).toBeVisible()

  await h.page.getByRole('button', { name: /add tag/i }).click()
  await h.page.getByPlaceholder('before-upgrade').fill('keeper')
  await h.page.getByRole('button', { name: /^add$/i }).click()

  const row = h.page.locator('tbody tr', { hasText: 'keeper' })
  await expect(row).toBeVisible()
  /* The Taken column was blank for every tag: the dates came from a
   * snapshot-list shape the daemon does not send. */
  await expect(row.locator('td').nth(2)).not.toHaveText('')

  await h.page.screenshot({ path: 'test-results/tags.png', fullPage: true })
  await h.page.getByRole('button', { name: /back/i }).first().click()
})

test('retention: the five values the daemon actually reads', async () => {
  await h.page.getByRole('button', { name: /policy|retention/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Retention' })).toBeVisible()

  /* Defaults are shown as the values in force, not as blanks. */
  const daily = h.page.locator('input[type="number"]').first()
  await expect(daily).toHaveValue('7')
  await expect(h.page.getByText(/daemon default/i).first()).toBeVisible()

  await daily.fill('14')
  await expect(h.page.getByText(/1 unsaved change/i)).toBeVisible()
  await h.page.getByRole('button', { name: /^save$/i }).click()
  await expect(h.page.getByText('Saved', { exact: true })).toBeVisible()

  await h.page.screenshot({ path: 'test-results/retention.png', fullPage: true })

  /* It persisted, and is no longer flagged as a default. */
  await h.page.getByRole('button', { name: /back/i }).first().click()
  await h.page.getByRole('button', { name: /policy|retention/i }).first().click()
  await expect(h.page.locator('input[type="number"]').first()).toHaveValue('14')
})
