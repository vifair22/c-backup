import { test, expect } from '@playwright/test'
import { startHarness, openRepo, descendTo, type Harness } from './fixtures'

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
  await expect(row.locator('td').nth(5)).toHaveText('-')

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

  await descendTo(h.page, h.work)
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

test('objects: the store, by object', async () => {
  /*
   * Sizes here came from `uncompressed_size`/`compressed_size` on rows that
   * carry orig_size and phys_size, so both size columns were empty and the
   * ratio divided by undefined.
   */
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /loose|objects/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Objects' })).toBeVisible()

  /* The fixture packs on backup completion, so the interesting rows are
   * under Packed rather than Loose. */
  await h.page.getByRole('button', { name: /^packed$/i }).click()
  const row = h.page.locator('tbody tr').first()
  await expect(row).toBeVisible()
  await expect(row.locator('td').nth(3)).not.toHaveText('-')
  await expect(row.locator('td').nth(4)).not.toHaveText('-')

  await h.page.screenshot({ path: 'test-results/objects.png', fullPage: true })

  /* A row opens the object, and its content can be read back - which is the
   * whole claim a content-addressed store makes. */
  await row.click()
  await expect(h.page.getByText('Referenced by')).toBeVisible()
  await h.page.getByRole('button', { name: /view content/i }).click()
  await expect(h.page.getByText('Hash', { exact: true }).first()).toBeVisible()
  /*
   * Size from the index, and size measured off the object itself. Two
   * different facts, which is the point: when they disagree the index is
   * stale, and nothing else in the app would say so.
   */
  await expect(h.page.getByText('Record on disk')).toBeVisible()
  await expect(h.page.getByText('Payload', { exact: true })).toBeVisible()
  /* A manifest tree is object type 5, and every component's own copy of the
   * type list stopped at 4, so most of a repository read as "Unknown (5)". */
  await expect(h.page.getByText(/Unknown \(\d+\)/)).toHaveCount(0)
  await expect(h.page.getByText('Header', { exact: true })).toBeVisible()
  /* They agree here, so the staleness warning must not be showing. */
  await expect(h.page.getByText(/index is\s+stale/i)).toHaveCount(0)
  await h.page.screenshot({ path: 'test-results/object-content.png' })
  await h.page.keyboard.press('Escape')
})

test('packs: entries, and the hash distribution', async () => {
  /*
   * This screen threw on open: it listed packs with a 'scan' action the
   * bridge had no case for. The fanout chart then read a cumulative table
   * out of a .idx header that no longer exists - the recorded gap.
   */
  await h.page.reload()
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /pack/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Packs' })).toBeVisible()

  const pack = h.page.locator('tbody tr').first()
  await expect(pack).toBeVisible()
  await pack.click()
  const entry = h.page.locator('tbody tr').last()
  await expect(entry).toBeVisible()

  await h.page.screenshot({ path: 'test-results/packs.png', fullPage: true })

  await h.page.getByRole('button', { name: /hash distribution/i }).click()
  /* Wait for real buckets. "0 objects" is what the panel shows before the
   * fetch lands, and an assertion loose enough to match that would pass on
   * an empty chart - which is exactly what it did the first time. */
  await expect(h.page.getByText(/^(?!0 objects)\d+ objects,/)).toBeVisible()
  await h.page.screenshot({ path: 'test-results/fanout.png' })
  await h.page.keyboard.press('Escape')
})

test('diff: what changed between two snapshots', async () => {
  /*
   * The filter buttons matched single-letter change codes against the words
   * the daemon sends, so every row was filtered out and the table was
   * always empty. The fixture adds a file between its two snapshots, so
   * there is exactly one thing to find.
   */
  await h.page.reload()
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /compare/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Compare Snapshots' })).toBeVisible()

  await expect(h.page.getByText('second.md', { exact: false })).toBeVisible()
  await expect(h.page.getByRole('button', { name: /Added 1/ })).toBeVisible()

  await h.page.screenshot({ path: 'test-results/diff.png', fullPage: true })

  /* Opening the row shows the file's contents, which needs the hash the
   * daemon only started sending with this change. */
  await h.page.getByText('second.md', { exact: false }).click()
  await expect(h.page.getByText('added later', { exact: false })).toBeVisible({ timeout: 10_000 })
  await h.page.screenshot({ path: 'test-results/diff-content.png' })
})

test('daemon: whether anything is wrong', async () => {
  /*
   * `health` and `metrics` were reachable from the CLI and from nothing in
   * the app, which meant the single most important number in a backup
   * system - how long since anything was backed up - lived in Prometheus and
   * in a terminal, and nowhere a person would look.
   *
   * /metrics is Prometheus text, so it also could not have worked before:
   * the transport put any non-JSON body under `error`, so a successful
   * response arrived looking like a failure.
   */
  await h.page.reload()
  await h.page.getByText('This machine', { exact: false }).first().click({ button: 'right' })
  await h.page.getByText('Daemon status').click()
  await expect(h.page.getByRole('heading', { name: 'Daemon' })).toBeVisible()

  /* Ready, not merely up: the process stays alive when its database stops
   * answering, which is the failure that actually happens. */
  await expect(h.page.getByText('ready', { exact: true })).toBeVisible()

  /*
   * Parsed metrics, not an error blob. The fixture registers two
   * repositories and takes two snapshots, so both are known values rather
   * than "some number appeared".
   */
  await expect(h.page.getByText('Repositories', { exact: true })).toBeVisible()
  await expect(h.page.getByText('Newest snapshot', { exact: true })).toBeVisible()
  /* The age of the newest snapshot, which is the number this screen exists
   * for. Seconds, because the fixture just made it. */
  await expect(h.page.getByText(/^\d+s ago$/).first()).toBeVisible()

  /* The encrypted fixture repo has never had its key acknowledged, so the
   * screen should be saying so rather than reporting all clear. */
  await expect(h.page.getByText(/exists only on this machine/i)).toBeVisible()

  await h.page.screenshot({ path: 'test-results/daemon-status.png', fullPage: true })
})

test('snapshots: deleting one, which the app could not do at all', async () => {
  /*
   * The CLI has had `snapshot delete` since the rewrite; the app had no way
   * to reach it. Deleted last, because every earlier test in this file wants
   * both fixture snapshots present.
   */
  await h.page.reload()
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(h.page.getByText('#1', { exact: true })).toBeVisible()

  const row = h.page.locator('tr', { hasText: '#1' }).first()
  await row.getByRole('button', { name: /delete/i }).click()

  /* The dialog has to say that space does not come back until a garbage
   * collection runs, or deleting a snapshot looks like it did nothing. */
  await expect(h.page.getByText(/garbage collection runs/i)).toBeVisible()
  await h.page.screenshot({ path: 'test-results/snapshot-delete.png' })
  await h.page.getByRole('button', { name: /^delete$/i }).last().click()

  await expect(h.page.getByText('#1', { exact: true })).toHaveCount(0)
  await expect(h.page.getByText('#2', { exact: true })).toBeVisible()

  /* And it is gone from the daemon, not just from this table. */
  await h.page.reload()
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(h.page.getByText('#2', { exact: true })).toBeVisible()
  await expect(h.page.getByText('#1', { exact: true })).toHaveCount(0)
})
