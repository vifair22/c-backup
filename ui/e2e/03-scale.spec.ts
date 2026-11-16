import { test, expect } from '@playwright/test'
import { startHarness, openRepo, descendTo, clock, record, median, reportTimings, timings, BUDGET_MS, type Harness } from './fixtures'

/*
 * The same screens, against a directory bigger than a page.
 *
 * Everything else here runs on a three-file repository, which proves a
 * screen renders and proves nothing about what it does at scale. Three of
 * these assertions could not have passed before: the browser fetched one
 * page of a directory and rendered it with no indication that more existed,
 * so a directory of 20,000 files showed 500 and looked complete. In a backup
 * browser that is the worst kind of wrong - somebody concludes a file was
 * never backed up.
 *
 * 1200 files: comfortably over the 500-entry page so the paging is real,
 * small enough that building the fixture and backing it up stays under a
 * couple of seconds.
 */

const WIDE = 1200
const PAGE = 500

let h: Harness

test.beforeAll(async () => { h = await startHarness({ wideDir: WIDE }) }, )
test.afterAll(async () => {
  reportTimings()
  await h?.close()
})

test.describe.configure({ mode: 'serial' })

/*
 * Five runs and the median, for the timings below that cannot be made
 * relative to anything: opening a big directory is what a person waits for,
 * so it keeps an absolute budget. On one sample those ran at 61-78% of
 * budget on a loaded runner, close enough that one slow scheduling slice
 * fails them. The median is what stops that, not a bigger number.
 */
const RUNS = 5

test('a directory bigger than one page says so', async () => {
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()

  /*
   * The tree caches every page it has read, so collapsing and reopening
   * the directory, or reading page two again, would time a render from
   * memory. Each run leaves the snapshot and comes back instead, which
   * remounts the browser with nothing cached, and times the same two reads
   * the first visit makes.
   */
  const more = h.page.getByRole('button', { name: /shown, load more/i })
  const open: number[] = []
  const next: number[] = []
  for (let i = 0; i < RUNS; i++) {
    if (i > 0) {
      await h.page.getByRole('button', { name: /back/i }).first().click()
      await expect(h.page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
    }
    await h.page.locator('tbody tr').first().click()
    await expect(h.page.getByText(/Snapshot #/).first()).toBeVisible()
    await descendTo(h.page, h.work)

    open.push(await clock(async () => {
      await h.page.getByText('wide', { exact: true }).click()
      await expect(h.page.getByText('f00000.txt', { exact: true })).toBeVisible()
    }))

    /* The claim: it says how much it is showing rather than implying it
     * has everything. Without this the remaining 700 files are simply
     * absent. Checked on every run, since every run is a fresh browser. */
    await expect(more).toBeVisible()
    await expect(more).toContainText(String(PAGE))
    await expect(h.page.getByText('f00700.txt', { exact: true })).toHaveCount(0)

    if (i === 0) await h.page.screenshot({ path: 'test-results/wide-dir.png', fullPage: false })

    next.push(await clock(async () => {
      await more.click()
      await expect(h.page.getByText('f00700.txt', { exact: true })).toBeVisible()
    }))
  }

  const openDir = median(open)
  const nextPage = median(next)
  record('open a 1200-entry directory (median)', openDir, BUDGET_MS.dialog)
  record('load the next page (median)', nextPage, BUDGET_MS.dialog)
  expect(openDir, `one page of a large directory opens promptly: ${open.join('/')} ms`)
    .toBeLessThan(BUDGET_MS.dialog)
  expect(nextPage, `the next page loads promptly: ${next.join('/')} ms`)
    .toBeLessThan(BUDGET_MS.dialog)

  /* Third page finishes it, and the control goes away rather than offering
   * a page that does not exist. */
  await h.page.getByRole('button', { name: /shown, load more/i }).click()
  await expect(h.page.getByText('f01199.txt', { exact: true })).toBeVisible()
  await expect(h.page.getByRole('button', { name: /shown, load more/i })).toHaveCount(0)
})

test('searching an index with real entries in it', async () => {
  await h.page.getByRole('button', { name: /back/i }).first().click()
  await h.page.getByRole('button', { name: /dashboard/i }).first().click()
  await h.page.getByRole('button', { name: /^search$/i }).first().click()

  const search = async (q: string, rows: number): Promise<void> => {
    await h.page.getByPlaceholder(/filename contains/i).fill(q)
    await h.page.getByRole('button', { name: /^search$/i }).last().click()
    await expect(h.page.locator('tbody tr')).toHaveCount(rows)
  }

  /*
   * A broad search should cost what a narrow one costs plus the rows it
   * draws, not grow with something per match. That is a ratio, so it is
   * asserted as one: the narrow search, timed against the same daemon on the
   * same page in the same moments, is the calibration, and a runner made
   * slow by its neighbours slows both. An absolute 600 ms here failed at
   * 631 and 602 ms on a loaded runner while the same search took 210-310 ms
   * on a quiet one (FLAKE-11).
   *
   * What the ratio cannot see, at this size, is whether the daemon used the
   * trigram index. Measured against this fixture's database (2,402 matching
   * rows over two snapshots), the route's FTS query takes 1.8 ms and a LIKE
   * scan of the nodes table 0.5 ms; the whole daemon request is 20 ms broad
   * and 0.4 ms narrow. The index wins at hundreds of thousands of names,
   * which an e2e fixture cannot build. That belongs to the daemon's tests.
   *
   * What it does see is any cost per result: a request per row, a render
   * that goes quadratic, a route that reads every match to fill a page. In
   * CI the narrow search's median is about 80-190 ms and the broad one
   * about 2-3 times that, so 5x plus 150 ms leaves the healthy figure two
   * to three times clear and still fails on half a millisecond per result
   * across a 1000-row page.
   *
   * Broad and narrow alternate, so every broad run draws 1000 rows from an
   * empty table and both medians come from the same stretch of runner load.
   * Broad first, so the screenshot afterwards is the one-row page.
   */
  const RATIO = 5
  const FLOOR_MS = 150
  /* The route's page cap, which the screen asks for; ~1200 names match. */
  const BROAD_ROWS = 1000
  const broad: number[] = []
  const narrow: number[] = []
  for (let i = 0; i < RUNS; i++) {
    broad.push(await clock(() => search('txt', BROAD_ROWS)))
    narrow.push(await clock(() => search('f00042', 1)))
  }
  await expect(h.page.getByText('f00042.txt', { exact: false }).first()).toBeVisible()

  const one = median(narrow)
  const many = median(broad)
  const bound = RATIO * one + FLOOR_MS
  record('search matching one name (median)', one, BUDGET_MS.submit)
  record(`search matching ~1200 names (median, ${RATIO}x narrow + ${FLOOR_MS})`, many, bound)

  expect(many, `a broad name search costs no more per match than a narrow one: ` +
                `broad ${broad.join('/')} ms, narrow ${narrow.join('/')} ms`).toBeLessThan(bound)
  /* The narrow search is also what a person waits for, so it keeps the
   * absolute budget; the median is what stops one slow slice failing it. */
  expect(one, `narrow ${narrow.join('/')} ms`).toBeLessThan(BUDGET_MS.submit)

  await h.page.screenshot({ path: 'test-results/wide-search.png', fullPage: true })
})

test('the object list at a few thousand objects', async () => {
  await h.page.getByRole('button', { name: /dashboard/i }).first().click()

  /* Back to the dashboard between runs: the screen remounts on the loose
   * tab with nothing loaded, so every run reads the packed page afresh.
   * Waiting for the full page rather than its first row, because the
   * count below is the claim. */
  const runs: number[] = []
  for (let i = 0; i < RUNS; i++) {
    if (i > 0) {
      await h.page.getByRole('button', { name: /back/i }).first().click()
      await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
    }
    runs.push(await clock(async () => {
      await h.page.getByRole('button', { name: /loose|objects/i }).first().click()
      await expect(h.page.getByRole('heading', { name: 'Objects' })).toBeVisible()
      await h.page.getByRole('button', { name: /^packed$/i }).click()
      await expect(h.page.locator('tbody tr')).toHaveCount(200)
    }))
  }
  const objects = median(runs)
  record('open objects at scale (median)', objects, BUDGET_MS.navigation)
  expect(objects, `objects at scale: ${runs.join('/')} ms`).toBeLessThan(BUDGET_MS.navigation)

  /* Paged, so the table is 200 rows whatever the store holds. */
  await expect(h.page.locator('tbody tr')).toHaveCount(200)
  await expect(h.page.getByRole('button', { name: /^next$/i })).toBeEnabled()
  await h.page.screenshot({ path: 'test-results/wide-objects.png', fullPage: true })
})

test('nothing measured was over budget', async () => {
  const over = timings.filter(t => t.ms > t.budget)
  expect(over.map(t => `${t.label}: ${t.ms}ms > ${t.budget}ms`), 'interactions over budget').toEqual([])
})
