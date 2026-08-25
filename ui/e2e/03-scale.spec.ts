import { test, expect } from '@playwright/test'
import { startHarness, openRepo, descendTo, timed, reportTimings, timings, BUDGET_MS, type Harness } from './fixtures'

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

test('a directory bigger than one page says so', async () => {
  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await h.page.locator('tbody tr').first().click()
  await expect(h.page.getByText(/Snapshot #/).first()).toBeVisible()

  await descendTo(h.page, h.work)

  const openDir = await timed('open a 1200-entry directory', BUDGET_MS.dialog, async () => {
    await h.page.getByText('wide', { exact: true }).click()
    await expect(h.page.getByText('f00000.txt', { exact: true })).toBeVisible()
  })
  expect(openDir, 'one page of a large directory is one indexed query').toBeLessThan(BUDGET_MS.dialog)

  /* The claim: it says how much it is showing rather than implying it has
   * everything. Without this the remaining 700 files are simply absent. */
  const more = h.page.getByRole('button', { name: /shown, load more/i })
  await expect(more).toBeVisible()
  await expect(more).toContainText(String(PAGE))
  await expect(h.page.getByText('f00700.txt', { exact: true })).toHaveCount(0)

  await h.page.screenshot({ path: 'test-results/wide-dir.png', fullPage: false })

  const nextPage = await timed('load the next page', BUDGET_MS.dialog, async () => {
    await more.click()
    await expect(h.page.getByText('f00700.txt', { exact: true })).toBeVisible()
  })
  expect(nextPage).toBeLessThan(BUDGET_MS.dialog)

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

  /* A trigram index, not a scan: a query matching a thousand names should
   * cost what a query matching one costs. */
  const many = await timed('search matching ~1200 names', BUDGET_MS.submit, async () => {
    await h.page.getByPlaceholder(/filename contains/i).fill('txt')
    await h.page.getByRole('button', { name: /^search$/i }).last().click()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(many, 'a broad name search is still an index lookup').toBeLessThan(BUDGET_MS.submit)

  const one = await timed('search matching one name', BUDGET_MS.submit, async () => {
    await h.page.getByPlaceholder(/filename contains/i).fill('f00042')
    await h.page.getByRole('button', { name: /^search$/i }).last().click()
    await expect(h.page.getByText('f00042.txt', { exact: false }).first()).toBeVisible()
  })
  expect(one).toBeLessThan(BUDGET_MS.submit)

  await h.page.screenshot({ path: 'test-results/wide-search.png', fullPage: true })
})

test('the object list at a few thousand objects', async () => {
  await h.page.getByRole('button', { name: /dashboard/i }).first().click()
  const objects = await timed('open objects at scale', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /loose|objects/i }).first().click()
    await expect(h.page.getByRole('heading', { name: 'Objects' })).toBeVisible()
    await h.page.getByRole('button', { name: /^packed$/i }).click()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(objects).toBeLessThan(BUDGET_MS.navigation)

  /* Paged, so the table is 200 rows whatever the store holds. */
  await expect(h.page.locator('tbody tr')).toHaveCount(200)
  await expect(h.page.getByRole('button', { name: /^next$/i })).toBeEnabled()
  await h.page.screenshot({ path: 'test-results/wide-objects.png', fullPage: true })
})

test('nothing measured was over budget', async () => {
  const over = timings.filter(t => t.ms > t.budget)
  expect(over.map(t => `${t.label}: ${t.ms}ms > ${t.budget}ms`), 'interactions over budget').toEqual([])
})
