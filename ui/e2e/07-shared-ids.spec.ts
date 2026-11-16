import { test, expect, type Page } from '@playwright/test'
import { startHarness, openRepo, descendTo, type Harness } from './fixtures'

/*
 * File ids are shared, directory ids are not (docs/index-reuse.md).
 *
 * The index keys a file's row by the tree it sits in, so a file has the same
 * id in every snapshot holding it in an unchanged directory, and every copy
 * of an identical directory within one snapshot shows the same file ids.
 * The fixture's twin-a and twin-b hold one file under two names, so their
 * trees are the same tree: two rows on screen, one file id. Anything in the
 * browser keyed by a file's id treats them as one row, and anything that
 * outlives a snapshot switch can land on the same file in another snapshot.
 *
 * Rows expose their path, selection and highlight as attributes
 * (data-path, aria-selected, data-highlighted) so these can be asserted
 * without depending on styling classes.
 */

let h: Harness

test.beforeAll(async () => { h = await startHarness({ twins: true }) })
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

const selectedRows = (page: Page) => page.locator('tbody tr[aria-selected="true"]')
const highlightedRows = (page: Page) => page.locator('tbody tr[data-highlighted="true"]')

async function openSnapshot(page: Page, id: number): Promise<void> {
  await page.reload()
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await page.locator('tbody tr', { hasText: `#${id}` }).first().click()
  await expect(page.getByText(`Snapshot #${id}`).first()).toBeVisible()
}

async function searchFor(page: Page, text: string): Promise<void> {
  await page.reload()
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: /^search$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'File Search' })).toBeVisible()
  await page.getByPlaceholder(/filename contains/i).fill(text)
  await page.getByRole('button', { name: /^search$/i }).last().click()
}

test('browse: selecting one of two identical files selects that one only', async () => {
  await openSnapshot(h.page, 2)
  await descendTo(h.page, h.work)
  await h.page.getByText('twin-a', { exact: true }).click()
  await expect(h.page.locator('tbody tr[data-path$="/src/twin-a/same.txt"]')).toBeVisible()
  await h.page.getByText('twin-b', { exact: true }).click()
  await expect(h.page.locator('tbody tr[data-path$="/src/twin-b/same.txt"]')).toBeVisible()

  await h.page.locator('tbody tr[data-path$="/src/twin-b/same.txt"]').click()
  await expect(selectedRows(h.page)).toHaveCount(1)
  await expect(selectedRows(h.page)).toHaveAttribute('data-path', /\/src\/twin-b\/same\.txt$/)
  /* The card names the copy that was clicked. */
  await expect(h.page.getByText(/\/src\/twin-b\/same\.txt$/).first()).toBeVisible()

  await h.page.screenshot({ path: 'test-results/shared-ids-twin-selection.png', fullPage: true })
  await h.page.getByRole('button', { name: 'Close' }).click()
  await expect(selectedRows(h.page)).toHaveCount(0)
})

test('browse: a selection does not follow the file into another snapshot', async () => {
  /* notes.txt is unchanged between the two snapshots, so it is the same
   * entry row, and the same id, in both. */
  await openSnapshot(h.page, 2)
  await descendTo(h.page, h.work)
  await h.page.locator('tbody tr[data-path$="/src/notes.txt"]').click()
  await expect(h.page.getByText('View Content')).toBeVisible()

  await openSnapshot(h.page, 1)
  await descendTo(h.page, h.work)
  await expect(h.page.locator('tbody tr[data-path$="/src/notes.txt"]')).toBeVisible()
  await expect(selectedRows(h.page)).toHaveCount(0)
  await expect(h.page.getByText('View Content')).toHaveCount(0)
})

test('search: opening one copy highlights that copy only', async () => {
  await searchFor(h.page, 'same.txt')
  /* One result per path: the two names stay two results. */
  const results = h.page.locator('tbody tr', { hasText: 'same.txt' })
  await expect(results).toHaveCount(2)

  await h.page.locator('tbody tr', { hasText: '/src/twin-b/same.txt' }).first().click()
  await expect(h.page.getByText(/Snapshot #/).first()).toBeVisible()
  await expect(highlightedRows(h.page)).toHaveCount(1)
  await expect(highlightedRows(h.page)).toHaveAttribute('data-path', /\/src\/twin-b\/same\.txt$/)
  await expect(h.page.locator('tbody tr[data-path$="/src/twin-a/same.txt"]')).toHaveCount(0)
  await h.page.screenshot({ path: 'test-results/shared-ids-search-highlight.png', fullPage: true })
})

test('search: a directory result opens that directory', async () => {
  /* A directory's id is its own in each snapshot, and the result carries
   * the newest snapshot's, so the browser can open it. */
  await searchFor(h.page, 'twin-b')
  const row = h.page.locator('tbody tr', { hasText: '/src/twin-b' }).first()
  await expect(row).toBeVisible()
  await row.click()

  await expect(highlightedRows(h.page)).toHaveCount(1)
  await expect(highlightedRows(h.page)).toHaveAttribute('data-path', /\/src\/twin-b$/)
  await expect(h.page.locator('tbody tr[data-path$="/src/twin-b/same.txt"]')).toBeVisible()
})
