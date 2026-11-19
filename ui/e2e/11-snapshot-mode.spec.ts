import { test, expect, type Locator } from '@playwright/test'
import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * Source snapshot mode in the backup dialog and the job view (Manual 6.6).
 *
 * The harness's daemon has no helper configured, which is exactly the case
 * worth seeing: only the helper can take a snapshot, so a source in strict
 * mode fails the backup before anything is read, and one in warn mode is
 * read live with the job saying so. Both are what the daemon computes; the
 * app only has to show them. The toggle is checked through the daemon's own
 * configuration, not the screen alone.
 */

let h: Harness
let source = ''

test.beforeAll(async () => {
  h = await startHarness()
  source = JSON.parse(cli(h.sock, ['sources', 'list', String(h.repos.plain)])).sources[0]
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

async function setMode(mode: 'off' | 'strict' | 'warn'): Promise<void> {
  await h.page.getByRole('button', { name: /backup/i }).first().click()
  await h.page.getByRole('button', { name: /^edit$/i }).first().click()
  await h.page.getByLabel(`Snapshot mode for ${source}`).selectOption(mode)
  await h.page.getByRole('button', { name: /^save$/i }).first().click()
  await expect(h.page.getByRole('button', { name: /^edit$/i }).first()).toBeVisible({ timeout: 10_000 })
}

test('backup dialog: a source set to strict snapshot mode is saved and shown', async () => {
  await openRepo(h.page, 'Documents')
  await setMode('strict')
  await expect(h.page.getByText('snapshot: strict').first()).toBeVisible()
  const listed = JSON.parse(cli(h.sock, ['sources', 'list', String(h.repos.plain)]))
  expect(listed.snapshot).toEqual([{ path: source, mode: 'strict' }])
  await h.page.screenshot({ path: 'test-results/11-snapshot-strict-dialog.png', fullPage: true })
})

test('job view: a strict source with no snapshot fails the backup and says what to do', async () => {
  await h.page.getByRole('button', { name: /start backup/i }).click()
  await expect(h.page.getByText('strict snapshot mode', { exact: false }).first()).toBeVisible({ timeout: 20_000 })
  await expect(h.page.getByText('no helper is configured', { exact: false }).first()).toBeVisible()
  await expect(h.page.getByTestId('snapshot-strict-hint').first()).toBeVisible()
  await h.page.screenshot({ path: 'test-results/11-snapshot-strict-refusal.png', fullPage: true })
  await h.page.getByRole('button', { name: /back/i }).first().click()
})

test('job view: a warn source is read live and the job lists it with the reason', async () => {
  await setMode('warn')
  await expect(h.page.getByText('snapshot: warn').first()).toBeVisible()
  await h.page.getByRole('button', { name: /start backup/i }).click()
  const panel = h.page.getByTestId('snapshot-live-sources').first()
  await expect(panel).toBeVisible({ timeout: 20_000 })
  await expect(panel).toContainText(source)
  await expect(panel).toContainText('no helper is configured')
  await h.page.screenshot({ path: 'test-results/11-snapshot-warn-live.png', fullPage: true })
  await h.page.getByRole('button', { name: /back/i }).first().click()
})

test('backup dialog: setting a source back to off clears its snapshot mode', async () => {
  await setMode('off')
  await expect(h.page.getByText('snapshot: warn')).toHaveCount(0)
  const listed = JSON.parse(cli(h.sock, ['sources', 'list', String(h.repos.plain)]))
  expect(listed.snapshot).toEqual([])
  await h.page.getByRole('button', { name: /^cancel$/i }).first().click()
})

/* Where an element's box ends, against where the box clipping it does. */
async function rightEdges(inner: Locator, outer: Locator): Promise<{ inner: number; outer: number }> {
  const a = await inner.boundingBox()
  const b = await outer.boundingBox()
  if (!a || !b) throw new Error('no layout box')
  return { inner: a.x + a.width, outer: b.x + b.width }
}

test('backup dialog: a long source path gives way to its snapshot badge (Demo L2)', async () => {
  /* About 40 characters, which used to cut the badge to "sna...", and one
   * far too long for the dialog, whose path has to ellipsize instead. */
  const near = join(h.work, 'short-src-dir-x')
  const long = join(h.work, 'a-source-directory-with-a-name-long-enough-to-overflow')
  mkdirSync(near, { recursive: true })
  mkdirSync(long, { recursive: true })
  expect(near.length).toBeGreaterThanOrEqual(38)
  cli(h.sock, ['sources', 'set', String(h.repos.plain), near, long,
               '--snapshot', near, '--snapshot-warn', long])

  await h.page.getByRole('button', { name: /backup/i }).first().click()
  for (const [p, mode] of [[near, 'strict'], [long, 'warn']] as const) {
    const row = h.page.locator('li', { hasText: p })
    const badge = row.getByText(`snapshot: ${mode}`, { exact: true })
    await expect(badge).toBeVisible({ timeout: 10_000 })
    /* Inside the row that clips it, and not cut short in itself. */
    const e = await rightEdges(badge, row)
    expect(e.inner).toBeLessThanOrEqual(e.outer + 0.5)
    expect(await badge.evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true)
  }
  /* The long path is the one that gives, and says so with an ellipsis
   * rather than overrunning; its title still has all of it. */
  const longPath = h.page.locator('li', { hasText: long }).getByTestId('backup-source-path')
  await expect(longPath).toHaveAttribute('title', long)
  expect(await longPath.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true)
  expect(await longPath.evaluate(el => getComputedStyle(el).textOverflow)).toBe('ellipsis')
  await h.page.screenshot({ path: 'test-results/11-snapshot-long-source.png', fullPage: true })
  await h.page.getByRole('button', { name: /^cancel$/i }).first().click()
})
