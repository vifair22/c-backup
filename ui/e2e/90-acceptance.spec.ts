import { test, expect, _electron as electron, type ElectronApplication, type Page } from '@playwright/test'
import { mkdtempSync, writeFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

/*
 * The UI half of docs/acceptance.md.
 *
 * Unlike every other spec here, this one seeds nothing and starts no daemon.
 * It attaches the app to a daemon that is already running against real
 * repositories, so the numbers it records are the app driving the same data
 * the CLI half drove - which is the only way the two columns of the
 * checklist are comparable.
 *
 * Skipped unless CBACKUP_ACCEPTANCE_SOCKET points at that daemon, so a
 * normal `npx playwright test` run is unaffected.
 */

const SOCKET = process.env.CBACKUP_ACCEPTANCE_SOCKET ?? ''
const APP_MAIN = resolve(__dirname, '..', 'out', 'main', 'index.js')

test.describe.configure({ mode: 'serial' })
test.skip(!SOCKET, 'set CBACKUP_ACCEPTANCE_SOCKET to run the acceptance pass')

interface Row { id: string; what: string; ms: number; note: string }
const rows: Row[] = []

let app: ElectronApplication
let page: Page

async function timed(id: string, what: string, fn: () => Promise<void>, note = ''): Promise<number> {
  const t0 = Date.now()
  await fn()
  const ms = Date.now() - t0
  rows.push({ id, what, ms, note })
  return ms
}

test.beforeAll(async () => {
  if (!SOCKET) return
  if (!existsSync(APP_MAIN)) throw new Error(`run 'npm run build' first: ${APP_MAIN}`)
  const userData = mkdtempSync(join(tmpdir(), 'cbackup-acc-ui-'))
  writeFileSync(join(userData, 'config.json'), JSON.stringify({
    connections: [{ type: 'local', name: 'Acceptance', socketPath: SOCKET }],
  }, null, 2))
  app = await electron.launch({
    args: [APP_MAIN, `--user-data-dir=${userData}`, '--no-sandbox'],
    env: { ...process.env, NODE_ENV: 'production' },
  })
  page = await app.firstWindow()
  await page.waitForLoadState('domcontentloaded')
})

test.afterAll(async () => {
  if (rows.length) {
    const w = Math.max(...rows.map(r => r.what.length))
    console.log('\n  UI acceptance timings')
    for (const r of rows)
      console.log(`    ${r.id.padEnd(5)} ${r.what.padEnd(w)}  ${String(r.ms).padStart(6)} ms  ${r.note}`)
  }
  await app?.close().catch(() => {})
})

test('A2/A3 the app sees the real repositories', async () => {
  await expect(page.getByText('Acceptance')).toBeVisible()
  await timed('A2', 'list repositories', async () => {
    await expect(page.getByText('Documents', { exact: true })).toBeVisible()
    await expect(page.getByText('Git', { exact: true })).toBeVisible()
    await expect(page.getByText('Encrypted', { exact: true })).toBeVisible()
  })
})

test('A3/I3 the dashboard of a 6.8 GB repository', async () => {
  const ms = await timed('I3', 'open Documents dashboard', async () => {
    await page.getByText('Documents', { exact: true }).first().click()
    await expect(page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
  })
  expect(ms, 'a dashboard is a summary query, size should not matter').toBeLessThan(1000)
  await page.screenshot({ path: 'test-results/acc-dashboard.png', fullPage: true })
})

test('C1/C2 the snapshot list', async () => {
  const ms = await timed('C1', 'open snapshot list', async () => {
    await page.getByRole('button', { name: /^snapshots$/i }).first().click()
    await expect(page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
    await expect(page.getByText('#1', { exact: true })).toBeVisible()
  })
  expect(ms).toBeLessThan(1000)
  /* The sizes and file counts are the real ones, not blanks. */
  const row = page.locator('tr', { hasText: '#3' }).first()
  await expect(row.locator('td').nth(3)).not.toHaveText('-')
  await page.screenshot({ path: 'test-results/acc-snapshots.png', fullPage: true })
})

test('C3/C4 browsing into a real tree', async () => {
  await timed('C3', 'open a snapshot tree', async () => {
    await page.locator('tr', { hasText: '#3' }).first().click()
    await expect(page.getByText('Snapshot #3').first()).toBeVisible()
    await expect(page.locator('tbody tr').first()).toBeVisible()
  })

  /* Down to the real Documents directory: home > vifair > Documents. */
  for (const seg of ['home', 'vifair', 'Documents']) {
    await timed('C4', `expand ${seg}`, async () => {
      await page.getByText(seg, { exact: true }).first().click()
      await page.waitForTimeout(50)
    })
  }
  await expect(page.locator('tbody tr').nth(4)).toBeVisible()
  await page.screenshot({ path: 'test-results/acc-browse.png', fullPage: true })
})

test('C5 searching a real index', async () => {
  await page.getByRole('button', { name: /back/i }).first().click()
  await page.getByRole('button', { name: /dashboard/i }).first().click()
  await page.getByRole('button', { name: /^search$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'File Search' })).toBeVisible()

  const ms = await timed('C5', 'search a real index', async () => {
    await page.getByPlaceholder(/filename contains/i).fill('Scan2025')
    await page.getByRole('button', { name: /^search$/i }).last().click()
    await expect(page.locator('tbody tr').first()).toBeVisible()
  })
  expect(ms, 'the name index should not care how much data is behind it').toBeLessThan(1000)
  await expect(page.locator('tbody tr').first()).toContainText('/home/vifair/Documents/')
  await page.screenshot({ path: 'test-results/acc-search.png', fullPage: true })
})

test('I5 the daemon screen against real repositories', async () => {
  await page.getByRole('button', { name: /dashboard/i }).first().click()
  await page.getByText('Acceptance', { exact: false }).first().click({ button: 'right' })
  await page.getByText('Daemon status').click()
  const ms = await timed('I5', 'open daemon status', async () => {
    await expect(page.getByRole('heading', { name: 'Daemon' })).toBeVisible()
    await expect(page.getByText('Newest snapshot', { exact: true })).toBeVisible()
  })
  expect(ms).toBeLessThan(1000)
  await page.screenshot({ path: 'test-results/acc-daemon.png', fullPage: true })
})
