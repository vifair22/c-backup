import { test, expect } from '@playwright/test'
import { existsSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { startHarness, type Harness } from './fixtures'

/*
 * Editing a signed-in remote connection (B105).
 *
 * A rename rebuilt the connection, so a remote that was signed in and
 * browsing dropped to "(remote)" and sat disconnected until somebody clicked
 * Connect, even though its token had been kept. A rename is a label on the
 * same daemon: the connection should not notice. A new address is another
 * daemon, and the app should go there rather than wait to be asked.
 *
 * Serial, one daemon, each step on the state the last one left.
 */

const PASSWORD = 'correcthorsebattery'

let h: Harness

test.describe.configure({ mode: 'serial' })
test.beforeAll(async () => { h = await startHarness({ remote: { password: PASSWORD, seed: true } }) })
test.afterAll(async () => { await h?.close() })

function credentials(): string {
  const p = join(h.userData, 'credentials.json')
  return existsSync(p) ? readFileSync(p, 'utf-8') : ''
}

const connRow = (name: string) =>
  h.page.locator('strong', { hasText: new RegExp(`^${name}$`) }).first()

async function editConnection(name: string, edit: () => Promise<void>): Promise<void> {
  const { page } = h
  await connRow(name).click({ button: 'right' })
  await page.getByText('Edit', { exact: true }).click()
  await expect(page.getByText('Edit connection')).toBeVisible()
  await edit()
  await page.getByRole('button', { name: 'Save', exact: true }).click()
  await expect(page.getByText('Edit connection')).toHaveCount(0)
}

test('sign in to the seeded remote and open a repository', async () => {
  const { page } = h
  await page.getByRole('button', { name: 'Connect', exact: true }).click({ timeout: 15_000 })
  await expect(page.getByText('Sign in: nas')).toBeVisible({ timeout: 15_000 })
  await page.locator('input[type="password"]').fill(PASSWORD)
  await page.locator('form', { hasText: 'Sign in: nas' })
    .getByRole('button', { name: 'Sign in', exact: true }).click()
  await expect(page.getByText('(remote, connected)')).toBeVisible({ timeout: 15_000 })

  await page.getByText('Documents', { exact: true }).first().click()
  await expect(page.getByText('Last Backup', { exact: false }).first()).toBeVisible({ timeout: 15_000 })
  expect(credentials()).toContain('"nas"')
})

test('renaming it keeps it connected, under the new name, with nothing to click', async () => {
  const { page } = h
  await editConnection('nas', async () => {
    await page.getByPlaceholder('nas', { exact: true }).fill('office')
  })

  await expect(connRow('office')).toBeVisible()
  await expect(page.locator('strong', { hasText: /^nas$/ })).toHaveCount(0)
  await expect(page.getByText('(remote, connected)')).toBeVisible()
  /* The state B105 left it in, and the ways out of it. Checked after a poll
   * of the connection list has had time to run, so a status that decays a
   * moment later is caught too. */
  await page.waitForTimeout(3_500)
  await expect(page.getByText('(remote)', { exact: true })).toHaveCount(0)
  await expect(page.getByText('(remote, connected)')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Connect', exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toHaveCount(0)
  await expect(page.getByText(/^Sign in: /)).toHaveCount(0)
  await page.screenshot({ path: 'test-results/06-renamed.png' })

  /* The open repository stays open and still loads from the daemon. */
  await expect(page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
  await page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  await expect(page.locator('tr', { hasText: '#2' }).first()).toBeVisible({ timeout: 15_000 })
  await page.getByRole('button', { name: /dashboard/i }).first().click()

  /* The stored session moved with it. */
  const creds = credentials()
  expect(creds).toContain('"office"')
  expect(creds).not.toContain('"nas"')
  expect(readFileSync(join(h.userData, 'config.json'), 'utf-8')).toContain('"office"')
})

test('the renamed connection reconnects on its own after a restart', async () => {
  const page = await h.relaunch()
  await expect(page.getByText('(remote, connected)')).toBeVisible({ timeout: 15_000 })
  await expect(connRow('office')).toBeVisible()
  await expect(page.getByText('Documents', { exact: true }).first()).toBeVisible()
  await expect(page.getByText(/^Sign in: /)).toHaveCount(0)
})

test('a new address reconnects there straight away, without the old token', async () => {
  const { page } = h
  await editConnection('office', async () => {
    await page.locator('input[type="number"]').fill('1')
  })
  /* It tried the new address on its own: nothing was clicked after Save. */
  await expect(page.getByText(/no daemon at 127\.0\.0\.1:1/).first()).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText('(remote, connected)')).toHaveCount(0)
  expect(credentials()).not.toContain('"office"')
})
