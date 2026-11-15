import { test, expect } from '@playwright/test'
import { spawnSync } from 'node:child_process'
import { existsSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { startHarness, REPO_ROOT, type Harness } from './fixtures'

/*
 * The app against a daemon over TCP, the way it reaches one on another host.
 *
 * Every other spec connects over the unix socket, which takes no credential,
 * so none of them could see that remote access had never worked: the app
 * asked /readyz before it signed in and a configured daemon answered 401.
 * This drives the whole remote path from the window - add the connection,
 * sign in, browse, back up, restore, quit and reopen, sign out - against a
 * real daemon listening on loopback with an admin password set.
 *
 * Serial, one daemon: each step builds on the state the last one left, which
 * is also how a person meets these screens.
 */

const PASSWORD = 'correcthorsebattery'
const CLI = join(REPO_ROOT, 'build', 'bins', 'c-backup')

let h: Harness

test.describe.configure({ mode: 'serial' })
test.beforeAll(async () => { h = await startHarness({ remote: { password: PASSWORD, seed: false } }) })
test.afterAll(async () => { await h?.close() })

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function snapshotCount(): number {
  const r = spawnSync(CLI, ['--socket', h.sock, 'snapshots', String(h.repos.plain)],
                      { encoding: 'utf-8' })
  return (r.stdout.match(/"id":/g) ?? []).length
}

function credentials(): string {
  const p = join(h.userData, 'credentials.json')
  return existsSync(p) ? readFileSync(p, 'utf-8') : ''
}

/* The connection row in the sidebar, by its name. */
const connRow = () => h.page.locator('strong', { hasText: /^nas$/ }).first()

test('a remote connection added in the dialog asks for the password', async () => {
  const { page } = h
  await page.getByRole('button', { name: '+ Add' }).click()
  await page.getByPlaceholder('nas', { exact: true }).fill('nas')
  await page.getByRole('button', { name: 'Remote', exact: true }).click()
  await page.getByPlaceholder('nas.internal').fill('127.0.0.1')
  await page.locator('input[type="number"]').fill(String(h.port))
  await page.getByRole('button', { name: 'Add', exact: true }).click()

  await expect(page.getByText('Sign in: nas')).toBeVisible({ timeout: 15_000 })
  /* Staying signed in keeps a token and says so, whatever the keyring. */
  await expect(page.getByText(/Not the password\./)).toBeVisible()
  await page.screenshot({ path: 'test-results/05-sign-in.png' })
})

test('a wrong password is refused and the dialog stays open', async () => {
  const { page } = h
  await page.locator('input[type="password"]').fill('not the password')
  await page.getByRole('button', { name: 'Sign in', exact: true }).click()
  await expect(page.getByText(/login failed/)).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText('Sign in: nas')).toBeVisible()
  expect(credentials()).not.toContain('nas')
})

test('signing in connects and lists the repositories', async () => {
  const { page } = h
  await page.locator('input[type="password"]').fill(PASSWORD)
  await page.getByRole('button', { name: 'Sign in', exact: true }).click()

  await expect(page.getByText('Sign in: nas')).toHaveCount(0, { timeout: 15_000 })
  await expect(page.getByText('(remote, connected)')).toBeVisible()
  await expect(page.getByText('Documents', { exact: true }).first()).toBeVisible()
  await expect(page.getByText('Photos', { exact: true }).first()).toBeVisible()

  /* A token is kept, owner-only, and the password is not. */
  const creds = credentials()
  expect(creds).toContain('nas')
  expect(creds).not.toContain(PASSWORD)
  expect(statSync(join(h.userData, 'credentials.json')).mode & 0o777).toBe(0o600)
  expect(readFileSync(join(h.userData, 'config.json'), 'utf-8')).not.toContain(PASSWORD)
})

test('a repository opens and its snapshots browse over TCP', async () => {
  const { page } = h
  await page.getByText('Documents', { exact: true }).first().click()
  await expect(page.getByText('Last Backup', { exact: false }).first()).toBeVisible({ timeout: 15_000 })

  await page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  await page.locator('tr', { hasText: '#2' }).first().click()
  await expect(page.getByText('Snapshot #2').first()).toBeVisible()
  await expect(page.locator('tbody tr').first()).toBeVisible()
  await page.screenshot({ path: 'test-results/05-browse.png' })

  await page.getByRole('button', { name: /back/i }).first().click()
  await page.getByRole('button', { name: /dashboard/i }).first().click()
})

test('a backup started from the app runs on the remote daemon', async () => {
  const { page } = h
  const before = snapshotCount()
  await expect(page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
  await page.getByRole('button', { name: /run backup/i }).first().click()
  /* The sources shown are paths on the daemon's host, and the dialog says so. */
  await expect(page.getByText('/tmp/cbackup-e2e-', { exact: false }).first()).toBeVisible()
  await page.getByRole('button', { name: /start backup/i }).click()

  for (let i = 0; i < 100 && snapshotCount() <= before; i++) await sleep(200)
  expect(snapshotCount()).toBe(before + 1)
})

test('a restore started from the app writes on the remote daemon', async () => {
  const { page } = h
  const dest = join(h.work, 'restored')
  await page.getByRole('button', { name: /^snapshots$/i }).first().click()
  await expect(page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  await page.locator('tr', { hasText: '#1' }).first().click()
  await expect(page.getByText('Snapshot #1').first()).toBeVisible()

  await page.getByRole('button', { name: 'Restore', exact: true }).first().click()
  await expect(page.getByRole('heading', { name: 'Restore Snapshot' })).toBeVisible()
  await expect(page.getByText('Destination path on 127.0.0.1')).toBeVisible()
  await page.getByPlaceholder('/path/to/restore/destination').fill(dest)
  await page.getByRole('button', { name: 'Start Restore' }).click()

  /* The snapshot records absolute paths, so the restore lands under them. */
  const restored = resolve(dest + join(h.work, 'src', 'notes.txt'))
  for (let i = 0; i < 100 && !existsSync(restored); i++) await sleep(200)
  expect(existsSync(restored)).toBe(true)
  await sleep(300)
  expect(readFileSync(restored, 'utf-8'))
    .toBe(readFileSync(join(h.work, 'src', 'notes.txt'), 'utf-8'))
})

test('the session survives quitting and reopening the app', async () => {
  const page = await h.relaunch()
  await expect(page.getByText('(remote, connected)')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByText('Documents', { exact: true }).first()).toBeVisible()
  await expect(page.getByText(/^Sign in: /)).toHaveCount(0)
})

test('signing out revokes the session and forgets it', async () => {
  const { page } = h
  await connRow().click({ button: 'right' })
  await page.getByText('Sign out', { exact: true }).click()

  await expect(page.getByText('(remote, signed out)')).toBeVisible({ timeout: 15_000 })
  await expect(page.getByRole('button', { name: 'Sign in', exact: true })).toBeVisible()
  expect(credentials()).not.toContain('"token"')

  /* And it stays signed out across a restart, rather than finding a token.
   * A remote with nothing stored waits to be asked rather than prompting at
   * startup, so the way in is Connect, which then asks for the password. */
  const next = await h.relaunch()
  await expect(next.getByRole('button', { name: 'Connect', exact: true })).toBeVisible({ timeout: 15_000 })
  await expect(next.getByText('(remote, connected)')).toHaveCount(0)
  await next.getByRole('button', { name: 'Connect', exact: true }).click()
  await expect(next.getByText('Sign in: nas')).toBeVisible({ timeout: 15_000 })
})
