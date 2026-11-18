import { test, expect, type Locator } from '@playwright/test'
import { existsSync, readFileSync, statSync, writeFileSync, mkdirSync, rmSync, readdirSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { startHarness, openRepo, cli, unitPaths, type Harness } from './fixtures'

/*
 * Unit restore and NBD exports in the app (Manual 22.7, 22.8), against a
 * real daemon and the real helper serving the fake provider: the files to
 * a directory, one disk to a file, the unit under a new name and in place,
 * the daemon's refusals shown whole (a missing target root, a target root
 * already holding the unit's paths, and the helper's B188 and B210
 * refusals, the last offering the restore to a path), and a disk exported,
 * listed and revoked. Each step is checked on disk or through the CLI.
 */

let h: Harness
let vmSnap = 0
let webSnap = 0

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

function snapOf(unit: string): number {
  const out = cli(h.sock, ['unit', 'history', String(h.repos.plain), '--provider', 'fake', unit])
  return (JSON.parse(out) as { snapshots: Array<{ id: number }> }).snapshots[0].id
}

function dialog(): Locator {
  return h.page.getByRole('dialog', { name: 'Restore unit snapshot' })
}

async function openUnit(name: string): Promise<void> {
  const { page } = h
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: 'Units', exact: true }).click()
  await page.locator(`tr.unit-row[data-unit="${name}"]`).click()
  await expect(page.getByRole('heading', { name })).toBeVisible({ timeout: 10_000 })
}

async function openRestore(snap: number): Promise<void> {
  await h.page.locator(`tr.unit-snapshot[data-snapshot="${snap}"]`).getByRole('button', { name: 'Restore' }).click()
  await expect(dialog()).toBeVisible()
}

async function closeDialog(): Promise<void> {
  await dialog().getByRole('button', { name: /^(Close|Cancel)$/ }).click()
  await expect(dialog()).toHaveCount(0)
}

test.beforeAll(async () => {
  h = await startHarness({ units: true })
  const out = cli(h.sock, ['unit', 'backup', String(h.repos.plain), '--provider', 'fake', '--all'])
  if (!out.includes('"ok":2')) throw new Error(`unit backup failed: ${out}`)
  vmSnap = snapOf('vm1')
  webSnap = snapOf('web')
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('unit restore: the whole snapshot as files, under a directory', async () => {
  await openUnit('cbk-uat-web')
  await openRestore(webSnap)
  const dest = join(h.work, 'restore-files')
  await dialog().getByLabel('Destination', { exact: true }).fill(dest)
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  await expect(dialog().locator('.unit-restore-result')).toContainText('Restored', { timeout: 30_000 })
  const motd = join(dest + unitPaths(h.work).lxcLive, 'etc', 'motd')
  expect(readFileSync(motd, 'utf-8')).toBe('restored from the snapshot\n')
  expect(existsSync(join(dest, '.cbackup-unit', 'config', 'lxc', 'config'))).toBe(true)
  await closeDialog()
})

test('unit restore: one disk, to a new file', async () => {
  await openUnit('cbk-uat-vm')
  await openRestore(vmSnap)
  await dialog().getByLabel('One item, to a file').check()
  await dialog().getByLabel('Item', { exact: true }).selectOption('vda')
  const dest = join(h.work, 'vda.raw')
  await dialog().getByLabel('Destination', { exact: true }).fill(dest)
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  await expect(dialog().locator('.unit-restore-result')).toContainText('4.0 MB written', { timeout: 30_000 })
  expect(statSync(dest).size).toBe(4194304)
  await closeDialog()
})

test('unit restore refused: a new name without a target root, in the daemon\'s words', async () => {
  await openUnit('cbk-uat-web')
  await openRestore(webSnap)
  await dialog().getByLabel('As a new unit, under a new name').check()
  await dialog().getByLabel('New name', { exact: true }).fill('cbk-uat-web2')
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  await expect(dialog().locator('.unit-restore-error')).toContainText(
    'a restore under a new name needs target_root: its data goes under target_root/<original path>, never onto the paths of web')
})

test('unit restore refused: a target root that already holds the unit\'s paths', async () => {
  const troot = join(h.work, 'occupied')
  mkdirSync(troot + unitPaths(h.work).lxcLive, { recursive: true })
  await dialog().getByLabel('Target root', { exact: true }).fill(troot)
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  await expect(dialog().locator('.unit-restore-error')).toContainText(
    `unit restore: ${troot}${unitPaths(h.work).lxcLive} exists; the target root must not hold the unit's paths yet`,
    { timeout: 30_000 })
})

test('unit restore refused: the helper\'s host-bind rule (B188), whole, nothing written', async () => {
  const refusal = 'docker: refused, nothing created: the copy would bind /etc/ssl -> /etc/ssl: host configuration under /etc'
  writeFileSync(unitPaths(h.work).refuse, refusal)
  await dialog().getByLabel('Target root', { exact: true }).fill(join(h.work, 'copy-b188'))
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  const err = dialog().locator('.unit-restore-error')
  await expect(err).toContainText(`unit restore refused, nothing written: helper: failed: ${refusal}`, { timeout: 30_000 })
  await expect(err).toContainText('Refused, nothing written')
  await expect(err.getByRole('button', { name: 'Restore the files to a path instead' })).toHaveCount(0)
  expect(existsSync(join(h.work, 'copy-b188'))).toBe(false)
})

test('unit restore refused: a block device it cannot verify (B210) offers the restore to a path', async () => {
  const live = unitPaths(h.work).lxcLive
  const refusal = `docker: refused, nothing created: the copy would bind ${live} -> /data: cannot verify it is not ` +
    'a block device: the original is gone from the host and the snapshot has no entry for it; restore the files ' +
    `to a path instead (c-backup restore <repo> <snapshot> <dir> --subpath ${live}, or c-backup unit restore ` +
    '<repo> <snapshot> <dir> for the whole unit) and create the container by hand'
  writeFileSync(unitPaths(h.work).refuse, refusal)
  await dialog().getByLabel('Target root', { exact: true }).fill(join(h.work, 'copy-b210'))
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  const err = dialog().locator('.unit-restore-error')
  await expect(err).toContainText(`unit restore refused, nothing written: helper: failed: ${refusal}`, { timeout: 30_000 })
  await h.page.screenshot({ path: 'test-results/09-unit-restore-refused.png', fullPage: true })
  await err.getByRole('button', { name: 'Restore the files to a path instead' }).click()
  await expect(dialog().getByLabel('Destination', { exact: true })).toBeVisible()
  await expect(dialog().getByLabel('Files, to a directory')).toBeChecked()
  rmSync(unitPaths(h.work).refuse)
})

test('unit restore: under a new name, its data under the target root', async () => {
  await dialog().getByLabel('As a new unit, under a new name').check()
  await dialog().getByLabel('New name', { exact: true }).fill('cbk-uat-web2')
  const troot = join(h.work, 'copy')
  await dialog().getByLabel('Target root', { exact: true }).fill(troot)
  await dialog().getByRole('button', { name: 'Start Restore' }).click()
  const res = dialog().locator('.unit-restore-result')
  await expect(res).toContainText('Defined as cbk-uat-web2', { timeout: 30_000 })
  await expect(res).toContainText(`restored to ${troot}`)
  /* The helper's warning, as the job carried it. */
  await expect(res).toContainText('fake define: nothing was created')
  const live = unitPaths(h.work).lxcLive
  expect(readFileSync(join(troot + live, 'etc', 'motd'), 'utf-8')).toBe('restored from the snapshot\n')
  expect(readFileSync(join(live, 'etc', 'motd'), 'utf-8')).toBe('the live copy\n')
  expect(readFileSync(join(unitPaths(h.work).helperState, 'fake-define-web.json'), 'utf-8')).toContain(troot)
  await closeDialog()
})

test('unit restore: in place needs the unit\'s name typed, then replaces its data', async () => {
  await openRestore(webSnap)
  await dialog().getByLabel('In place, replacing the unit').check()
  const go = dialog().getByRole('button', { name: 'Restore in place' })
  await expect(go).toBeDisabled()
  await dialog().getByLabel('Confirm unit name').fill('cbk-uat-we')
  await expect(go).toBeDisabled()
  await dialog().getByLabel('Confirm unit name').fill('cbk-uat-web')
  await expect(go).toBeEnabled()
  await h.page.screenshot({ path: 'test-results/09-unit-restore-in-place.png', fullPage: true })
  await go.click()
  const res = dialog().locator('.unit-restore-result')
  await expect(res).toContainText('Defined as web', { timeout: 30_000 })
  await expect(res).toContainText('old data moved to')
  const live = unitPaths(h.work).lxcLive
  expect(readFileSync(join(live, 'etc', 'motd'), 'utf-8')).toBe('restored from the snapshot\n')
  const aside = readdirSync(dirname(live)).filter(n => n.startsWith('rootfs.cbackup-old-'))
  expect(aside.length).toBe(1)
  expect(readFileSync(join(dirname(live), aside[0], 'etc', 'motd'), 'utf-8')).toBe('the live copy\n')
  await closeDialog()
  /* The fake define leaves the unit stopped, and the page says so. */
  await expect(h.page.locator('.unit-state')).toHaveText('stopped', { timeout: 10_000 })
})

test('exports: a lifetime the CLI would refuse is refused before asking', async () => {
  await openUnit('cbk-uat-vm')
  const tr = h.page.locator(`tr.unit-snapshot[data-snapshot="${vmSnap}"]`)
  await tr.getByRole('button', { name: 'Export' }).click()
  await h.page.getByLabel('Lifetime').fill('8d')
  await h.page.getByRole('button', { name: 'Create export' }).click()
  await expect(h.page.locator('.export-error')).toContainText('60 seconds to 7 days')
})

test('exports: a disk exported, listed with its socket, and revoked', async () => {
  const { page } = h
  await page.getByLabel('Lifetime').fill('10m')
  await page.getByRole('button', { name: 'Create export' }).click()
  const card = page.locator('.export-created')
  await expect(card).toContainText('open', { timeout: 15_000 })
  const listed = JSON.parse(cli(h.sock, ['vm', 'exports'])) as { exports: Array<{ id: number; snap_id: number; item: string; expires: number; created_at: number; socket: string }> }
  expect(listed.exports.length).toBe(1)
  const x = listed.exports[0]
  expect(x.snap_id).toBe(vmSnap)
  expect(x.item).toBe('vda')
  expect(x.expires - x.created_at).toBe(600)
  await expect(card).toContainText(x.socket)
  await expect(card).toContainText('qemu-nbd --read-only')
  /* The name: 32 hex digits, shown here and nowhere else. */
  await expect(card).toContainText(/[0-9a-f]{32}/)
  const row = page.locator(`tr.export-row[data-export="${x.id}"]`)
  await expect(row).toContainText(x.socket)
  await page.screenshot({ path: 'test-results/09-unit-export.png', fullPage: true })

  await row.getByRole('button', { name: 'Revoke' }).click()
  await page.getByRole('button', { name: 'Revoke', exact: true }).last().click()
  await expect(page.locator('.export-closed')).toContainText(`Export #${x.id} closed: revoked`, { timeout: 10_000 })
  await expect(row).toHaveCount(0)
  for (let i = 0; i < 20 && (JSON.parse(cli(h.sock, ['vm', 'exports'])) as { exports: unknown[] }).exports.length; i++) await sleep(100)
  expect((JSON.parse(cli(h.sock, ['vm', 'exports'])) as { exports: unknown[] }).exports.length).toBe(0)
  expect(cli(h.sock, ['vm', 'export-show', String(x.id)])).toContain('"close_reason":"revoked"')
})
