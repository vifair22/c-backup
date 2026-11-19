import { test, expect, type Locator } from '@playwright/test'
import http from 'node:http'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * The rest of the unit features in the app, against a real daemon and the
 * real helper serving the fake provider: the providers and the helper on
 * the Units screen (`unit providers`), a unit's policy shown, set, refused
 * and cleared (`unit policy show|set`), a policy for a unit never backed
 * up, and the incremental chain with its reset (`vm checkpoints`, `vm
 * reset-incremental`). Each step is checked against what the daemon then
 * says.
 */

let h: Harness

/* A raw GET on the daemon's socket, for what the CLI cannot ask: `vm
 * checkpoints` names libvirt, and these units are the fake provider's. */
function get(path: string): Promise<{ status: number; body: Record<string, unknown> }> {
  return new Promise((done, fail) => {
    const req = http.request({ socketPath: h.sock, path, method: 'GET' }, res => {
      let t = ''
      res.on('data', c => { t += c })
      res.on('end', () => done({ status: res.statusCode ?? 0, body: JSON.parse(t) }))
    })
    req.on('error', fail)
    req.end()
  })
}

function policy(unit: string): { unit: Record<string, string>; effective: Record<string, number> } {
  return JSON.parse(cli(h.sock, ['unit', 'policy', 'show', String(h.repos.plain), '--provider', 'fake', unit]))
}

function policyRow(key: string): Locator {
  return h.page.locator(`tr.unit-policy-row[data-key="${key}"]`)
}

async function openUnit(name: string): Promise<void> {
  const { page } = h
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: 'Units', exact: true }).click()
  await page.locator(`tr.unit-row[data-unit="${name}"]`).click()
  await expect(page.getByRole('heading', { name })).toBeVisible({ timeout: 10_000 })
}

test.beforeAll(async () => {
  h = await startHarness({ units: true })
  const out = cli(h.sock, ['unit', 'backup', String(h.repos.plain), '--provider', 'fake', 'cbk-uat-vm'])
  if (!out.includes('"ok":1')) throw new Error(`unit backup failed: ${out}`)
})
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('unit providers: every provider, the one in use through the helper, and the helper itself', async () => {
  const { page } = h
  await openRepo(page, 'Documents')
  await page.getByRole('button', { name: 'Units', exact: true }).click()
  await page.getByRole('button', { name: 'Providers and helper' }).click()
  const daemon = JSON.parse(cli(h.sock, ['unit', 'providers'])) as {
    providers: Array<{ name: string; enabled: boolean }>; daemon_id: string; holds_open: number }
  const panel = page.locator('.providers-panel')
  for (const p of daemon.providers) {
    await expect(panel.locator(`tr.provider-row[data-provider="${p.name}"]`)).toContainText(p.enabled ? 'yes' : 'no')
  }
  const fake = panel.locator('tr.provider-row[data-provider="fake"]')
  await expect(fake).toContainText('helper (connected)')
  await expect(fake).toContainText('available')
  await expect(panel).toContainText(daemon.daemon_id)
  await expect(panel.locator('.holds-open')).toHaveText(String(daemon.holds_open))
  await expect(panel.locator('.helper-info')).toContainText('connected')
  await expect(panel.locator('.helper-info')).toContainText('offers fake')
  await page.screenshot({ path: 'test-results/10-unit-providers.png', fullPage: true })
})

test('unit policy: shown as the daemon has it, the repository\'s in force', async () => {
  await openUnit('cbk-uat-vm')
  const p = policy('vm1')
  expect(p.unit).toEqual({})
  for (const [k, v] of Object.entries(p.effective)) {
    await expect(policyRow(k)).toContainText('repository\'s')
    await expect(policyRow(k).locator('.effective')).toHaveText(String(v))
  }
})

test('unit policy: set a retention count, exclusions and a capture group', async () => {
  const { page } = h
  await page.locator('.unit-policy').getByRole('button', { name: 'Edit' }).click()
  await page.getByLabel('retention.keep_daily').fill('14')
  await page.getByLabel('capture.exclude').fill('/var/cache/*\n/tmp/*')
  await page.getByLabel('capture.group').fill('proxy')
  await page.getByRole('button', { name: 'Save policy' }).click()
  await expect(page.locator('.unit-policy')).toContainText('saved')
  const p = policy('vm1')
  expect(p.unit).toEqual({
    'retention.keep_daily': '14', 'capture.exclude': '/var/cache/*\n/tmp/*', 'capture.group': 'proxy',
  })
  expect(p.effective['retention.keep_daily']).toBe(14)
  await expect(policyRow('retention.keep_daily')).toContainText('14 (unit)')
  await expect(policyRow('retention.keep_daily').locator('.effective')).toHaveText('14')
  await expect(page.locator('.capture-group')).toHaveText('proxy')
  await page.screenshot({ path: 'test-results/10-unit-policy.png', fullPage: true })
})

test('unit policy refused: a group name the daemon does not take, in its words', async () => {
  const { page } = h
  await page.locator('.unit-policy').getByRole('button', { name: 'Edit' }).click()
  await page.getByLabel('capture.group').fill('Proxy')
  await page.getByRole('button', { name: 'Save policy' }).click()
  await expect(page.locator('.unit-policy-error')).toHaveText(
    'capture.group is a name of up to 63 of a-z 0-9 _ . -, or "" for none')
  expect(policy('vm1').unit['capture.group']).toBe('proxy')
})

test('unit policy: an emptied field drops the unit\'s own value', async () => {
  const { page } = h
  await page.getByLabel('capture.group').fill('proxy')
  await page.getByLabel('retention.keep_daily').fill('')
  await page.getByRole('button', { name: 'Save policy' }).click()
  await expect(page.locator('.unit-policy')).toContainText('saved retention.keep_daily')
  const p = policy('vm1')
  expect(p.unit['retention.keep_daily']).toBeUndefined()
  expect(p.effective['retention.keep_daily']).toBe(7)
  await expect(policyRow('retention.keep_daily')).toContainText('repository\'s')
})

test('unit policy: set for a unit never backed up here, stored under its id', async () => {
  const { page } = h
  await openUnit('cbk-uat-web')
  await page.locator('.unit-policy').getByRole('button', { name: 'Edit' }).click()
  await page.getByLabel('retention.keep_snaps').fill('3')
  await page.getByRole('button', { name: 'Save policy' }).click()
  await expect(policyRow('retention.keep_snaps')).toContainText('3 (unit)')
  const p = policy('web')
  expect(p.unit).toEqual({ 'retention.keep_snaps': '3' })
  expect(p.effective['retention.keep_snaps']).toBe(3)
})

test('vm checkpoints: the unit\'s chain as the daemon reports it', async () => {
  await openUnit('cbk-uat-vm')
  const { status, body } = await get(`/repos/${h.repos.plain}/units/fake/vm1/checkpoints`)
  expect(status).toBe(200)
  const inc = body.incremental as { enabled: boolean }
  const panel = h.page.locator('.unit-chain')
  await expect(panel.locator('.incremental-state')).toHaveText(inc.enabled ? 'on' : 'off')
  expect(body.tokens).toEqual([])
  await expect(panel).toContainText('No checkpoint: the next backup reads in full')
})

test('vm reset-incremental: confirmed, done, and journalled', async () => {
  const { page } = h
  const panel = page.locator('.unit-chain')
  await panel.getByRole('button', { name: 'Reset incrementals' }).click()
  await page.getByRole('button', { name: 'Reset', exact: true }).click()
  await expect(panel.locator('.unit-chain-reset')).toContainText('No failed audit had turned them off.')
  const j = cli(h.sock, ['journal', String(h.repos.plain)])
  expect(j).toContain('"operation":"reset-incremental"')
  expect(j).toContain('incrementals reset for fake:vm1')
  await expect(page.locator('tr.unit-journal').filter({ hasText: 'incrementals reset for fake:vm1' })).toBeVisible()
  await page.screenshot({ path: 'test-results/10-unit-chain.png', fullPage: true })
})
