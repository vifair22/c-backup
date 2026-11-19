import { test, expect } from '@playwright/test'
import { request } from 'node:http'
import { startHarness, openRepo, cli, type Harness } from './fixtures'

/*
 * B240: enabling a template the helper's policy does not allow is refused
 * with a 409 that carries the line to add as its own field, policy_line.
 * The app used to cut that line out of the error's text after a fixed
 * phrase, so rewording the daemon's message would have silently lost it.
 * Here the line on screen is checked against the field the daemon sends,
 * through the real helper.
 */

let h: Harness

interface Finding { id: number; engine: string; state: string; scope: string }

function pgFinding(): Finding {
  const out = cli(h.sock, ['db', 'list', String(h.repos.plain), '--limit', '100'])
  const f = (JSON.parse(out) as { findings: Finding[] }).findings.find(x => x.engine === 'postgres')
  if (!f) throw new Error(`no postgres finding: ${out}`)
  return f
}

/* A POST over the daemon's socket: the status and the parsed body. */
function post(path: string, body: unknown): Promise<{ status: number; body: unknown }> {
  return new Promise((done, fail) => {
    const data = JSON.stringify(body)
    const req = request({ socketPath: h.sock, path, method: 'POST',
                          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } },
      (res) => {
        let text = ''
        res.setEncoding('utf-8')
        res.on('data', (c: string) => { text += c })
        res.on('end', () => {
          try { done({ status: res.statusCode ?? 0, body: JSON.parse(text) }) } catch (e) { fail(e) }
        })
      })
    req.on('error', fail)
    req.end(data)
  })
}

test.beforeAll(async () => {
  h = await startHarness({ units: true, unitDb: true })
  const out = cli(h.sock, ['unit', 'backup', String(h.repos.plain), '--provider', 'fake', 'cbk-uat-pg'])
  if (!out.includes('"engine":"postgres"')) throw new Error(`no finding from the unit backup: ${out}`)
})
test.afterAll(async () => { await h?.close() })

test('databases: a template the helper refuses shows the policy line from its field', async () => {
  const f = pgFinding()
  /* What the daemon itself answers, so the screen is held to its field. */
  const refusal = await post(`/repos/${h.repos.plain}/databases/${f.id}/enable`, { template: 'pg-dumpall' })
  expect(refusal.status).toBe(409)
  const line = (refusal.body as { policy_line?: string }).policy_line ?? ''
  expect(line).toContain('allow_templates')

  await openRepo(h.page, 'Documents')
  await h.page.getByRole('button', { name: /^databases/i }).first().click()
  const row = h.page.locator(`.db-card-actions[data-finding-id="${f.id}"]`)
  await row.getByRole('button', { name: 'Preview' }).click()
  const enable = row.getByRole('button', { name: /^enable/i })
  await expect(enable).toBeEnabled({ timeout: 15_000 })
  await enable.click()
  await expect(h.page.getByText("The helper's policy does not allow this template.", { exact: true })).toBeVisible({ timeout: 10_000 })
  const shown = h.page.getByText("Add to the helper's policy:").locator('..').locator('code')
  await expect(shown).toHaveText(line)
  await h.page.screenshot({ path: 'test-results/11-db-policy-refused.png', fullPage: true })
  expect(pgFinding().state).not.toBe('enabled')
})
