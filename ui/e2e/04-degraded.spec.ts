import { test, expect } from '@playwright/test'
import { startHarness, openRepo, type Harness } from './fixtures'

/*
 * What the app does when the daemon is not there.
 *
 * Everything else here tests the happy path against a daemon that answers.
 * This is the case that decides whether somebody can tell a broken app from
 * a stopped service, and it is the one nothing had ever exercised: the
 * transport recognised an absent daemon and said so during connect, and then
 * never checked again. A daemon that died after connecting left the sidebar
 * reporting "connected" while every request failed.
 *
 * Runs last and kills the daemon, so it needs its own harness.
 */

let h: Harness

test.beforeAll(async () => { h = await startHarness() })
test.afterAll(async () => { await h?.close() })

test.describe.configure({ mode: 'serial' })

test('a daemon that dies is reported as gone, not as connected', async () => {
  await openRepo(h.page, 'Documents')
  await expect(h.page.getByText('connected').first()).toBeVisible()

  h.killDaemon()

  /*
   * Any request will do; the point is that the failure changes the connection
   * status rather than only printing an error into the page. Opening a
   * screen is what somebody would actually do next.
   */
  await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()

  /* The message names the daemon and how to start it. "HTTP 0" or a raw
   * ENOENT would leave somebody debugging the wrong thing. */
  await expect(h.page.getByText(/no daemon at/i).first()).toBeVisible({ timeout: 15_000 })
  await expect(h.page.getByText(/systemctl start c-backupd/i).first()).toBeVisible()

  /* And the sidebar stops claiming otherwise. */
  await expect(h.page.getByText('connected')).toHaveCount(0)

  /*
   * Electron rewrites a main-process error into "Error invoking remote
   * method 'api:call': ...", which read like the app itself was broken
   * rather than like a service being down. The message a person sees is the
   * message, not the channel it arrived on.
   */
  await expect(h.page.getByText(/invoking remote method/i)).toHaveCount(0)

  /* And the screen does not report a count for a question that was never
   * answered: "0 total" beside a connection error reads as an empty
   * repository. */
  await expect(h.page.getByText('0 total')).toHaveCount(0)

  await h.page.screenshot({ path: 'test-results/daemon-gone.png', fullPage: true })
})

test('the daemon screen says it is not answering', async () => {
  await h.page.reload()
  await h.page.getByText('This machine', { exact: false }).first().click({ button: 'right' })

  /*
   * Daemon status is offered only on a connected connection, which is
   * correct: it cannot report on a daemon it cannot reach. The connection
   * shows its error instead, which is the useful thing at that point.
   */
  await expect(h.page.getByText('Daemon status')).toHaveCount(0)
  await h.page.keyboard.press('Escape')
  await expect(h.page.getByText(/no daemon at|not running|error/i).first()).toBeVisible()
})
