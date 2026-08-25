import { test, expect } from '@playwright/test'
import { startHarness, timed, reportTimings, timings, BUDGET_MS, type Harness } from './fixtures'

/*
 * How fast the app answers.
 *
 * Everything the desktop app talks to is a daemon on the same machine over a
 * unix socket, so there is no network to blame: a screen that takes half a
 * second is doing something it did not need to do. That is not visible by
 * using the app - it feels fine until it does not - so it gets measured, and
 * the numbers are printed whether or not anything failed.
 *
 * The interactions here are the ones a person actually performs, in the order
 * they perform them. What is asserted is the click-to-answer time, because
 * that is what somebody experiences. Work that is genuinely slow - a backup,
 * a scrub - is a job by design: the assertion is that handing it over is
 * fast and that progress appears, not that the work finishes quickly.
 */

let h: Harness

test.beforeAll(async () => { h = await startHarness() })
test.afterAll(async () => {
  reportTimings()
  await h?.close()
})

test.describe.configure({ mode: 'serial' })

test('opening a repository', async () => {
  const ms = await timed('open repository', BUDGET_MS.navigation, async () => {
    await h.page.getByText('Documents', { exact: true }).first().click()
    await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
  })
  expect(ms, 'opening a repository should not feel like waiting').toBeLessThan(BUDGET_MS.navigation)
})

test('moving between screens', async () => {
  const screens: [string, RegExp, RegExp][] = [
    ['schedules',  /schedules/i,  /Schedules/],
    ['integrity',  /integrity/i,  /Integrity checks/],
    ['encryption', /encryption/i, /Encryption/],
  ]

  for (const [name, button, heading] of screens) {
    const ms = await timed(`open ${name}`, BUDGET_MS.navigation, async () => {
      await h.page.getByRole('button', { name: button }).first().click()
      await expect(h.page.getByText(heading).first()).toBeVisible()
    })
    expect(ms, `${name} should open promptly`).toBeLessThan(BUDGET_MS.navigation)

    await timed(`back from ${name}`, BUDGET_MS.navigation, async () => {
      await h.page.getByRole('button', { name: /^back$/i }).first().click()
      await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
    })
  }
})

test('opening the backup dialog', async () => {
  /* This one reads the repository's sources before it can render, so it is
   * the dialog most likely to stall. */
  const ms = await timed('open backup dialog', BUDGET_MS.dialog, async () => {
    await h.page.getByRole('button', { name: /run backup/i }).first().click()
    await expect(h.page.getByText('Sources', { exact: false }).first()).toBeVisible()
    await expect(h.page.getByText('/tmp/cbackup-e2e-', { exact: false }).first()).toBeVisible()
  })
  expect(ms, 'the backup dialog reads sources and should still be quick').toBeLessThan(BUDGET_MS.dialog)
})

test('starting a backup hands over immediately', async () => {
  /*
   * The backup itself takes as long as it takes. What must not take long is
   * the handoff: the daemon queues a job and answers with its id, so the
   * dialog should close and the jobs view appear without waiting for any
   * work to happen.
   */
  const ms = await timed('start backup', BUDGET_MS.submit, async () => {
    await h.page.getByRole('button', { name: /start backup/i }).click()
    await expect(h.page.getByText('Sources', { exact: false })).toHaveCount(0)
  })
  expect(ms, 'starting a job must not wait for the job').toBeLessThan(BUDGET_MS.submit)
})

test('switching between repositories', async () => {
  const ms = await timed('switch repository', BUDGET_MS.navigation, async () => {
    await h.page.getByText('Photos', { exact: true }).first().click()
    await expect(h.page.getByText('Photos').first()).toBeVisible()
  })
  expect(ms, 'switching repositories should not stall').toBeLessThan(BUDGET_MS.navigation)
})

test('nothing measured was over budget', async () => {
  /*
   * A single place to fail, so a run reports every slow interaction rather
   * than stopping at the first. Reading the whole list is what tells you
   * whether one screen is slow or the app is.
   */
  const over = timings.filter(t => t.ms > t.budget)
  expect(over.map(t => `${t.label}: ${t.ms}ms > ${t.budget}ms`), 'interactions over budget').toEqual([])
})
