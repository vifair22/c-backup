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

test('the content search screen opens instantly', async () => {
  /*
   * The search itself is slow by nature - it reads every file in a snapshot.
   * Opening the screen is not, and the two must not be confused: a screen
   * that waits for a search before it renders would make an honest delay
   * look like a broken app.
   */
  await h.page.getByText('Documents', { exact: true }).first().click()
  await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()

  const ms = await timed('open find-in-files', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /find in files/i }).first().click()
    await expect(h.page.getByText('Search file contents').first()).toBeVisible()
  })
  expect(ms).toBeLessThan(BUDGET_MS.navigation)

  /* Submitting is a handoff too: the daemon accepts the search and reads in
   * the background, so the click returns before any file is opened. */
  await h.page.getByPlaceholder(/text to look for/i).fill('quick brown')
  const submit = await timed('submit search', BUDGET_MS.submit, async () => {
    await h.page.getByRole('button', { name: /^search$/i }).click()
    await expect(h.page.getByText('Searching...').first()).toBeVisible()
  })
  expect(submit, 'submitting a search must not wait for the search').toBeLessThan(BUDGET_MS.submit)

  await expect(h.page.getByText('notes.txt', { exact: false }).first())
    .toBeVisible({ timeout: 20_000 })
  await h.page.getByRole('button', { name: /^back$/i }).first().click()
})

test('the snapshot list and the tree', async () => {
  /*
   * Both of these page rather than loading everything: a directory is an
   * index range scan and a snapshot list is one page of rows. The point of
   * timing them is that the moment either starts loading a whole snapshot
   * the number here moves, and nothing else would say so.
   */
  await h.page.getByText('Documents', { exact: true }).first().click()
  await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()

  const list = await timed('open snapshot list', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /^snapshots$/i }).first().click()
    await expect(h.page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  })
  expect(list).toBeLessThan(BUDGET_MS.navigation)

  const browse = await timed('open snapshot tree', BUDGET_MS.navigation, async () => {
    await h.page.locator('tr', { hasText: '#2' }).first().click()
    await expect(h.page.getByText('Snapshot #2').first()).toBeVisible()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(browse).toBeLessThan(BUDGET_MS.navigation)

  /* Expanding a directory is one request for one level. */
  const expand = await timed('expand a directory', BUDGET_MS.navigation, async () => {
    await h.page.locator('tbody tr').filter({ hasText: '/' }).last().click()
    await h.page.waitForTimeout(30)
  })
  expect(expand).toBeLessThan(BUDGET_MS.navigation)

  /* Back from the tree lands on the list it was opened from. */
  await h.page.getByRole('button', { name: /back/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Snapshots' })).toBeVisible()
  await h.page.getByRole('button', { name: /dashboard/i }).first().click()
  await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()
})

test('the introspection screens', async () => {
  /*
   * These read the index rather than the repository, so they are the ones
   * that should be fastest, and the ones where a regression would be a
   * missing index rather than a slow disk. The diff is the exception: it
   * walks manifest trees on the daemon side and is honest work, so it gets
   * the submit budget rather than the navigation one.
   */
  await h.page.getByText('Documents', { exact: true }).first().click()
  await expect(h.page.getByText('Last Backup', { exact: false }).first()).toBeVisible()

  const search = await timed('open file search', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /^search$/i }).first().click()
    await expect(h.page.getByRole('heading', { name: 'File Search' })).toBeVisible()
  })
  expect(search).toBeLessThan(BUDGET_MS.navigation)

  const query = await timed('run a name search', BUDGET_MS.submit, async () => {
    await h.page.getByPlaceholder(/filename contains/i).fill('notes')
    await h.page.getByRole('button', { name: /^search$/i }).last().click()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(query, 'a name search is an index lookup, not a scan').toBeLessThan(BUDGET_MS.submit)
  await h.page.getByRole('button', { name: /dashboard/i }).first().click()

  const journal = await timed('open activity', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /journal|activity/i }).first().click()
    await expect(h.page.getByRole('heading', { name: 'Activity' })).toBeVisible()
  })
  expect(journal).toBeLessThan(BUDGET_MS.navigation)
  await h.page.getByRole('button', { name: /back/i }).first().click()

  const packs = await timed('open packs', BUDGET_MS.navigation, async () => {
    await h.page.getByRole('button', { name: /pack/i }).first().click()
    await expect(h.page.getByRole('heading', { name: 'Packs' })).toBeVisible()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(packs).toBeLessThan(BUDGET_MS.navigation)
  await h.page.getByRole('button', { name: /back/i }).first().click()

  await h.page.getByRole('button', { name: /compare/i }).first().click()
  await expect(h.page.getByRole('heading', { name: 'Compare Snapshots' })).toBeVisible()
  /*
   * Against the first snapshot rather than the default pair. An earlier test
   * here runs a backup, so the two newest snapshots are identical and their
   * diff prunes at the root hash - the fastest case there is, and a useless
   * thing to time.
   */
  const diff = await timed('compare two snapshots', BUDGET_MS.submit, async () => {
    await h.page.locator('select').first().selectOption('1')
    await h.page.getByRole('button', { name: /^compare$/i }).click()
    await expect(h.page.locator('tbody tr').first()).toBeVisible()
  })
  expect(diff, 'a diff walks trees, but only the ones whose hashes differ').toBeLessThan(BUDGET_MS.submit)
  await h.page.getByRole('button', { name: /back/i }).first().click()
})

test('the daemon status screen', async () => {
  /*
   * Three requests and a text parse, so it should be no slower than a
   * screen that makes one. It also polls while open, which is the thing to
   * watch: a poll that cost real time would make the whole app feel heavy
   * while this screen sits in front.
   */
  await h.page.getByText('This machine', { exact: false }).first().click({ button: 'right' })
  await h.page.getByText('Daemon status').click()

  const ms = await timed('open daemon status', BUDGET_MS.navigation, async () => {
    await expect(h.page.getByRole('heading', { name: 'Daemon' })).toBeVisible()
    await expect(h.page.getByText('Newest snapshot', { exact: true })).toBeVisible()
  })
  expect(ms).toBeLessThan(BUDGET_MS.navigation)
  await h.page.getByRole('button', { name: /back/i }).first().click()
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
