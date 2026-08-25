import { defineConfig } from '@playwright/test'

/*
 * Playwright against the packaged Electron app.
 *
 * No webServer and no baseURL: this is a desktop application, so each spec
 * launches the built main process itself and drives the window it opens.
 * The daemon it talks to is started per spec by e2e/fixtures.ts, on a unix
 * socket in a temp directory, with real repositories behind it.
 *
 * Screenshots are written on success as well as failure. The failure ones
 * are for debugging; the success ones are the point - a screen is easier to
 * judge from a picture than from a diff, and test-results/ is where the
 * pictures for a review live.
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  timeout: 60_000,
  reporter: process.env.CI
    ? [['junit', { outputFile: 'test-results/e2e.xml' }], ['list']]
    : 'list',
  outputDir: 'test-results/playwright',
  use: {
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
})
