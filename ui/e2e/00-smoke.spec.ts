import { test, expect } from '@playwright/test'
import { startHarness, type Harness } from './fixtures'

/* Does the app start and see the daemon at all? Everything else assumes it. */
test('the app launches and lists the seeded repositories', async () => {
  const h: Harness = await startHarness()
  try {
    await expect(h.page.getByText('Documents').first()).toBeVisible({ timeout: 15_000 })
    await expect(h.page.getByText('Photos').first()).toBeVisible()
    await h.page.screenshot({ path: 'test-results/00-launch.png', fullPage: true })
  } finally {
    await h.close()
  }
})
