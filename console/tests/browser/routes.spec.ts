import { expect, test } from '@playwright/test';

const routes = [
  ['/', 'Overview'],
  ['/jobs', 'Job Detail'],
  ['/timeline', 'Timeline'],
  ['/topology', 'Topology'],
  ['/sandboxes', 'Sandbox View'],
  ['/decisions', 'Decision Explorer'],
  ['/experiments', 'Experiment Compare'],
] as const;

for (const [path, heading] of routes) {
  test(`${heading} route renders without browser errors`, async ({ page }) => {
    const errors: string[] = [];
    page.on('console', (message) => {
      if (message.type() === 'error') errors.push(message.text());
    });
    page.on('pageerror', (error) => errors.push(error.message));

    const response = await page.goto(path);

    expect(response?.ok()).toBe(true);
    await expect(page.getByRole('heading', { name: heading, level: 1 })).toBeVisible();
    await expect(page.locator('main')).toBeVisible();
    expect(errors).toEqual([]);
  });
}
