import { expect, test } from '@playwright/test';

const routes = [
  ['/', '运行总览'],
  ['/jobs', '任务中心'],
  ['/traces', '链路追踪'],
  ['/timeline', '事件时间线'],
  ['/topology', '资源拓扑'],
  ['/resources', '算力资源'],
  ['/sandboxes', '运行沙箱'],
  ['/decisions', '调度决策'],
  ['/experiments', '实验对比'],
] as const;

test('Compose service hostname is accepted by the local Console server', async ({ request }) => {
  const response = await request.get('/', { headers: { Host: 'console:4173' } });
  expect(response.ok()).toBe(true);
});

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
    if (path === '/traces') {
      await expect(page.locator('.trace-waterfall')).toBeVisible();
      await expect(page.locator('.track-group')).toHaveCount(4);
      if (process.env.VITE_TGSRL_API_ADAPTER === 'mock') {
        await expect(page.getByRole('region', { name: '跨轨调用关联' })).toBeVisible();
      }
    }
    expect(errors).toEqual([]);
  });
}

for (const width of [1366, 1180, 1024, 820, 390]) {
  test(`调度决策在 ${width}px 宽度下保持在视口内`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto('/decisions');

    await expect(page.getByRole('heading', { name: '调度决策', level: 1 })).toBeVisible();
    await expect(page.locator('.trace-workbench')).toBeVisible();
    await expect(page.locator('.decision-list-panel .select-card').first()).toBeVisible();

    const layout = await page.evaluate(() => {
      const panels = [...document.querySelectorAll<HTMLElement>('.trace-workbench > .panel')].map((panel) => {
        const bounds = panel.getBoundingClientRect();
        return { left: bounds.left, right: bounds.right, width: bounds.width };
      });
      return {
        viewportWidth: window.innerWidth,
        documentWidth: document.documentElement.scrollWidth,
        panels,
      };
    });

    expect(layout.documentWidth).toBeLessThanOrEqual(layout.viewportWidth);
    expect(layout.panels).toHaveLength(2);
    for (const panel of layout.panels) {
      expect(panel.left).toBeGreaterThanOrEqual(0);
      expect(panel.right).toBeLessThanOrEqual(layout.viewportWidth);
    }
    if (width <= 1024) {
      expect(Math.abs(layout.panels[0].left - layout.panels[1].left)).toBeLessThanOrEqual(1);
    } else {
      expect(layout.panels[1].left).toBeGreaterThan(layout.panels[0].left);
    }
  });
}

for (const width of [1024, 390]) {
  test(`${width}px 宽度下所有工作台都不会撑开页面`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    for (const [path, heading] of routes) {
      await page.goto(path);
      await expect(page.getByRole('heading', { name: heading, level: 1 })).toBeVisible();
      const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      expect(documentWidth, `${path} 不应产生页面级横向滚动`).toBeLessThanOrEqual(width);
    }
  });
}
