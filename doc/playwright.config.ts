import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: false,
  use: { baseURL: 'http://127.0.0.1:4326', screenshot: 'only-on-failure' },
  webServer: {
    command: 'node scripts/search-fixture.mjs && node scripts/serve-build.mjs',
    url: 'http://127.0.0.1:4326/manual/1.2/ru/',
    reuseExistingServer: false,
  },
  projects: [
    { name: 'desktop', use: { viewport: { width: 1440, height: 1000 } } },
    { name: 'mobile', use: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true } },
  ],
});
