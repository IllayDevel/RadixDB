import { test, expect, type Locator } from '@playwright/test';
import { readFileSync } from 'node:fs';

async function contrastRatio(locator: Locator) {
  const { color, backgroundColor } = await locator.evaluate((element) => {
    const style = getComputedStyle(element);
    return { color: style.color, backgroundColor: style.backgroundColor };
  });
  const luminance = (value: string) => {
    const channels = value.match(/[\d.]+/g)?.slice(0, 3).map(Number);
    if (!channels || channels.length !== 3) throw new Error(`Cannot parse CSS color: ${value}`);
    const [red, green, blue] = channels.map(channel => {
      const normalized = channel / 255;
      return normalized <= 0.04045 ? normalized / 12.92 : ((normalized + 0.055) / 1.055) ** 2.4;
    });
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
  };
  const foreground = luminance(color);
  const background = luminance(backgroundColor);
  return (Math.max(foreground, background) + 0.05) / (Math.min(foreground, background) + 0.05);
}

test('locales, glossary and layout', async ({ page }, info) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  for (const locale of ['ru', 'en']) {
    await page.goto(`/manual/1.2/${locale}/`);
    await expect(page.locator('h1')).toContainText(locale === 'ru' ? 'Руководство' : 'Manual');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-index.png`), fullPage: true });
    await page.locator('main a', { hasText: locale === 'ru' ? 'Глоссарий' : 'Glossary' }).last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/appendices/glossary/`));
    await expect(page.locator('main table')).toBeVisible();
    const widths = await page.evaluate(() => ({ total: document.documentElement.scrollWidth, viewport: window.innerWidth }));
    expect(widths.total).toBeLessThanOrEqual(widths.viewport + 1);
    await page.screenshot({ path: info.outputPath(`${locale}-glossary.png`), fullPage: true });
  }
  expect(errors).toEqual([]);
});

for (const locale of ['en', 'ru']) test(`search returns the ${locale} glossary`, async ({ page }) => {
  await page.goto(`/manual/1.2/${locale}/`);
  await page.locator('site-search button').first().click();
  const search = page.getByRole('dialog').getByRole('textbox');
  await expect(search).toBeVisible();
  await search.fill(locale === 'ru' ? 'Глоссарий' : 'Glossary');
  const glossary = page.locator('.pagefind-ui__result-link').filter({
    hasText: locale === 'ru' ? 'Глоссарий' : 'Glossary',
  }).first();
  await expect(glossary).toBeVisible();
  await glossary.click();
  await expect(page).toHaveURL(new RegExp(`/manual/1\\.2/${locale}/appendices/glossary/`));
});

test('language switch preserves the chapter', async ({ page }, info) => {
  await page.goto('/manual/1.2/en/appendices/glossary/');
  if (info.project.name === 'mobile') await page.getByRole('button', { name: 'Menu', exact: true }).click();
  await page.getByRole('combobox', { name: 'Select language' }).selectOption({ label: 'Русский' });
  await expect(page).toHaveURL(/\/ru\/appendices\/glossary\//);
  await expect(page.locator('h1')).toHaveText('Глоссарий');
});

test('build identity distinguishes target and application', async ({ page, request }) => {
  await page.goto('/manual/1.2/ru/');
  const identity = page.getByRole('complementary', { name: 'Версия документации' });
  await expect(identity).toContainText('1.2 development');
  const response = await request.get('/manual/1.2/build-manifest.json');
  expect(response.ok()).toBeTruthy();
  const manifest = await response.json();
  expect(manifest.target).toBe('1.2');
  expect(manifest.channel).toBe('development');
  expect(manifest.revision).toMatch(/^[a-f0-9]{40}$/);
  await expect(identity).toContainText(manifest.application);
  await expect(identity).toContainText(manifest.revision.slice(0, 12));
});

test('tutorial renders the tested SQL in both languages', async ({ page }, info) => {
  const setup = readFileSync(new URL('../examples/tutorial/setup.sql', import.meta.url), 'utf8').trim();
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/tutorial/first-database/`);
    await expect(page.locator('h1')).toHaveText(locale === 'ru' ? 'Первая база данных' : 'Your First Database');
    const block = page.locator('.expressive-code pre').filter({ hasText: 'CREATE TABLE departments' });
    await expect(block).toHaveText(setup, { useInnerText: true });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-tutorial.png`), fullPage: true });
  }
});

test('tutorial continues through server connection to client interfaces', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/tutorial/transactions/`);
    await page.locator('main a[href$="/server-connection/"]').first().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/tutorial/server-connection/`));
    await expect(page.locator('main')).toContainText('127.0.0.1:15443');
    await expect(page.locator('main')).toContainText('RADIXDB_PASSWORD');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-server.png`), fullPage: true });
    await page.locator('main a[href$="/clients/overview/"]').first().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/clients/overview/`));
    await expect(page.locator('main table')).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-clients.png`), fullPage: true });
  }
});

test('schema definition and index chapters render in both languages', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/sql/ddl/`);
    await expect(page.locator('main h1')).toHaveText(locale === 'ru' ? 'Определение схемы' : 'Defining a Schema');
    await expect(page.locator('main')).toContainText('AUTO_INCREMENT');
    const describeLine = page.locator('.expressive-code .ec-line').filter({ hasText: 'DESCRIBE assets;' });
    await expect(describeLine.locator('span').first()).toHaveText('DESCRIBE');
    await page.getByRole('main').getByRole('link', {
      name: locale === 'ru' ? 'об индексах' : 'indexes', exact: true,
    }).click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/sql/indexes/`));
    await expect(page.locator('main h1')).toHaveText(locale === 'ru' ? 'Индексы' : 'Indexes');
    await expect(page.locator('main')).toContainText('contacts_email_active_uidx');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-indexes.png`), fullPage: true });
  }
});

test('sidebar selection and hover remain readable in both themes', async ({ page }, info) => {
  await page.goto('/manual/1.2/ru/sql/ddl/');
  if (info.project.name === 'mobile') await page.getByRole('button', { name: 'Меню', exact: true }).click();
  const current = page.locator('#starlight__sidebar a[aria-current="page"]');
  const other = page.locator('#starlight__sidebar a:not([aria-current])').filter({ hasText: 'Изменение данных' }).first();

  for (const theme of ['dark', 'light']) {
    await page.evaluate(selectedTheme => document.documentElement.dataset.theme = selectedTheme, theme);
    await expect(current).toBeVisible();
    expect(await contrastRatio(current)).toBeGreaterThanOrEqual(4.5);
    await other.hover();
    expect(await contrastRatio(other)).toBeGreaterThanOrEqual(4.5);
    await page.screenshot({ path: info.outputPath(`sidebar-${theme}.png`) });
  }
});

test('query chapter renders checked joins, windows and limits', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/sql/queries/`);
    await expect(page.locator('main h1')).toHaveText(locale === 'ru' ? 'Запросы к данным' : 'Querying Data');
    await expect(page.locator('main')).toContainText('FULL JOIN');
    await expect(page.locator('main')).toContainText('ROW_NUMBER');
    await expect(page.locator('main')).toContainText('QUALIFY');
    await expect(page.locator('main h2')).toHaveCount(7);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-queries.png`), fullPage: true });
  }
});

test('transaction chapter renders actual isolation and client limits', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/sql/transactions/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Транзакции и конкурентный доступ' : 'Transactions and Concurrency',
    );
    await expect(page.locator('main')).toContainText('READ COMMITTED');
    await expect(page.locator('main')).toContainText('SNAPSHOT');
    await expect(page.locator('main')).toContainText('SAVEPOINT');
    await expect(page.locator('main')).toContainText('ROLLBACK TO');
    await expect(page.locator('main')).toContainText('SHOW ISOLATION_LEVEL');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-transactions.png`), fullPage: true });
  }
});

test('navigable references render paths, explain and write boundary', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/sql/navigable-references/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Навигация по ссылкам' : 'Navigable References',
    );
    await expect(page.locator('main')).toContainText('e.department_id.profile_id.display_name');
    await expect(page.locator('main')).toContainText('Reference Navigation');
    await expect(page.locator('main')).toContainText('NAVIGATION_AMBIGUOUS_ROOT');
    await expect(page.locator('main')).toContainText('NAVIGATION_READ_ONLY');
    await expect(page.locator('main')).toContainText('512');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-navigation.png`), fullPage: true });
  }
});

test('SQL matrix exposes versioned support, limits and rejection evidence', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/appendices/compatibility/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Матрица покрытия SQL' : 'SQL Coverage Matrix',
    );
    await expect(page.locator('main table')).toHaveCount(7);
    await expect(page.locator('main tbody tr')).toHaveCount(83);
    await expect(page.locator('main')).toContainText('QUERY-12');
    await expect(page.locator('main')).toContainText('EXT-05');
    await expect(page.locator('main')).toContainText('test-transactions.mjs');
    await expect(page.locator('main')).toContainText(locale === 'ru' ? 'Отклоняется' : 'Rejected');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-sql-matrix.png`), fullPage: true });
  }
});

test('limits distinguish hard ceilings, configuration and measured scale', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/appendices/limits/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Ограничения' : 'Limits',
    );
    await expect(page.locator('main table')).toHaveCount(5);
    await expect(page.locator('main tbody tr')).toHaveCount(70);
    await expect(page.locator('main')).toContainText('HARD-45');
    await expect(page.locator('main')).toContainText('PLUG-09');
    await expect(page.locator('main')).toContainText('CFG-13');
    await expect(page.locator('main')).toContainText('SCALE-03');
    await expect(page.locator('main')).toContainText('23bf35df');
    await expect(page.locator('main')).toContainText(locale === 'ru' ? '100 000 000' : '100,000,000');
    await expect(page.locator('main')).toContainText(
      locale === 'ru' ? 'не является SLA' : 'not an SLA',
    );
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-limits.png`), fullPage: true });
  }
});

test('benchmark appendix keeps NVMe performance and HDD reliability separate', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/appendices/benchmarks/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Измерения' : 'Benchmarks',
    );
    await expect(page.locator('main table')).toHaveCount(4);
    await expect(page.locator('main tbody tr')).toHaveCount(27);
    await expect(page.locator('main')).toContainText('BENCH-03');
    await expect(page.locator('main')).toContainText('SOAK-07');
    await expect(page.locator('main')).toContainText('PostgreSQL 18.3');
    await expect(page.locator('main')).toContainText('12ef5963');
    await expect(page.locator('main')).toContainText(
      locale === 'ru' ? '11 870,421' : '11,870.421',
    );
    await expect(page.locator('main')).toContainText(
      locale === 'ru' ? 'не являются SLA' : 'not a product SLA',
    );
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-benchmarks.png`), fullPage: true });
  }
});

test('release notes separate current 1.2 behavior from historical releases', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/appendices/release-notes/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'История выпусков' : 'Release Notes',
    );
    await expect(page.locator('main table')).toHaveCount(0);
    await expect(page.locator('main')).toContainText('1.2');
    await expect(page.locator('main')).toContainText('1.1.0');
    await expect(page.locator('main')).toContainText('1.0.0');
    await expect(page.locator('main')).toContainText('protocol 17');
    await expect(page.locator('main')).toContainText('Argon2id');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-release-notes.png`), fullPage: true });
  }
});

test('installation and server chapters preserve lifecycle and readiness boundaries', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/installation/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Установка сервера' : 'Installing the Server',
    );
    await expect(page.locator('main')).toContainText('x86_64-unknown-linux-gnu');
    await expect(page.locator('main')).toContainText('SHA256SUMS');
    await expect(page.locator('main')).toContainText('0640');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-installation.png`), fullPage: true });
    await page.locator('main a[href$="/administration/server/"]').first().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/administration/server/`));
    await expect(page.locator('main')).toContainText('127.0.0.1:15441');
    await expect(page.locator('main')).toContainText('database_status');
    await expect(page.locator('main')).toContainText('SIGTERM');
    await expect(page.locator('main')).toContainText('stopped cleanly');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-server.png`), fullPage: true });
  }
});

test('configuration separates code defaults, release values and restart application', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/configuration/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Конфигурация сервера' : 'Server Configuration',
    );
    await expect(page.locator('main table')).toHaveCount(1);
    await expect(page.locator('main tbody tr')).toHaveCount(27);
    const connections = page.locator('main tbody tr').filter({ hasText: 'max_connections' });
    await expect(connections).toContainText('151');
    await expect(connections).toContainText('64');
    const rootVerifier = page.locator('main tbody tr').filter({
      hasText: 'authentication.root_password_verifier',
    });
    await expect(rootVerifier).toContainText('Argon2id PHC string');
    await expect(page.locator('main')).toContainText('plugins.package_directories');
    await expect(page.locator('main')).toContainText('max_inflight_frame_bytes');
    await expect(page.locator('main')).toContainText('256..=max_inflight_frame_bytes');
    await expect(page.locator('main')).toContainText('RADIXDB_RELEASE_');
    await expect(page.locator('main')).toContainText('--print-endpoint');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-configuration.png`), fullPage: true });
  }
});

test('storage chapter explains the hybrid generation and measured footprint', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/storage/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Архитектура хранения' : 'Storage Architecture',
    );
    await expect(page.locator('main table')).toHaveCount(1);
    await expect(page.locator('main')).toContainText(locale === 'ru' ? '65 536' : '65,536');
    await expect(page.locator('main')).toContainText('1964220021');
    await expect(page.locator('main')).toContainText('volume_compression');
    await expect(page.locator('main')).toContainText('PRAGMA VOLUME_STATS');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-storage.png`), fullPage: true });
  }
});

test('memory chapter separates budgets, RSS and the operating-system cache', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/memory/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Управление памятью' : 'Memory Management',
    );
    await expect(page.locator('main table')).toHaveCount(2);
    await expect(page.locator('main tbody tr')).toHaveCount(7);
    await expect(page.locator('main')).toContainText('scan_prefetch_cache_bytes');
    await expect(page.locator('main')).toContainText('1060020224');
    await expect(page.locator('main')).toContainText('page_cache_memory_reserve');
    await expect(page.locator('main')).toContainText('volume_cache_bytes');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-memory.png`), fullPage: true });
  }
});

test('backup chapter keeps snapshots, external artifacts and migration distinct', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/backup-restore/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Резервное копирование и восстановление' : 'Backup and Restore',
    );
    await expect(page.locator('main table')).toHaveCount(2);
    await expect(page.locator('main tbody tr')).toHaveCount(9);
    await expect(page.locator('main')).toContainText('SNAPSHOT.mft');
    await expect(page.locator('main')).toContainText('SHA256SUMS');
    await expect(page.locator('main')).toContainText('BACKUP.env');
    await expect(page.locator('main')).toContainText('0123456789abcdef0123456789abcdef');
    await expect(page.locator('main')).toContainText('--export-sql');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-backup-restore.png`), fullPage: true });
  }
});

test('operations chapters separate upgrade, telemetry and media recovery', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/upgrading/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Обновление RadixDB' : 'Upgrading RadixDB',
    );
    await expect(page.locator('main')).toContainText('--export-sql');
    await expect(page.locator('main')).toContainText('--import-sql');
    await expect(page.locator('main')).toContainText('RENAME_NOREPLACE');
    await page.locator('main a[href$="/administration/monitoring/"]').first().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/administration/monitoring/`));
    await expect(page.locator('main')).toContainText('PRAGMA RUNTIME_STATS');
    await expect(page.locator('main')).toContainText('complete = false');
    await expect(page.locator('main')).toContainText('disk_reserve_exhausted');
    await page.locator('main a[href$="/administration/troubleshooting/"]').first().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/administration/troubleshooting/`));
    await expect(page.locator('main')).toContainText('ENOSPC');
    await expect(page.locator('main')).toContainText('sync_mode=full');
    await expect(page.locator('main')).toContainText(locale === 'ru' ? 'Crash и потеря носителя' : 'Crash versus media loss');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-administration-operations.png`), fullPage: true });
  }
});

test('Rust client chapters preserve ownership and reliability boundaries', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/clients/embedded-rust/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Встраиваемый Rust' : 'Embedded Rust',
    );
    await expect(page.locator('main')).toContainText('Rows::error()');
    await expect(page.locator('main')).toContainText('Database::close()');
    await page.locator('main a[href$="/clients/rust-client/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/clients/rust-client/`));
    await expect(page.locator('main')).toContainText('CommandsOutOfSync');
    await expect(page.locator('main')).toContainText('is_reusable()');
    await expect(page.locator('main')).toContainText('outcome is unknown');
    await page.locator('main a[href$="/clients/orm/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/clients/orm/`));
    await expect(page.locator('main')).toContainText('DynamicRecord');
    await expect(page.locator('main')).toContainText('SchemaChanged');
    await expect(page.locator('main')).toContainText('connection.rollback()');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-clients-rust-orm.png`), fullPage: true });
  }
});

test('rendered manual links and anchors resolve', async ({ page, request }) => {
  test.setTimeout(60_000);
  const manifest = await (await request.get('/manual/1.2/build-manifest.json')).json();
  for (const locale of manifest.locales) for (const chapter of manifest.chapters) {
    await page.goto(`/manual/1.2/${locale}/${chapter ? `${chapter}/` : ''}`);
    const links = await page.locator('a[href]').evaluateAll(anchors => [...new Set(anchors.map(a => (a as HTMLAnchorElement).href))]);
    for (const href of links) {
      const url = new URL(href);
      if (url.origin !== new URL(page.url()).origin) continue;
      const response = await request.get(url.origin + url.pathname + url.search);
      expect(response.ok(), `${page.url()} -> ${href}`).toBeTruthy();
      if (url.hash) {
        const html = await response.text();
        const found = await page.evaluate(({ html, id }) => Boolean(new DOMParser().parseFromString(html, 'text/html').getElementById(id)), { html, id: decodeURIComponent(url.hash.slice(1)) });
        expect(found, href).toBeTruthy();
      }
    }
  }
});

test('version selector lists only the current built version', async ({ page }) => {
  await page.goto('/manual/1.2/ru/');
  const select = page.getByRole('combobox', { name: 'Версия руководства' });
  await expect(select.locator('option')).toHaveCount(1);
  await expect(select).toBeDisabled();
});

test('version selector preserves chapters and labels fallback in a fixture registry', async ({ page, request }) => {
  const current = await (await request.get('/manual/1.2/build-manifest.json')).json();
  const previous = { ...current, target: '1.0', channel: 'release', base: '/manual/1.0/', chapters: ['', 'appendices/glossary'] };
  await page.route('**/manual/1.2/versions.json', route => route.fulfill({ json: [{ ...current, base: '/manual/1.2/' }, previous] }));
  await page.route('**/manual/1.0/**', route => route.fulfill({ contentType: 'text/html', body: '<h1>Archive test fixture</h1>' }));
  for (const chapter of ['appendices/glossary', 'tutorial/first-database']) {
    await page.goto(`/manual/1.2/ru/${chapter}/`);
    const select = page.getByRole('combobox', { name: 'Версия руководства' });
    await expect(select).toBeEnabled();
    const fallback = chapter === 'tutorial/first-database';
    await expect(select.locator('option').last()).toHaveText(`1.0 release${fallback ? ' (оглавление)' : ''}`);
    await select.selectOption({ index: 1 });
    await expect(page).toHaveURL(`http://127.0.0.1:4326/manual/1.0/ru/${fallback ? '' : `${chapter}/`}`);
  }
});

test('invalid archive links never become selectable', async ({ page }) => {
  await page.route('**/versions.json', route => route.fulfill({ json: [{ target: '1.0', channel: 'release', base: '//evil.example/', locales: ['en'], chapters: [''] }] }));
  await page.goto('/manual/1.2/en/');
  await expect(page.getByRole('combobox', { name: 'Manual version' })).toBeDisabled();
});

test('separate version artifacts keep separate search indexes', async ({ page }) => {
  for (const version of ['1.2', '1.0']) {
    await page.goto(`/manual/${version}/en/`);
    const counts = await page.evaluate(async version => {
      const engine = await import(`/manual/${version}/pagefind/pagefind.js`);
      const archive = await engine.search('"archivalonlysentinel"');
      const current = await engine.search('"MVCC"');
      return { archive: archive.results.length, current: current.results.length };
    }, version);
    if (version === '1.0') {
      expect(counts.archive).toBe(1);
      expect(counts.current).toBe(0);
    } else {
      expect(counts.archive).toBe(0);
      expect(counts.current).toBeGreaterThan(0);
    }
  }
});

test('server programming follows the accepted PL, routine, trigger and Job chain', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/programming/pl-sql/`);
    await expect(page.locator('main h1')).toHaveText('RadixDB PL');
    await expect(page.locator('.starlight-aside--caution')).toHaveCount(0);
    await expect(page.locator('main')).toContainText('SQL_IDENTIFIER');
    await expect(page.locator('main')).toContainText('PL/pgSQL');
    await page.locator('main a[href$="/programming/routines/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/programming/routines/`));
    await expect(page.locator('main')).toContainText('10,000,000');
    await expect(page.locator('main')).toContainText('DROP FUNCTION');
    await page.locator('main a[href$="/programming/triggers/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/programming/triggers/`));
    await expect(page.locator('main')).toContainText('PL_TRIGGER_CYCLE');
    await expect(page.locator('main')).toContainText('DROP TRIGGER');
    await page.locator('main a[href$="/programming/jobs/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/programming/jobs/`));
    await expect(page.locator('main')).toContainText('at-least-once');
    await expect(page.locator('main')).toContainText('PL_JOB_ATTEMPT_FAILED');
    await expect(page.locator('main')).toContainText('scheduler_retryable');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-server-programming.png`), fullPage: true });
  }
});

test('authentication, ACL and routine security expose the verified 1.2 boundary', async ({ page }, info) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/administration/authentication/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Аутентификация' : 'Authentication',
    );
    await expect(page.locator('main')).toContainText('authenticate_database');
    await expect(page.locator('main')).toContainText('TlsClientConfig');
    await expect(page.locator('main')).toContainText('AuthenticationFailed');
    await expect(page.locator('main')).toContainText('radixdb-password');
    await expect(page.locator('main')).toContainText('root_password_verifier');
    await page.locator('main a[href$="/administration/access-control/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/administration/access-control/`));
    await expect(page.locator('main')).toContainText('WITH ADMIN OPTION');
    await expect(page.locator('main')).toContainText('REVOKE GRANT OPTION FOR');
    await expect(page.locator('main')).toContainText('CREATE POLICY');
    await page.locator('main a[href$="/programming/routine-security/"]').last().click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/programming/routine-security/`));
    await expect(page.locator('main')).toContainText('SECURITY DEFINER');
    await expect(page.locator('main')).toContainText('EXECUTE');
    await expect(page.locator('main')).toContainText('CURRENT_EFFECTIVE_PRINCIPAL');
    await expect(page.locator('main')).toContainText('CURRENT_JOB_ID');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-security.png`), fullPage: true });
  }
  expect(errors).toEqual([]);
});

test('program and configuration references cover the frozen command surfaces', async ({ page }, info) => {
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/reference/programs/cli/`);
    await expect(page.locator('main h1')).toHaveText('radixdb-cli');
    await expect(page.locator('main')).toContainText('--reset-storage');
    await expect(page.locator('main')).toContainText('sync_mode');
    await expect(page.locator('main')).toContainText(locale === 'ru' ? '100 000' : '100,000');
    await page.getByRole('main').getByRole('link', {
      name: locale === 'ru' ? 'Указатель конфигурации' : 'configuration index',
    }).click();
    await expect(page).toHaveURL(new RegExp(`/${locale}/reference/configuration/`));
    await expect(page.locator('main')).toContainText(
      locale === 'ru' ? 'принимается 39 ключей' : '39 accepted keys',
    );
    await expect(page.locator('main')).toContainText('max_compaction_output_bytes');
    await expect(page.locator('main')).toContainText('commit_batch_size');
    await page.goto(`/manual/1.2/${locale}/reference/programs/server/`);
    await expect(page.locator('main h1')).toHaveText('radixdb-server');
    await expect(page.locator('main')).toContainText('--help');
    await expect(page.locator('main')).toContainText('--print-endpoint');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-program-reference.png`), fullPage: true });
  }
});

test('root redirect and SQL command reference remain navigable', async ({ page }, info) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto('/manual/1.2/');
  await expect(page).toHaveURL(/\/manual\/1\.2\/en\/$/);
  const missing = await page.goto('/manual/1.2/en/not-a-real-page/');
  expect(missing?.status()).toBe(404);
  await expect(page.locator('main h1')).toContainText('Page not found');
  await expect(page.getByRole('main').getByRole('link', { name: 'Russian contents', exact: true }))
    .toBeVisible();
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/reference/sql/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Справочник команд SQL' : 'SQL Command Reference',
    );
    await page.locator('main a[href$="/reference/sql/select/"]').first().click();
    await expect(page.locator('main h1')).toHaveText('SELECT');
    await expect(page.locator('main')).toContainText('QUALIFY');
    await expect(page.locator('main')).toContainText('LATERAL');
    await expect(page.locator('.expressive-code pre[data-language="radixdb-sql"]'))
      .toContainText('CREATE TABLE ref_select');
    await page.goto(`/manual/1.2/${locale}/reference/sql/savepoint/`);
    await expect(page.locator('main h1')).toHaveText('SAVEPOINT');
    await expect(page.locator('main')).toContainText('CLI');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-sql-reference.png`), fullPage: true });
  }
  expect(errors).toEqual([]);
});

test('internals preserve request, storage and protocol ownership boundaries', async ({ page }, info) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  for (const locale of ['en', 'ru']) {
    await page.goto(`/manual/1.2/${locale}/internals/overview/`);
    await expect(page.locator('main h1')).toHaveText(
      locale === 'ru' ? 'Обзор архитектуры' : 'Architecture overview',
    );
    await expect(page.locator('main')).toContainText('DatabaseOwner');
    await expect(page.locator('main')).toContainText('radixdb-storage');
    await page.goto(`/manual/1.2/${locale}/internals/storage/`);
    await expect(page.locator('main')).toContainText('CONTROL.0');
    await expect(page.locator('main')).toContainText('immutable');
    await expect(page.locator('main')).toContainText('split-brain');
    await page.goto(`/manual/1.2/${locale}/internals/protocol/`);
    await expect(page.locator('main')).toContainText('protocol 17');
    await expect(page.locator('main')).toContainText('64 MiB');
    await expect(page.locator('main')).toContainText('CompactionBackpressure');
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBeTruthy();
    await page.screenshot({ path: info.outputPath(`${locale}-internals.png`), fullPage: true });
  }
  expect(errors).toEqual([]);
});
