import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parse } from 'smol-toml';
import { packageSite, routeOutput } from './package-site.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const fixture = mkdtempSync(path.join(root, '.publication-test.'));
const artifact = path.join(fixture, 'artifact');
const dist = path.join(fixture, 'dist');
const env = {
  ...process.env,
  DOCS_SITE: 'https://docs.invalid',
  DOCS_ROOT_BASE: '/preview/',
  DOCS_BASE: '/preview/manual/1.2/',
  DOCS_OUT_DIR: dist,
};

function filesBelow(directory, prefix = '') {
  return readdirSync(directory).flatMap(name => {
    const absolute = path.join(directory, name);
    const relative = path.posix.join(prefix, name);
    return statSync(absolute).isDirectory() ? filesBelow(absolute, relative) : [relative];
  });
}

try {
  const build = spawnSync('npm', ['run', 'build'], {
    cwd: root,
    env,
    encoding: 'utf8',
    timeout: 120000,
    maxBuffer: 32 * 1024 * 1024,
  });
  assert(!build.error, String(build.error));
  assert.equal(build.status, 0, `${build.stdout}\n${build.stderr}`);

  const packaged = packageSite({ env, artifactDir: artifact, distDir: dist });
  const publication = parse(readFileSync(path.join(root, '_meta/publication.toml'), 'utf8'));
  assert.deepEqual(packaged.manifest, {
    version: '1.2',
    channel: publication.channel,
    site: 'https://docs.invalid',
    root_base: '/preview/',
    docs_base: '/preview/manual/1.2/',
    redirect_sources: 51,
    redirect_routes: 102,
    redirect_files: 97,
    evidence_files: 21,
  });

  const canonicalRoot = path.join(artifact, 'manual/1.2');
  for (const file of [
    'en/index.html',
    'ru/index.html',
    'pagefind/pagefind.js',
    'build-manifest.json',
    'versions.json',
    'sitemap-index.xml',
    'evidence/README.md',
    'evidence/README.ru.md',
    'evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md',
    'evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.ru.md',
    'evidence/reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log',
  ]) {
    assert(readFileSync(path.join(canonicalRoot, file)).length > 0, `Missing publication file: ${file}`);
  }
  const sitemap = readFileSync(path.join(canonicalRoot, 'sitemap-0.xml'), 'utf8');
  assert(sitemap.includes('https://docs.invalid/preview/manual/1.2/en/'));
  assert(sitemap.includes('https://docs.invalid/preview/manual/1.2/ru/'));

  const registry = parse(readFileSync(path.join(root, '_meta/legacy-redirects.toml'), 'utf8'));
  for (const redirect of registry.redirects) for (const route of redirect.routes) {
    const html = readFileSync(path.join(artifact, routeOutput(route)), 'utf8');
    const target = `/preview/manual/1.2/${redirect.target}`;
    assert(html.includes(target), `${route} does not redirect to ${target}`);
    assert(html.includes('location.search + location.hash'), `${route} does not preserve URL suffixes`);
  }

  const page = readFileSync(path.join(canonicalRoot, 'en/index.html'), 'utf8');
  const asset = page.match(/(?:href|src)="(\/preview\/manual\/1\.2\/_astro\/[^"]+)"/)?.[1];
  assert(asset, 'Canonical page does not use the publication prefix for assets');
  assert(readFileSync(path.join(artifact, asset.slice('/preview/'.length))).length > 0,
    `Missing prefixed asset: ${asset}`);

  const documentationPages = filesBelow(canonicalRoot).filter(file =>
    file.endsWith('.html') && (file === '404.html' || file.startsWith('en/') || file.startsWith('ru/')));
  assert.equal(documentationPages.length, 145);
  for (const file of documentationPages) {
    const html = readFileSync(path.join(canonicalRoot, file), 'utf8');
    assert.equal(html.split('mc.yandex.ru/metrika/tag.js?id=112445210').length - 1, 1,
      `${file} must initialize Yandex Metrika exactly once`);
    assert.equal(html.split('mc.yandex.ru/watch/112445210').length - 1, 1,
      `${file} must contain one no-script Yandex Metrika pixel`);
  }

  console.log(JSON.stringify({ ...packaged.manifest, sitemap: true, assets: true,
    fragments: true, metrika_pages: documentationPages.length, passed: true }, null, 2));
} finally {
  rmSync(fixture, { recursive: true, force: true });
}
