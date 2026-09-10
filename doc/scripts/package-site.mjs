import assert from 'node:assert/strict';
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'smol-toml';

const root = fileURLToPath(new URL('../', import.meta.url));
const repo = path.resolve(root, '..');

function normalizeBase(value, name) {
  assert(typeof value === 'string' && value.startsWith('/'), `${name} must start with /`);
  assert(!value.includes('\\') && !value.includes('?') && !value.includes('#'), `${name} is not a URL path`);
  const segments = value.split('/').filter(Boolean);
  assert(!segments.includes('.') && !segments.includes('..'), `${name} contains traversal`);
  return `/${segments.join('/')}${segments.length ? '/' : ''}`;
}

function validateSite(value) {
  assert(typeof value === 'string' && value.length > 0, 'DOCS_SITE is required');
  const url = new URL(value);
  assert(['http:', 'https:'].includes(url.protocol), 'DOCS_SITE must use HTTP(S)');
  assert(!url.username && !url.password && !url.search && !url.hash, 'DOCS_SITE must not contain credentials, query or fragment');
  assert(url.pathname === '/' || url.pathname === '', 'DOCS_SITE must be an origin without a path');
  return url.origin;
}

function routeOutput(route) {
  assert(typeof route === 'string' && !route.startsWith('/') && !route.includes('\\'), `Invalid legacy route: ${route}`);
  const segments = route.split('/').filter(Boolean);
  assert(!segments.includes('.') && !segments.includes('..'), `Legacy route contains traversal: ${route}`);
  return route === '' || route.endsWith('/') ? path.posix.join(...segments, 'index.html') : segments.join('/');
}

function contentPath(target) {
  const [locale, ...parts] = target.split('/').filter(Boolean);
  assert(['en', 'ru'].includes(locale), `Invalid target locale: ${target}`);
  const chapter = parts.join('/') || 'index';
  return ['md', 'mdx'].map(extension => path.join(root, 'src/content/docs', locale, `${chapter}.${extension}`));
}

function htmlEscape(value) {
  return value.replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

function redirectHtml(site, target) {
  const absolute = new URL(target, site).href;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="robots" content="noindex">
  <meta http-equiv="refresh" content="0; url=${htmlEscape(target)}">
  <link rel="canonical" href="${htmlEscape(absolute)}">
  <title>RadixDB documentation moved</title>
</head>
<body>
  <p><a href="${htmlEscape(target)}">Continue to the RadixDB 1.2 manual</a></p>
  <script>location.replace(${JSON.stringify(target)} + location.search + location.hash);</script>
</body>
</html>
`;
}

export function publicationSettings(env = process.env) {
  const publication = parse(readFileSync(path.join(root, '_meta/publication.toml'), 'utf8'));
  const site = validateSite(env.DOCS_SITE || '');
  const rootBase = normalizeBase(env.DOCS_ROOT_BASE || '/', 'DOCS_ROOT_BASE');
  const docsBase = normalizeBase(env.DOCS_BASE || '', 'DOCS_BASE');
  assert(docsBase === `${rootBase}manual/${publication.version}/`,
    `DOCS_BASE must be ${rootBase}manual/${publication.version}/`);
  return { publication, site, rootBase, docsBase };
}

export function validateRedirects(registry) {
  assert.equal(registry.version, '1.2', 'Legacy redirect version mismatch');

  const routes = new Map();
  const sources = new Set();
  for (const redirect of registry.redirects) {
    assert(typeof redirect.source === 'string' && redirect.source.startsWith('docs/'),
      `Invalid legacy source: ${redirect.source}`);
    assert(!redirect.source.includes('..') && !redirect.source.includes('\\'),
      `Unsafe legacy source: ${redirect.source}`);
    assert(!sources.has(redirect.source), `Duplicate legacy source: ${redirect.source}`);
    sources.add(redirect.source);
    assert(redirect.routes.length > 0, `No routes for ${redirect.source}`);
    assert(contentPath(redirect.target).some(existsSync), `Missing target chapter: ${redirect.target}`);
    for (const route of redirect.routes) {
      const output = routeOutput(route);
      const previous = routes.get(output);
      assert(!previous || previous.target === redirect.target,
        `Conflicting routes ${previous?.source} and ${redirect.source} write ${output}`);
      routes.set(output, { source: redirect.source, target: redirect.target, route });
    }
  }
  return routes;
}

function filesBelow(directory, prefix = '') {
  return readdirSync(directory).flatMap(name => {
    const absolute = path.join(directory, name);
    const relative = path.posix.join(prefix, name);
    return statSync(absolute).isDirectory() ? filesBelow(absolute, relative) : [relative];
  });
}

export function packageSite({ env = process.env, artifactDir, distDir } = {}) {
  const settings = publicationSettings(env);
  const registry = parse(readFileSync(path.join(root, '_meta/legacy-redirects.toml'), 'utf8'));
  const routes = validateRedirects(registry);
  const evidence = filesBelow(path.join(root, 'public/evidence'));
  const artifact = path.resolve(artifactDir || env.DOCS_ARTIFACT_DIR || path.join(root, '.site-artifact'));
  const dist = path.resolve(distDir || path.join(root, 'dist'));
  assert(existsSync(path.join(dist, 'build-manifest.json')), 'Run the Astro build before packaging');
  assert(artifact !== repo && artifact !== root && artifact !== dist, 'Unsafe artifact directory');

  rmSync(artifact, { recursive: true, force: true });
  const docsMountPath = settings.docsBase.slice(settings.rootBase.length);
  const docsOutput = path.join(artifact, docsMountPath);
  mkdirSync(path.dirname(docsOutput), { recursive: true });
  cpSync(dist, docsOutput, { recursive: true });

  const rootOutput = artifact;
  for (const [output, redirect] of routes) {
    const file = path.join(rootOutput, output);
    mkdirSync(path.dirname(file), { recursive: true });
    writeFileSync(file, redirectHtml(settings.site, `${settings.docsBase}${redirect.target}`));
  }
  cpSync(path.join(dist, '404.html'), path.join(rootOutput, '404.html'));

  const manifest = {
    version: settings.publication.version,
    channel: settings.publication.channel,
    site: settings.site,
    root_base: settings.rootBase,
    docs_base: settings.docsBase,
    redirect_sources: registry.redirects.length,
    redirect_routes: registry.redirects.flatMap(redirect => redirect.routes).length,
    redirect_files: routes.size,
    evidence_files: evidence.length,
  };
  writeFileSync(path.join(rootOutput, 'publication-manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);
  return { artifact, settings, manifest, routes };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  console.log(JSON.stringify(packageSite().manifest, null, 2));
}

export { normalizeBase, redirectHtml, routeOutput, validateSite };
