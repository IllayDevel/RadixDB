import assert from 'node:assert/strict';
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { parse } from 'smol-toml';

const root = process.env.RADIXDB_DOCS_ROOT
  ? path.resolve(process.env.RADIXDB_DOCS_ROOT)
  : fileURLToPath(new URL('../', import.meta.url));
const repo = process.env.RADIXDB_DOCS_REPO
  ? path.resolve(process.env.RADIXDB_DOCS_REPO)
  : path.resolve(root, '..');
const registry = parse(readFileSync(path.join(root, '_meta/chapters.toml'), 'utf8'));
const publication = parse(readFileSync(path.join(root, '_meta/publication.toml'), 'utf8'));
const manifest = parse(readFileSync(path.join(repo, 'Cargo.toml'), 'utf8'));
assert.equal(registry.target_version, publication.version);
if (publication.channel === 'release') {
  // Appending only a zero patch is allowed: a 1.2 documentation target matches 1.2.0.
  const normalize = v => /^\d+\.\d+$/.test(v) ? `${v}.0` : v;
  assert.equal(normalize(publication.version), normalize(manifest.workspace.package.version));
}
const ids = new Set();
const declared = new Set();
const orders = new Set();
for (const chapter of registry.chapters) {
  assert(!ids.has(chapter.id), `Duplicate ID: ${chapter.id}`);
  assert(!declared.has(chapter.path), `Duplicate path: ${chapter.path}`);
  assert(!orders.has(chapter.order), `Duplicate order: ${chapter.order}`);
  assert(Number.isFinite(chapter.order) && chapter.order >= 0,
    `Invalid order for ${chapter.id}: ${chapter.order}`);
  assert(/^(?:[a-z0-9][a-z0-9_-]*\/)*[a-z0-9][a-z0-9_-]*\.mdx?$/.test(chapter.path),
    `Unsafe or mixed-case path: ${chapter.path}`);
  ids.add(chapter.id);
  declared.add(chapter.path);
  orders.add(chapter.order);
  assert(['planned', 'draft', 'verified', 'published'].includes(chapter.status));
  assert(['planned', 'implemented', 'accepted'].includes(chapter.feature_status));
  assert(Array.isArray(chapter.sources) && chapter.sources.length > 0,
    `Missing sources: ${chapter.id}`);
  assert(Array.isArray(chapter.tests) && chapter.tests.length > 0,
    `Missing tests: ${chapter.id}`);
  assert(typeof chapter.reviewer === 'string' && chapter.reviewer.length > 0,
    `Missing reviewer: ${chapter.id}`);
  assert(Array.isArray(chapter.legacy_paths), `Missing legacy_paths: ${chapter.id}`);
  if (chapter.feature_status !== 'planned') {
    assert(/^[a-f0-9]{40}$/.test(chapter.verified_commit),
      `Invalid verified commit: ${chapter.id}`);
  }
  for (const source of chapter.sources) assert(existsSync(path.join(repo, source)), `Missing source: ${source}`);
  if (chapter.status === 'planned') continue;
  const bodies = registry.locales.map(locale => {
    const filename = path.join(root, 'src/content/docs', locale, chapter.path);
    assert(existsSync(filename), `Missing locale: ${filename}`);
    const body = readFileSync(filename, 'utf8');
    assert(body.startsWith('---\n'), `Missing frontmatter: ${filename}`);
    return body;
  });
  const code = body => [...body.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(m => m[1]);
  assert.deepEqual(code(bodies[0]), code(bodies[1]), `Code mismatch: ${chapter.path}`);
}
function walk(dir, prefix = '') {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => entry.isDirectory()
    ? walk(path.join(dir, entry.name), `${prefix}${entry.name}/`)
    : /\.mdx?$/.test(entry.name) ? [`${prefix}${entry.name}`] : []);
}
for (const locale of registry.locales) {
  for (const p of walk(path.join(root, 'src/content/docs', locale))) assert(declared.has(p), `Unregistered: ${locale}/${p}`);
}
console.log(`PASS: ${ids.size} chapter IDs, EN/RU pairs and code parity; target ${publication.version} ${publication.channel}, application ${manifest.workspace.package.version}`);
