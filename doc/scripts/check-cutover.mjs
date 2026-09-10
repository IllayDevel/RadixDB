import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'smol-toml';

const root = fileURLToPath(new URL('../', import.meta.url));
const repo = path.resolve(root, '..');
function markdownBelow(input) {
  if (!existsSync(input)) return [];
  if (!statSync(input).isDirectory()) return [input];
  return readdirSync(input).flatMap(name => markdownBelow(path.join(input, name)));
}

const roots = [
  'README.md', 'README.ru.md', 'SECURITY.md', 'SECURITY.ru.md',
  'CONTRIBUTING.md', 'CONTRIBUTING.ru.md', 'CODE_OF_CONDUCT.md',
  'CODE_OF_CONDUCT.ru.md', 'doc/README.md', 'doc/README.ru.md',
  'doc/src/content/docs', 'release', 'examples/public',
  'doc/public/evidence',
  'crates/radixdb-client/README.md',
];
const files = roots.flatMap(relative => markdownBelow(path.join(repo, relative)))
  .filter(file => /\.mdx?$/.test(file));
const oldPublicLink = /\]\((?:\.\.\/)*docs\/(?:en|ru|architecture|benchmarks|internal)(?:\/|\)|#)/;
const stale = files.flatMap(file => {
  const text = readFileSync(file, 'utf8');
  return oldPublicLink.test(text) ? [path.relative(repo, file)] : [];
});
assert.deepEqual(stale, [], `Active files still link to legacy docs/: ${stale.join(', ')}`);

let localLinks = 0;
const missingLinks = [];
const contentRoot = path.join(root, 'src/content/docs');
for (const file of files) {
  const text = readFileSync(file, 'utf8');
  for (const match of text.matchAll(/(?<!\!)\[[^\]]*\]\(([^)]+)\)/g)) {
    const href = match[1].trim();
    if (!href || /^(?:#|https?:|mailto:|\/)/.test(href) || href.includes('://')) continue;
    const target = decodeURIComponent(href.split('#', 1)[0]);
    if (!target) continue;
    localLinks += 1;
    let exists;
    if (file.startsWith(`${contentRoot}${path.sep}`) && target.startsWith('../../../evidence/')) {
      exists = existsSync(path.join(root, 'public', target.slice('../../../'.length)));
    } else if (file.startsWith(`${contentRoot}${path.sep}`) && target.endsWith('/')) {
      const relative = path.relative(contentRoot, file).split(path.sep).join('/');
      const sourceRoute = relative.replace(/(?:index)?\.mdx?$/, '');
      const targetRoute = path.posix.normalize(path.posix.join(sourceRoute, target)).replace(/\/$/, '');
      exists = ['md', 'mdx'].some(extension => existsSync(path.join(contentRoot, `${targetRoute}.${extension}`))) ||
        ['md', 'mdx'].some(extension => existsSync(path.join(contentRoot, targetRoute, `index.${extension}`)));
    } else {
      exists = existsSync(path.resolve(path.dirname(file), target));
    }
    if (!exists) {
      missingLinks.push(`${path.relative(repo, file)} -> ${target}`);
    }
  }
}
assert.deepEqual(missingLinks, [], `Missing active links:\n${missingLinks.join('\n')}`);

const publicLeaks = files.flatMap(file => {
  const body = readFileSync(file, 'utf8');
  const markers = ['_doc/', '/home/asd/', '/var/tmp/', 'engine-tickets/'];
  return markers.filter(marker => body.includes(marker))
    .map(marker => `${path.relative(repo, file)} -> ${marker}`);
});
assert.deepEqual(publicLeaks, [], `Private references in public documentation:\n${publicLeaks.join('\n')}`);

const evidenceRoot = path.join(root, 'public/evidence');
const evidenceReports = markdownBelow(evidenceRoot)
  .filter(file => file.endsWith('.md') && path.basename(file) !== 'README.md' &&
    path.basename(file) !== 'README.ru.md');
const englishReports = evidenceReports.filter(file => !file.endsWith('.ru.md'));
const russianReports = evidenceReports.filter(file => file.endsWith('.ru.md'));
assert.equal(englishReports.length, 9, 'Unexpected English evidence report count');
assert.equal(russianReports.length, englishReports.length,
  'Every English evidence report must have a Russian counterpart');

function sha256Identities(source) {
  return [...source.matchAll(/\b[0-9a-f]{64}\b/g)].map(match => match[0]).sort();
}

for (const englishPath of englishReports) {
  const russianPath = englishPath.replace(/\.md$/, '.ru.md');
  assert(existsSync(russianPath), `Missing Russian evidence report: ${path.relative(repo, russianPath)}`);
  const english = readFileSync(englishPath, 'utf8');
  const russian = readFileSync(russianPath, 'utf8');
  const englishName = path.basename(englishPath);
  const russianName = path.basename(russianPath);
  assert(english.includes(`[Русский](${russianName})`), `${englishName} does not link ${russianName}`);
  assert(russian.includes(`[English](${englishName})`), `${russianName} does not link ${englishName}`);
  assert(!/[А-Яа-яЁё]/.test(english.replace(`[Русский](${russianName})`, '')),
    `${englishName} contains Russian prose`);
  assert(/[А-Яа-яЁё]/.test(russian), `${russianName} does not contain Russian prose`);
  assert.deepEqual(sha256Identities(english), sha256Identities(russian),
    `${englishName} and ${russianName} carry different SHA-256 identities`);
}

const publication = parse(readFileSync(path.join(root, '_meta/publication.toml'), 'utf8'));
assert.equal(publication.version, '1.2');
assert.equal(publication.channel, 'development');
const workflow = readFileSync(path.join(repo, '.github/workflows/jekyll-gh-pages.yml'), 'utf8');
assert(workflow.includes('npm run build:publication'));
assert(!workflow.includes('jekyll-build-pages'));
const ci = readFileSync(path.join(repo, '.github/workflows/ci.yml'), 'utf8');
assert(ci.includes('npm run test:publication'));
assert(!ci.includes('Path("docs")'));
const astroConfig = readFileSync(path.join(root, 'astro.config.mjs'), 'utf8');
const footer = readFileSync(path.join(root, 'src/components/ManualFooter.astro'), 'utf8');
assert(astroConfig.includes("process.env.DOCS_SITE || 'https://radixdb.org'"));
assert(astroConfig.includes("mc.yandex.ru/metrika/tag.js?id=112445210"));
assert(footer.includes('mc.yandex.ru/watch/112445210'));
for (const locale of ['en', 'ru']) {
  const index = readFileSync(path.join(contentRoot, locale, 'index.md'), 'utf8');
  const introduction = readFileSync(path.join(contentRoot, locale, 'preface/what-is-radixdb.md'), 'utf8');
  for (const marker of ['https://radixdb.org', 'mailto:dev@radixdb.org', 'http://light-soft.info/']) {
    assert(index.includes(marker), `${locale}/index.md is missing ${marker}`);
    assert(introduction.includes(marker), `${locale}/preface/what-is-radixdb.md is missing ${marker}`);
  }
}

console.log(JSON.stringify({ active_files: files.length, local_links: localLinks,
  live_docs_tree: false, stale_public_links: 0, publication_channel: publication.channel,
  public_evidence_files: markdownBelow(path.join(root, 'public/evidence')).length,
  bilingual_evidence_pairs: englishReports.length, private_references: 0,
  official_site: 'https://radixdb.org', metrika_id: 112445210,
  astro_pages_workflow: true, passed: true }, null, 2));
