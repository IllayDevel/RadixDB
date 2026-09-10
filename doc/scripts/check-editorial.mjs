import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const siteRoot = fileURLToPath(new URL('../', import.meta.url));
const root = path.join(siteRoot, 'src/content/docs');

function walk(directory, prefix = '') {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const relative = `${prefix}${entry.name}`;
    if (entry.isDirectory()) return walk(path.join(directory, entry.name), `${relative}/`);
    return /\.mdx?$/.test(entry.name) ? [relative] : [];
  });
}

function headings(source) {
  return [...source.matchAll(/^(#{2,6})\s+/gm)].map(match => match[1].length);
}

function links(source) {
  return [...source.matchAll(/(?<!!)\[[^\]]*\]\(([^)]+)\)/g)].map(match => match[1]);
}

const canonicalLink = target => target.replace(/\.ru\.md$/, '.md');

function prose(source) {
  return source
    .replace(/^```[^\n]*\n[\s\S]*?^```/gm, '')
    .replace(/`[^`\n]*`/g, '');
}

const forbidden = [
  ['editorial placeholder', /\b(?:TODO|TBD|FIXME)\b/i],
  ['engineering log marker', /\b(?:in-progress|Codex)\b/i],
  ['private workstation path', /\/(?:home\/asd|var\/tmp)\b|radixdb-docs-verify/i],
  ['private key', /BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY/],
  ['AWS-style access key', /AKIA[0-9A-Z]{16}/],
  ['stale 1.1 release promise', /upcoming (?:RadixDB )?1\.1|future 1\.1|not an accepted release/i],
  ['устаревшее обещание выпуска 1.1', /будущ\w*[^\n]*1\.1|не принятый релиз/i],
  ['stale implementation promise', /after the corresponding implementation is accepted|working design/i],
  ['устаревшее обещание реализации', /после при[её]мки соответствующей|рабочий проект/i],
  ['internal remediation reference', /engine[- ]ticket|remediation|_doc\/new_doc\/engine-tickets/i],
  ['internal remediation identifier', /\b(?:AUTH|ACL|BACKUP|BUILD|CLI|CLIENT|JOB|MIGRATION|MONITOR|PL|PROGRAMMING|SERVER|STORAGE)-\d+\b/],
];

const locales = ['en', 'ru'];
const paths = Object.fromEntries(locales.map(locale => [locale, walk(path.join(root, locale)).sort()]));
assert.deepEqual(paths.en, paths.ru, 'English and Russian public page sets differ');

for (const relative of paths.en) {
  assert(/^(?:[a-z0-9][a-z0-9_-]*\/)*[a-z0-9][a-z0-9_-]*\.mdx?$/.test(relative),
    `Unsafe or mixed-case public path: ${relative}`);
  const en = readFileSync(path.join(root, 'en', relative), 'utf8');
  const ru = readFileSync(path.join(root, 'ru', relative), 'utf8');
  assert.deepEqual(headings(en), headings(ru), `Heading structure differs: ${relative}`);
  assert.deepEqual(links(en).map(canonicalLink), links(ru).map(canonicalLink),
    `Link targets differ: ${relative}`);
  for (const [locale, source] of [['en', en], ['ru', ru]]) {
    const text = prose(source);
    for (const [label, pattern] of forbidden) {
      assert(!pattern.test(text), `${label}: ${locale}/${relative}`);
    }
  }
}

const notFound = readFileSync(path.join(root, '404.md'), 'utf8');
for (const [label, pattern] of forbidden) assert(!pattern.test(prose(notFound)), `${label}: 404.md`);

const readmes = ['README.md', 'README.ru.md'].map(name => readFileSync(path.join(siteRoot, name), 'utf8'));
assert.deepEqual(headings(readmes[0]), headings(readmes[1]), 'Project README heading structure differs');
const readmeLinks = readmes.map(links);
assert.equal(readmeLinks[0][0], 'README.ru.md', 'English README must link to Russian');
assert.equal(readmeLinks[1][0], 'README.md', 'Russian README must link to English');
assert.deepEqual(readmeLinks[0].slice(1).map(canonicalLink), readmeLinks[1].slice(1).map(canonicalLink),
  'Project README link targets differ');
for (const [index, source] of readmes.entries()) {
  for (const [label, pattern] of forbidden) {
    assert(!pattern.test(prose(source)), `${label}: ${index === 0 ? 'README.md' : 'README.ru.md'}`);
  }
}

console.log(`PASS: editorial contract for ${paths.en.length} EN/RU page pairs, project README pair and 404`);
