import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validatePublication, chapterDestination, chapterRoute } from './publication.mjs';
import { normalizeBase, redirectHtml, routeOutput, validateSite } from './package-site.mjs';

test('development target may precede application version', () => {
  validatePublication({ version: '1.1', channel: 'development' }, '1.0.0');
});
test('release must match application and have a valid channel', () => {
  validatePublication({ version: '1.1', channel: 'release' }, '1.1.0');
  assert.throws(() => validatePublication({ version: '1.1', channel: 'release' }, '1.0.0'));
  assert.throws(() => validatePublication({ version: '1.1', channel: 'draft' }, '1.1.0'));
});
test('version routing preserves language and chapter or returns to contents', () => {
  const build = { base: '/manual/1.0/', locales: ['en', 'ru'], chapters: ['', 'appendices/glossary'] };
  assert.deepEqual(chapterDestination(build, 'ru', 'appendices/glossary'), { href: '/manual/1.0/ru/appendices/glossary/', preserved: true });
  assert.deepEqual(chapterDestination(build, 'ru', 'programming/jobs'), { href: '/manual/1.0/ru/', preserved: false });
});

test('published chapter routes omit index file names', () => {
  assert.equal(chapterRoute({ path: 'index.md' }), '');
  assert.equal(chapterRoute({ path: 'reference/sql/index.md' }), 'reference/sql');
  assert.equal(chapterRoute({ path: 'reference/sql/select.md' }), 'reference/sql/select');
});

test('publication paths reject traversal and normalize slashes', () => {
  assert.equal(normalizeBase('/project/manual/1.1', 'base'), '/project/manual/1.1/');
  assert.equal(validateSite('https://docs.example'), 'https://docs.example');
  assert.throws(() => normalizeBase('/project/../private', 'base'));
  assert.throws(() => validateSite('file:///tmp/docs'));
});

test('legacy routes map to static files and preserve URL suffixes', () => {
  assert.equal(routeOutput('en/getting-started.html'), 'en/getting-started.html');
  assert.equal(routeOutput('en/getting-started/'), 'en/getting-started/index.html');
  const html = redirectHtml('https://docs.example', '/manual/1.1/en/tutorial/getting-started/');
  assert(html.includes('location.search + location.hash'));
  assert(html.includes('https://docs.example/manual/1.1/en/tutorial/getting-started/'));
});
