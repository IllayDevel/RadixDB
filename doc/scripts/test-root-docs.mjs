import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const manualRoot = fileURLToPath(new URL('../', import.meta.url));
const repoRoot = path.resolve(manualRoot, '..');
const release = '804027a4ee8426b6f7bd083c6e26a1895603dc38';
const licensingBaseline = '43ef351222d247145118043107212b0ac9a10eb5';
const expectedLegalHashes = {
  LICENSE: '422ced98345bf51cd39b63baa8291d97d07169a12063ccec28648eb77a9cdbba',
  NOTICE: 'cab58076f1c796396cdc506e0fed3217967819379e1d2ae979d2157ae03f1f42',
};

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: repoRoot, encoding: 'utf8', timeout: 15000, maxBuffer: 16 * 1024 * 1024,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

function read(relative) {
  return readFileSync(path.join(repoRoot, relative), 'utf8');
}

const requiredPairs = [
  ['README.md', 'README.ru.md'],
  ['CHANGELOG.md', 'CHANGELOG.ru.md'],
  ['CONTRIBUTING.md', 'CONTRIBUTING.ru.md'],
  ['SECURITY.md', 'SECURITY.ru.md'],
  ['CODE_OF_CONDUCT.md', 'CODE_OF_CONDUCT.ru.md'],
];
const agentPair = ['AGENTS.md', 'AGENTS.ru.md'];
const agentPresence = agentPair.map(relative => existsSync(path.join(repoRoot, relative)));
assert.equal(agentPresence[0], agentPresence[1], 'AGENTS translations must be present or absent together');
const pairs = [...requiredPairs, ...(agentPresence[0] ? [agentPair] : [])];

for (const [englishPath, russianPath] of pairs) {
  assert(existsSync(path.join(repoRoot, englishPath)), `missing ${englishPath}`);
  assert(existsSync(path.join(repoRoot, russianPath)), `missing ${russianPath}`);
  const english = read(englishPath);
  const russian = read(russianPath);
  assert(english.includes(`[Русский](${russianPath})`), `${englishPath} does not link ${russianPath}`);
  assert(russian.includes(`[English](${englishPath})`), `${russianPath} does not link ${englishPath}`);
}

for (const relative of ['README.md', 'README.ru.md']) {
  const source = read(relative);
  assert(source.includes('<img src="logo-light.png" alt="RadixDB" width="720">'),
    `${relative} does not render the light root logo`);
}
assert(read('README.md').indexOf('[Русский](README.ru.md)') < 300,
  'The primary English README must expose the Russian translation near its header');

for (const relative of ['README.md', 'README.ru.md', 'doc/README.md', 'doc/README.ru.md']) {
  const source = read(relative);
  for (const marker of [
    'https://radixdb.org',
    'mailto:dev@radixdb.org',
    'http://light-soft.info/',
  ]) assert(source.includes(marker), `${relative} does not name ${marker}`);
}

function codeBlocks(source) {
  return [...source.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(match => match[1]);
}
assert.deepEqual(codeBlocks(read('CONTRIBUTING.md')), codeBlocks(read('CONTRIBUTING.ru.md')));

function releases(source) {
  return [...source.matchAll(/^## (\d+\.\d+\.\d+) - (\d{4}-\d{2}-\d{2})$/gm)]
    .map(match => `${match[1]}:${match[2]}`);
}
assert.deepEqual(releases(read('CHANGELOG.md')), releases(read('CHANGELOG.ru.md')));
assert.deepEqual(releases(read('CHANGELOG.md')), ['1.1.0:2026-09-08', '1.0.0:2026-09-07']);

for (const relative of ['README.md', 'README.ru.md', 'CONTRIBUTING.md',
  'CONTRIBUTING.ru.md', 'SECURITY.md', 'SECURITY.ru.md']) {
  assert(read(relative).includes('1.1.0'), `${relative} does not name current release`);
}

for (const relative of ['README.md', 'README.ru.md', 'SECURITY.md', 'SECURITY.ru.md']) {
  const source = read(relative);
  assert(source.includes('radixdb-password') || relative.startsWith('SECURITY'),
    `${relative} does not describe the root password utility`);
  assert(source.includes('Argon2id'), `${relative} does not name the root verifier algorithm`);
  assert(source.includes('loopback'), `${relative} does not preserve the recovery boundary`);
}

const staleClaims = [
  'Development for 1.1 extends',
  'ACL enforcement is still under development',
  'accepted release baseline\nis 1.0.0',
  'Для версии 1.1 разрабатывается',
  'Проверки ACL ещё разрабатываются',
  'Базовый релиз 1.0.0 принят',
];
for (const relative of ['README.md', 'README.ru.md', 'CONTRIBUTING.md',
  'CONTRIBUTING.ru.md', 'SECURITY.md', 'SECURITY.ru.md']) {
  const source = read(relative);
  for (const claim of staleClaims) assert(!source.includes(claim), `${relative} retains: ${claim}`);
}

if (agentPresence[0]) {
  for (const marker of ['NVMe', 'Atom/HDD', '`10`', 'versioned-format-name']) {
    assert(read('AGENTS.md').includes(marker), `AGENTS.md is missing ${marker}`);
    assert(read('AGENTS.ru.md').includes(marker), `AGENTS.ru.md is missing ${marker}`);
  }
}

assert(read('LICENSE.ru.md').includes('не юридический перевод'));
assert(read('LICENSE.ru.md').includes('[LICENSE](LICENSE)'));
assert(read('NOTICE.ru.md').includes('Это справочный перевод'));
assert(read('NOTICE.ru.md').includes('Юридически определяющим'));
assert(read('NOTICE.ru.md').includes('[NOTICE](NOTICE)'));

const tagProbe = spawnSync('git', ['show-ref', '--verify', '--quiet', 'refs/tags/v1.1.0'], {
  cwd: repoRoot, encoding: 'utf8', timeout: 15000,
});
assert(!tagProbe.error, String(tagProbe.error));
assert([0, 1].includes(tagProbe.status), tagProbe.stderr);
const releaseTagVerified = tagProbe.status === 0;
if (releaseTagVerified) {
  const tagCommit = run('git', ['rev-parse', 'v1.1.0^{commit}']).trim();
  assert.equal(tagCommit, release);
}
const legalHashes = {};
for (const relative of ['LICENSE', 'NOTICE']) {
  const current = read(relative);
  const hash = createHash('sha256').update(current).digest('hex');
  assert.equal(hash, expectedLegalHashes[relative],
    `${relative} differs from the accepted licensing baseline`);
  legalHashes[relative] = hash;
}

const documents = [
  ...pairs.flat(), 'LICENSE', 'LICENSE.ru.md', 'NOTICE', 'NOTICE.ru.md',
];
let links = 0;
for (const relative of documents.filter(file => file.endsWith('.md'))) {
  const source = read(relative);
  for (const match of source.matchAll(/\[[^\]]+\]\(([^)]+)\)/g)) {
    const href = match[1];
    if (/^(?:https?:|mailto:|#)/.test(href)) continue;
    const target = decodeURIComponent(href.split('#')[0]);
    assert(existsSync(path.resolve(repoRoot, path.dirname(relative), target)),
      `${relative} has a missing local link: ${href}`);
    links += 1;
  }
}

console.log(JSON.stringify({
  release_tag: 'v1.1.0',
  release_commit: release,
  release_tag_verified: releaseTagVerified,
  licensing_baseline_commit: licensingBaseline,
  bilingual_pairs: pairs.length,
  legal_explanations: 2,
  local_links: links,
  legal_original_sha256: legalHashes,
  passed: true,
}, null, 2));
