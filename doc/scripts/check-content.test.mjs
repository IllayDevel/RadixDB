import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const checker = fileURLToPath(new URL('./check-content.mjs', import.meta.url));

function fixture() {
  const repo = mkdtempSync(path.join(tmpdir(), 'radixdb-docs-content-contract-'));
  const root = path.join(repo, 'doc');
  mkdirSync(path.join(root, '_meta'), { recursive: true });
  mkdirSync(path.join(root, 'src/content/docs/en/guide'), { recursive: true });
  mkdirSync(path.join(root, 'src/content/docs/ru/guide'), { recursive: true });
  writeFileSync(path.join(repo, 'Cargo.toml'), '[workspace.package]\nversion = "1.1.0"\n');
  writeFileSync(path.join(repo, 'source.rs'), '// fixture source\n');
  writeFileSync(path.join(root, '_meta/publication.toml'), [
    'version = "1.1"',
    'channel = "development"',
    'application_manifest = "../Cargo.toml"',
    '',
  ].join('\n'));
  writeFileSync(path.join(root, '_meta/chapters.toml'), [
    'target_version = "1.1"',
    'locales = ["en", "ru"]',
    '',
    '[[chapters]]',
    'id = "fixture"',
    'path = "guide/index.md"',
    'order = 1',
    'status = "draft"',
    'feature_status = "implemented"',
    'sources = ["source.rs"]',
    'tests = ["fixture"]',
    'verified_commit = "e77b2a73c6252bcfc1ba380c5a97a24d335acbd4"',
    'reviewer = "Fixture"',
    'legacy_paths = []',
    '',
  ].join('\n'));
  const body = '---\ntitle: Fixture\n---\n\n```sql\nSELECT 1;\n```\n';
  writeFileSync(path.join(root, 'src/content/docs/en/guide/index.md'), body);
  writeFileSync(path.join(root, 'src/content/docs/ru/guide/index.md'), body);
  return { repo, root };
}

function check(repo, root) {
  return spawnSync(process.execPath, [checker], {
    encoding: 'utf8',
    env: { ...process.env, RADIXDB_DOCS_REPO: repo, RADIXDB_DOCS_ROOT: root },
  });
}

test('complete bilingual fixture passes the content contract', () => {
  const { repo, root } = fixture();
  try {
    const result = check(repo, root);
    assert.equal(result.status, 0, result.stderr);
  } finally {
    rmSync(repo, { recursive: true, force: true });
  }
});

test('missing localized chapter fails the content contract', () => {
  const { repo, root } = fixture();
  try {
    rmSync(path.join(root, 'src/content/docs/ru/guide/index.md'));
    const result = check(repo, root);
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /Missing locale:/);
  } finally {
    rmSync(repo, { recursive: true, force: true });
  }
});
