import assert from 'node:assert/strict';
import {
  cpSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const docRoot = fileURLToPath(new URL('../', import.meta.url));
const repositoryRoot = path.resolve(docRoot, '..');
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
assert(worktree && path.isAbsolute(worktree),
  'Set RADIXDB_DOCS_WORKTREE to the absolute frozen worktree path');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: worktree,
    encoding: 'utf8',
    timeout: 600000,
    maxBuffer: 64 * 1024 * 1024,
    ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function source(relative) {
  return readFileSync(path.join(worktree, relative), 'utf8');
}

function document(locale, relative) {
  return readFileSync(path.join(docRoot, 'src/content/docs', locale, relative), 'utf8');
}

function codeBlocks(body) {
  return [...body.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

const status = run('git', ['status', '--short']);
assert.equal(status.status, 0, status.stderr);
assert.equal(status.stdout, '');
const head = run('git', ['rev-parse', 'HEAD']);
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);

const documents = [
  'administration/extensions.md',
  'programming/native-extensions.md',
  'reference/sql/extensions.md',
  'reference/programs/cargo-radixdb-plugin.md',
];
let blockCount = 0;
for (const relative of documents) {
  const en = document('en', relative);
  const ru = document('ru', relative);
  const enBlocks = codeBlocks(en);
  assert.deepEqual(enBlocks, codeBlocks(ru), `EN/RU code mismatch: ${relative}`);
  blockCount += enBlocks.length;
}

for (const marker of [
  'It is not a sandbox',
  'package_directories',
  'restricted diagnostic mode',
  'ALTER EXTENSION UPDATE',
]) assert(document('en', 'administration/extensions.md').includes(marker),
  `extension administration misses ${marker}`);

for (const marker of [
  '#[radixdb_plugin',
  '#[radix_type',
  '#[radixdb_scalar',
  '#[radixdb_batch',
  '#[radixdb_operator_class',
  '#[radixdb_planner_support',
  'semantic_revision',
]) assert(document('en', 'programming/native-extensions.md').includes(marker),
  `extension authoring guide misses ${marker}`);

const protocol = source('crates/radixdb-protocol/src/lib.rs');
for (const marker of [
  'pub const PROTOCOL_VERSION: u16 = 17;',
  'ExternalValueV1',
  'WireValue::External',
]) assert(protocol.includes(marker), `protocol owner misses ${marker}`);

const catalog = source('crates/radixdb-catalog/src/kind.rs');
assert(catalog.includes('pub const EXTENSION_CATALOG_MINOR: u16 = 2;'));
assert(catalog.includes('pub const LATEST_CATALOG_MINOR: u16 = EXTENSION_CATALOG_MINOR;'));

const abi = source('crates/radixdb-plugin-abi/src/constants.rs');
for (const marker of [
  'pub const RADIX_ABI_MAJOR: u16 = 1;',
  'pub const RADIX_ABI_MINOR: u16 = 0;',
  'pub const RADIX_MAX_LOCAL_ID_BYTES: u32 = 255;',
  'pub const RADIX_MAX_EXTERNAL_VALUE_BYTES: u32 = 16 * 1024 * 1024;',
  'pub const RADIX_MAX_PLANNER_SPANS: u32 = 4096;',
]) assert(abi.includes(marker), `plugin ABI misses ${marker}`);

const highlighter = readFileSync(path.join(docRoot, 'src/syntax/radixdb-sql.mjs'), 'utf8');
for (const keyword of [
  'EXTENSION', 'TYPE', 'VERSION', 'NATIVE', 'OPERATOR', 'CLASS', 'PLANNER',
  'SUPPORT', 'LEFTARG', 'RIGHTARG',
]) assert(highlighter.includes(`'${keyword}'`), `SQL highlighter misses ${keyword}`);

const engineGates = [
  ['-p', 'radixdb-sql', '--test', 'extension_ddl'],
  ['-p', 'radixdb-catalog', '--test', 'catalog_extension_binding'],
  ['-p', 'radixdb-plugin-abi', '--test', 'contract'],
  ['-p', 'radixdb-plugin', '--test', 'codec_contract'],
  ['-p', 'radixdb-executor', '--test', 'extension_binding',
    'reopen_without_package_is_restricted_but_root_can_drop_the_binding', '--', '--exact'],
  ['--manifest-path', 'crates/radixdb-spatial/Cargo.toml', '--test', 'database_lifecycle'],
];
for (const args of engineGates) {
  const result = run('cargo', ['test', '--locked', ...args]);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
}

const temporary = mkdtempSync(path.join(os.tmpdir(), 'radixdb-doc-plugin-'));
try {
  const project = path.join(temporary, 'rust-plugin');
  cpSync(path.join(repositoryRoot, 'examples/public/rust-plugin'), project, {
    recursive: true,
    filter: sourcePath => !sourcePath.endsWith(`${path.sep}target`),
  });
  const manifestPath = path.join(project, 'Cargo.toml');
  const sdkPath = path.join(worktree, 'crates/radixdb-plugin');
  const manifest = readFileSync(manifestPath, 'utf8').replace(
    'path = "../../../crates/radixdb-plugin"',
    `path = "${sdkPath}"`,
  );
  writeFileSync(manifestPath, manifest);

  for (const command of ['check', 'test-host']) {
    const result = run('cargo', [
      'run', '--locked', '--quiet', '-p', 'cargo-radixdb-plugin', '--', command,
      '--manifest-path', manifestPath,
      '--target-dir', path.join(temporary, 'target'),
    ]);
    assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}

console.log(JSON.stringify({
  revision,
  documents: documents.length * 2,
  code_blocks: blockCount,
  engine_gates: engineGates.length,
  public_plugin_check: true,
  public_plugin_host_test: true,
  passed: true,
}, null, 2));
