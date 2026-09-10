import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { parse } from 'smol-toml';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const serverBin = process.env.RADIXDB_DOCS_SERVER;
const smokeBin = process.env.RADIXDB_DOCS_SMOKE;
const revision = process.env.RADIXDB_DOCS_REVISION;
for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_SERVER', serverBin],
  ['RADIXDB_DOCS_SMOKE', smokeBin],
]) assert(value && path.isAbsolute(value), `Set ${name} to an absolute pinned path`);
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 300000, maxBuffer: 32 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function cleanWorktree() {
  const result = run('git', ['status', '--short'], { cwd: worktree, timeout: 15000 });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, '');
}

function source(locale) {
  return readFileSync(path.join(root, 'src/content/docs', locale,
    'administration/configuration.md'), 'utf8');
}

function codeBlocks(locale) {
  return [...source(locale).matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)].map(match => match[1]);
}

function rows(locale) {
  return source(locale).split('\n').filter(line => /^\| `[^`]+` \|/.test(line))
    .map(line => {
      const [key, codeDefault, releaseValue, rule] = line.split('|').slice(1, 5)
        .map(value => value.trim().replaceAll('`', ''));
      return { key, codeDefault, releaseValue, rule };
    });
}

async function freePort() {
  const probe = net.createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const address = probe.address();
  assert(address && typeof address !== 'string');
  probe.close();
  await once(probe, 'close');
  return address.port;
}

async function waitFor(child, output, needle) {
  const deadline = Date.now() + 15000;
  while (!output.value.includes(needle)) {
    assert.equal(child.exitCode, null, output.value);
    assert(Date.now() < deadline, `timeout waiting for ${needle}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

async function stop(child, output) {
  const exited = once(child, 'exit');
  assert(child.kill('SIGTERM'));
  const [code, signal] = await Promise.race([
    exited,
    new Promise((_, reject) => setTimeout(() => reject(new Error('stop timeout')), 15000)),
  ]);
  assert.equal(signal, null);
  assert.equal(code, 0, output.value);
  assert(output.value.includes('stopped cleanly'), output.value);
}

async function start(config, needle) {
  const child = spawn(serverBin, ['--config', config], { stdio: ['ignore', 'pipe', 'pipe'] });
  const output = { value: '' };
  child.stdout.on('data', chunk => { output.value += chunk; });
  child.stderr.on('data', chunk => { output.value += chunk; });
  try {
    await waitFor(child, output, needle);
    return { child, output };
  } catch (error) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGTERM');
      await Promise.race([once(child, 'exit'), new Promise(resolve => setTimeout(resolve, 15000))]);
    }
    throw error;
  }
}

cleanWorktree();
const identity = run(serverBin, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
assert(identity.stdout.includes('protocol=17 '), identity.stdout);
assert(identity.stdout.includes('profile=release '), identity.stdout);

assert.deepEqual(codeBlocks('en'), codeBlocks('ru'));
assert.equal(codeBlocks('en').length, 6);
const enRows = rows('en');
const ruRows = rows('ru');
assert.equal(enRows.length, 27);
assert.deepEqual(enRows, ruRows);
assert.equal(new Set(enRows.map(row => row.key)).size, 27);

const templateBlock = codeBlocks('en').find(block => block.startsWith('[server]\n'));
assert(templateBlock);
const published = parse(templateBlock).server;
const tracked = parse(readFileSync(path.join(worktree, 'release/server.toml'), 'utf8')).server;
assert.deepEqual(published, tracked);
const serverRows = enRows.filter(row => ![
  'transport', 'authentication.root_password_verifier', 'plugins.package_directories',
].includes(row.key));
assert.deepEqual(serverRows.map(row => row.key), Object.keys(tracked));
for (const row of serverRows) assert.equal(row.releaseValue, String(tracked[row.key]), row.key);
assert.equal(enRows.find(row => row.key === 'transport').releaseValue, 'omitted (plaintext)');
assert.equal(enRows.find(row => row.key === 'authentication.root_password_verifier').releaseValue,
  'absent');
assert.equal(enRows.find(row => row.key === 'max_connections').codeDefault, '151');
assert.equal(enRows.find(row => row.key === 'max_connections').releaseValue, '64');

const configTests = run('cargo', ['test', '--locked', '--lib', 'server::config::tests::'], {
  cwd: worktree,
});
assert.equal(configTests.status, 0, `${configTests.stdout}\n${configTests.stderr}`);
const configTestCount = [...`${configTests.stdout}\n${configTests.stderr}`.matchAll(
  /test result: ok\. (\d+) passed/g)].reduce((sum, match) => sum + Number(match[1]), 0);
assert(configTestCount > 0, configTests.stdout);

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-config-'));
let running;
try {
  const oldPort = await freePort();
  let newPort = await freePort();
  while (newPort === oldPort) newPort = await freePort();
  const config = path.join(temp, 'server.toml');
  const writeConfig = port => writeFileSync(config,
    `[server]\nbind_ip = "127.0.0.1"\nport = ${port}\ndata_dir = "${path.join(temp, 'data')}"\n`);

  writeConfig(oldPort);
  running = await start(config, `listening on 127.0.0.1:${oldPort}`);
  writeConfig(newPort);
  const selected = run(serverBin, ['--config', config, '--print-endpoint']);
  assert.equal(selected.status, 0, selected.stderr);
  assert.equal(selected.stdout.trim(), `127.0.0.1 ${newPort}`);
  const oldSmoke = run(smokeBin, [`127.0.0.1:${oldPort}`], { timeout: 15000 });
  assert.equal(oldSmoke.status, 0, oldSmoke.stderr);
  const newBeforeRestart = run(smokeBin, [`127.0.0.1:${newPort}`], { timeout: 15000 });
  assert.notEqual(newBeforeRestart.status, 0);
  await stop(running.child, running.output);
  running = undefined;

  running = await start(config, `listening on 127.0.0.1:${newPort}`);
  const newAfterRestart = run(smokeBin, [`127.0.0.1:${newPort}`], { timeout: 15000 });
  assert.equal(newAfterRestart.status, 0, newAfterRestart.stderr);
  await stop(running.child, running.output);
  running = undefined;

  const invalidCases = [
    [{ port: 0 }, 'server port must not be zero'],
    [{ max_frame_bytes: 1024, max_inflight_frame_bytes: 512 }, 'must not exceed max_inflight_frame_bytes'],
    [{ max_frame_bytes: 1024, cursor_batch_max_bytes: 1025 }, 'must not exceed max_frame_bytes'],
    [{ max_compaction_jobs: 9 }, 'must be in 1..=8'],
    [{ page_cache_level: 11 }, 'must be in 0..=10'],
    [{ target_volume_rows: 65535 }, 'must be at least 65536'],
    [{ data_dir: `${path.join(temp, 'invalid')}?query` }, 'must not contain `?`'],
  ];
  for (const [index, [overrides, error]] of invalidCases.entries()) {
    const values = {
      bind_ip: '127.0.0.1', port: 15441, data_dir: path.join(temp, 'invalid-data'),
      ...overrides,
    };
    const invalid = path.join(temp, `invalid-${index}.toml`);
    const lines = ['[server]', ...Object.entries(values).map(([key, value]) =>
      `${key} = ${typeof value === 'string' ? JSON.stringify(value) : value}`)];
    writeFileSync(invalid, `${lines.join('\n')}\n`);
    const rejected = run(serverBin, ['--config', invalid], { timeout: 15000 });
    assert.notEqual(rejected.status, 0, JSON.stringify(overrides));
    assert(rejected.stderr.includes(error), `${JSON.stringify(overrides)}: ${rejected.stderr}`);
  }

  const unknown = path.join(temp, 'unknown.toml');
  writeFileSync(unknown,
    `[server]\nbind_ip = "127.0.0.1"\nport = ${await freePort()}\ndata_dir = "${temp}"\nmax_conections = 12\n`);
  const rejectedUnknown = run(serverBin, ['--config', unknown, '--print-endpoint'], { timeout: 15000 });
  assert.notEqual(rejectedUnknown.status, 0);
  assert(rejectedUnknown.stderr.includes('unknown field'), rejectedUnknown.stderr);

  const retired = path.join(temp, 'retired.toml');
  writeFileSync(retired,
    `[server]\nbind_ip = "127.0.0.1"\nport = ${await freePort()}\ndata_dir = "${temp}"\nscan_prefetch_cache_bytes = 1024\n`);
  const rejectedRetired = run(serverBin, ['--config', retired, '--print-endpoint'], {
    timeout: 15000,
  });
  assert.notEqual(rejectedRetired.status, 0);
  assert(rejectedRetired.stderr.includes('unknown field'), rejectedRetired.stderr);

  const remotePlaintext = path.join(temp, 'remote-plaintext.toml');
  writeFileSync(remotePlaintext,
    `[server]\nbind_ip = "0.0.0.0"\nport = ${await freePort()}\ndata_dir = "${temp}"\n\n[server.transport]\nmode = "plaintext"\n`);
  const acceptedRemotePlaintext = run(serverBin,
    ['--config', remotePlaintext, '--print-endpoint'], { timeout: 15000 });
  assert.equal(acceptedRemotePlaintext.status, 0, acceptedRemotePlaintext.stderr);

  const malformedVerifier = path.join(temp, 'malformed-verifier.toml');
  writeFileSync(malformedVerifier,
    `[server]\nbind_ip = "127.0.0.1"\nport = ${await freePort()}\ndata_dir = "${temp}"\n\n[server.authentication]\nroot_password_verifier = "not-a-phc-string"\n`);
  const rejectedVerifier = run(serverBin,
    ['--config', malformedVerifier, '--print-endpoint'], { timeout: 15000 });
  assert.notEqual(rejectedVerifier.status, 0);
  assert(rejectedVerifier.stderr.includes('valid PHC string'), rejectedVerifier.stderr);

  cleanWorktree();
  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    parameters: enRows.length,
    published_code_blocks: 6,
    release_template_exact: true,
    config_engine_tests: configTestCount,
    rejected_runtime_configs: invalidCases.length,
    rejected_unknown_keys: 2,
    rejected_root_verifier: true,
    remote_plaintext_password_transport: true,
    hot_reload: false,
    restart_applied_new_endpoint: true,
    passed: true,
  }, null, 2));
} finally {
  if (running?.child.exitCode === null && running?.child.signalCode === null) {
    running.child.kill('SIGTERM');
    await Promise.race([once(running.child, 'exit'), new Promise(resolve => setTimeout(resolve, 15000))]);
  }
  rmSync(temp, { recursive: true, force: true });
}
