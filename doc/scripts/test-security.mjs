import assert from 'node:assert/strict';
import { once } from 'node:events';
import {
  copyFileSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const serverBin = process.env.RADIXDB_DOCS_SERVER;
const passwordBin = serverBin && path.join(path.dirname(serverBin), 'radixdb-password');
const revision = process.env.RADIXDB_DOCS_REVISION;
for (const [name, value] of [
  ['RADIXDB_DOCS_WORKTREE', worktree],
  ['RADIXDB_DOCS_SERVER', serverBin],
  ['radixdb-password', passwordBin],
]) assert(value && path.isAbsolute(value), `Set ${name} to an absolute pinned path`);
assert(revision && /^[0-9a-f]{40}$/.test(revision),
  'Set RADIXDB_DOCS_REVISION to the pinned 40-hex revision');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    timeout: 300000,
    maxBuffer: 64 * 1024 * 1024,
    ...options,
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

function cargoTest(args) {
  const result = run('cargo', ['test', '--locked', ...args], { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const counts = [...`${result.stdout}\n${result.stderr}`.matchAll(
    /test result: ok\. (\d+) passed/g)].map(match => Number(match[1]));
  assert(counts.length > 0, result.stdout);
  const passed = counts.reduce((sum, count) => sum + count, 0);
  assert(passed > 0, `empty test selection: cargo test ${args.join(' ')}`);
  return passed;
}

async function freePort() {
  const probe = net.createServer();
  probe.listen(0, '127.0.0.1');
  await once(probe, 'listening');
  const address = probe.address();
  assert(address && typeof address !== 'string');
  const port = address.port;
  probe.close();
  await once(probe, 'close');
  return port;
}

async function waitForOutput(child, output, needle) {
  const deadline = Date.now() + 15000;
  while (!output.value.includes(needle)) {
    assert.equal(child.exitCode, null, `server exited before ${needle}: ${output.value}`);
    assert(Date.now() < deadline, `timeout waiting for ${needle}: ${output.value}`);
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

function cargoPath(value) {
  return value.replaceAll('\\', '\\\\').replaceAll('"', '\\"');
}

cleanWorktree();
const head = run('git', ['rev-parse', 'HEAD'], { cwd: worktree, timeout: 15000 });
assert.equal(head.status, 0, head.stderr);
assert.equal(head.stdout.trim(), revision);
const identity = run(serverBin, ['--version'], { timeout: 15000 });
assert.equal(identity.status, 0, identity.stderr);
assert(identity.stdout.includes(`git=${revision} `), identity.stdout);
assert(identity.stdout.includes('protocol=17 '), identity.stdout);
assert(identity.stdout.includes('profile=release '), identity.stdout);

const sessionSource = readFileSync(path.join(worktree, 'src/server/session.rs'), 'utf8');
assert(sessionSource.includes('ClientMessage::AuthenticatePrincipal'));
assert(sessionSource.match(/ReadySession\s*\{[^}]*principal_id/s));
const protocolSource = readFileSync(path.join(worktree,
  'crates/radixdb-protocol/src/lib.rs'), 'utf8');
assert(protocolSource.includes('AuthenticatePrincipal'));
const clientSource = readFileSync(path.join(worktree,
  'crates/radixdb-client/src/async_client.rs'), 'utf8');
assert(clientSource.includes('authenticate_database'));
const protocolSecurity = run('rg', [
  '-ni', 'rustls|native[_-]?tls|tls_acceptor|starttls|cert_file|certificate_file',
  'Cargo.toml', 'src/server', 'crates/radixdb-client', 'crates/radixdb-protocol',
], { cwd: worktree, timeout: 15000 });
assert.equal(protocolSecurity.status, 0, protocolSecurity.stderr);
assert(protocolSecurity.stdout.includes('rustls'), protocolSecurity.stdout);

const statementSource = readFileSync(path.join(worktree,
  'crates/radixdb-sql/src/ast/statement.rs'), 'utf8');
assert(statementSource.includes('AlterSecuritySubject('));
assert(statementSource.includes('DropSecuritySubject('));
const securityAst = readFileSync(path.join(worktree,
  'crates/radixdb-sql/src/ast/security.rs'), 'utf8');
assert(securityAst.includes('admin_option: bool'));
assert(securityAst.includes('grant_option: bool'));
const productionContextValues = run('rg', [
  '-n', 'CURRENT_PRINCIPAL|CURRENT_EFFECTIVE_PRINCIPAL|CURRENT_STATEMENT_TIMESTAMP|CURRENT_REQUEST_ID|CURRENT_JOB_ID',
  'crates/radixdb-procedural/src', 'crates/radixdb-executor/src', 'crates/radixdb-sql/src',
], { cwd: worktree, timeout: 15000 });
assert.equal(productionContextValues.status, 0, productionContextValues.stderr);
for (const value of [
  'CURRENT_PRINCIPAL', 'CURRENT_EFFECTIVE_PRINCIPAL', 'CURRENT_STATEMENT_TIMESTAMP',
  'CURRENT_REQUEST_ID', 'CURRENT_JOB_ID',
]) assert(productionContextValues.stdout.includes(value), `missing ${value} runtime support`);
const expressionCompiler = readFileSync(path.join(worktree,
  'crates/radixdb-executor/src/expression/compiler.rs'), 'utf8');
assert(expressionCompiler.includes('"CURRENT_TRANSACTION_ID" =>'));

const docs = [
  'administration/authentication.md',
  'administration/access-control.md',
  'programming/routine-security.md',
];
const codeBlocks = body => [...body.matchAll(/^```[^\n]*\n([\s\S]*?)^```/gm)]
  .map(match => match[1]);
for (const document of docs) {
  const en = readFileSync(path.join(root, 'src/content/docs/en', document), 'utf8');
  const ru = readFileSync(path.join(root, 'src/content/docs/ru', document), 'utf8');
  assert.deepEqual(codeBlocks(en), codeBlocks(ru), `EN/RU code mismatch: ${document}`);
}
const authenticationDocs = readFileSync(path.join(root,
  'src/content/docs/en/administration/authentication.md'), 'utf8');
assert(authenticationDocs.includes('ordinary TCP'));
assert(authenticationDocs.includes('TLS is optional'));
assert(authenticationDocs.includes('authenticate_database'));
const accessDocs = readFileSync(path.join(root,
  'src/content/docs/en/administration/access-control.md'), 'utf8');
assert(accessDocs.includes('WITH GRANT OPTION'));
assert(accessDocs.includes('REVOKE ADMIN OPTION FOR'));
assert(accessDocs.includes('CREATE'));
assert(accessDocs.includes('does not implement row-level security'));
const routineSecurityDocs = readFileSync(path.join(root,
  'src/content/docs/en/programming/routine-security.md'), 'utf8');
for (const value of [
  'CURRENT_PRINCIPAL', 'CURRENT_EFFECTIVE_PRINCIPAL', 'CURRENT_REQUEST_ID', 'CURRENT_JOB_ID',
]) assert(routineSecurityDocs.includes(value), `missing ${value} from routine-security.md`);
assert(routineSecurityDocs.includes('EXECUTE'));

const temp = mkdtempSync(path.join(os.tmpdir(), 'radixdb-docs-security-'));
let child;
let authenticationMarker;
let aclMarker;
try {
  for (const name of ['authentication', 'acl']) {
    copyFileSync(path.join(root, 'examples/security', `${name}.rs`),
      path.join(temp, `${name}.rs`));
  }
  writeFileSync(path.join(temp, 'Cargo.toml'), `[package]\nname = "radixdb-docs-security"\nversion = "0.0.0"\nedition = "2021"\nrust-version = "1.97"\npublish = false\n\n[workspace]\n\n[dependencies]\nradixdb-catalog = { path = "${cargoPath(path.join(worktree, 'crates/radixdb-catalog'))}" }\nradixdb-client = { path = "${cargoPath(path.join(worktree, 'crates/radixdb-client'))}" }\nradixdb-core = { path = "${cargoPath(path.join(worktree, 'crates/radixdb-core'))}" }\nradixdb-executor = { path = "${cargoPath(path.join(worktree, 'crates/radixdb-executor'))}" }\nradixdb-storage = { path = "${cargoPath(path.join(worktree, 'crates/radixdb-storage'))}" }\n\n[[bin]]\nname = "security_authentication"\npath = "authentication.rs"\n\n[[bin]]\nname = "security_acl"\npath = "acl.rs"\n`);
  const cargoEnv = { ...process.env, CARGO_TARGET_DIR: path.join(worktree, 'target') };
  let result = run('cargo', ['generate-lockfile', '--offline'], { cwd: temp, env: cargoEnv });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  result = run('cargo', ['build', '--locked', '--offline', '--bins'], {
    cwd: temp, env: cargoEnv,
  });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);

  const acl = run(path.join(worktree, 'target/debug/security_acl'), [], { timeout: 60000 });
  assert.equal(acl.status, 0, `${acl.stdout}\n${acl.stderr}`);
  aclMarker = acl.stdout.trim();
  assert(aclMarker.includes('acl-ok columns=enforced role-revoke=immediate definer=bounded'));
  assert(aclMarker.includes('usage-create=separate delegated-grant=cascade'));
  assert(aclMarker.includes('trigger-execute=enforced context-values=verified'));
  assert(aclMarker.includes('job-execute=enforced'));

  const port = await freePort();
  const config = path.join(temp, 'server.toml');
  const rootPassword = 'docs-root-secret';
  const generatedVerifier = run(passwordBin, [], { input: `${rootPassword}\n`, timeout: 15000 });
  assert.equal(generatedVerifier.status, 0, generatedVerifier.stderr);
  const verifier = generatedVerifier.stdout.trim();
  assert.match(verifier, /^\$argon2id\$/);
  writeFileSync(config, `[server]\nbind_ip = "127.0.0.1"\nport = ${port}\ndata_dir = "${cargoPath(path.join(temp, 'server-data'))}"\nmax_connections = 8\n\n[server.authentication]\nroot_password_verifier = ${JSON.stringify(verifier)}\n`);
  child = spawn(serverBin, ['--config', config], { stdio: ['ignore', 'pipe', 'pipe'] });
  const output = { value: '' };
  child.stdout.on('data', chunk => { output.value += chunk; });
  child.stderr.on('data', chunk => { output.value += chunk; });
  await waitForOutput(child, output, 'radixdb-server listening on');

  const passwordlessRoot = run(
    path.join(worktree, 'target/debug/security_authentication'),
    [`127.0.0.1:${port}`, 'docs_security'],
    { timeout: 60000 },
  );
  assert.notEqual(passwordlessRoot.status, 0,
    'configured root unexpectedly accepted passwordless authentication');
  assert(`${passwordlessRoot.stdout}\n${passwordlessRoot.stderr}`.includes('authentication failed'));

  const authentication = run(
    path.join(worktree, 'target/debug/security_authentication'),
    [`127.0.0.1:${port}`, 'docs_security'],
    { timeout: 60000, env: { ...process.env, RADIXDB_ROOT_PASSWORD: rootPassword } },
  );
  assert.equal(authentication.status, 0,
    `${authentication.stdout}\n${authentication.stderr}\n${output.value}`);
  authenticationMarker = authentication.stdout.trim();
  assert(authenticationMarker.includes(
    'authentication-ok plaintext-password=accepted unknown=indistinguishable'));
  assert(authenticationMarker.includes(
    'disable=new-logins password-rotation=verified'));

  const exited = once(child, 'exit');
  assert(child.kill('SIGTERM'));
  const [code, signal] = await Promise.race([
    exited,
    new Promise((_, reject) => setTimeout(
      () => reject(new Error('server stop timeout')), 15000)),
  ]);
  assert.equal(signal, null);
  assert.equal(code, 0, output.value);
  assert(output.value.includes('radixdb-server stopped cleanly'), output.value);
  child = undefined;

  const testCounts = [
    cargoTest(['--lib', 'server::config::tests::plaintext_is_an_explicitly_supported_remote_password_transport']),
    cargoTest(['--lib', 'server::config::tests::configured_root_verifier_is_validated_and_redacted']),
    cargoTest(['--lib', 'server::session::tests::configured_root_password_is_required_and_works_on_remote_bindings']),
    cargoTest(['--lib', 'server::tcp_server::tests::configured_root_password_is_enforced_over_the_wire']),
    cargoTest(['--test', 'root_password_tool_test']),
    cargoTest(['--lib', 'server::tcp_server::tests::r12_batch_d_plaintext_password_login_has_durable_acl_lifecycle']),
    cargoTest(['--lib', 'server::tcp_server::tests::r12_batch_d_tls_policy_rejects_invalid_identity_and_plaintext_downgrade']),
    cargoTest(['-p', 'radixdb-executor', '--lib', 'authorization::tests::r12_batch_c_']),
    cargoTest(['-p', 'radixdb-executor', '--lib', 'procedural::tests::transaction_runtime::dynamic_sql_rechecks_object_privileges']),
    cargoTest(['-p', 'radixdb-executor', '--lib', 'procedural::tests::trigger_record_fields_bind_typed_static_sql_leaves']),
    cargoTest(['-p', 'radixdb-catalog', '--lib', 'payload::security::tests::']),
  ];

  cleanWorktree();
  console.log(JSON.stringify({
    identity: identity.stdout.trim(),
    authentication_marker: authenticationMarker,
    configured_root_password: 'accepted',
    configured_root_passwordless: 'rejected',
    acl_marker: aclMarker,
    engine_tests: testCounts.reduce((sum, count) => sum + count, 0),
    test_groups: testCounts,
    documentation_code_blocks: docs.reduce((sum, document) => sum + codeBlocks(readFileSync(
      path.join(root, 'src/content/docs/en', document), 'utf8')).length, 0),
    passed: true,
  }, null, 2));
} finally {
  if (child && child.exitCode === null && child.signalCode === null) {
    child.kill('SIGTERM');
    await Promise.race([
      once(child, 'exit'),
      new Promise(resolve => setTimeout(resolve, 15000)),
    ]);
  }
  rmSync(temp, { recursive: true, force: true });
}
