import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const worktree = process.env.RADIXDB_DOCS_WORKTREE;
const revision = process.env.RADIXDB_DOCS_REVISION
  ?? '40b1b3d13e050afa2666a0414b7215d5ac1452c0';
const release = '804027a4ee8426b6f7bd083c6e26a1895603dc38';
assert(worktree && path.isAbsolute(worktree), 'Set RADIXDB_DOCS_WORKTREE to the pinned worktree');

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: 'utf8', timeout: 30000, maxBuffer: 32 * 1024 * 1024, ...options,
  });
  assert(!result.error, String(result.error));
  assert.equal(result.signal, null, `${command} was terminated by ${result.signal}`);
  return result;
}

function git(args) {
  const result = run('git', args, { cwd: worktree });
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

assert.equal(git(['rev-parse', 'HEAD']).trim(), revision);
assert.equal(git(['status', '--short']), '');
assert.equal(git(['cat-file', '-t', 'v1.1.0']).trim(), 'tag');
assert.equal(git(['rev-parse', 'v1.1.0^{commit}']).trim(), release);

function content(locale, page) {
  return readFileSync(path.join(root, 'src/content/docs', locale, 'appendices', `${page}.md`), 'utf8');
}

function ids(locale, page) {
  return content(locale, page).split('\n')
    .filter(line => /^\| (?:BENCH|CMP|PERF|SOAK|REL)-\d+ \|/.test(line))
    .map(line => line.split('|')[1].trim());
}

const benchmarkIds = ids('en', 'benchmarks');
const releaseIds = ids('en', 'release-notes');
assert.deepEqual(benchmarkIds, ids('ru', 'benchmarks'));
assert.deepEqual(releaseIds, ids('ru', 'release-notes'));
assert.equal(benchmarkIds.length, 27);
assert.equal(releaseIds.length, 0);
assert.equal(new Set(benchmarkIds).size, 27);

for (const locale of ['en', 'ru']) {
  const benchmark = content(locale, 'benchmarks');
  const notes = content(locale, 'release-notes');
  for (const marker of [
    'b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc',
    'dd0bf75c9176bceb70ce8f1d2a07057610ec381b',
    '100000000:49734600639880',
    '499488e7eeca18a91a2ebb763473308e15d4e929a7c0770d933c9fd9bba084d4',
    locale === 'ru' ? 'CA_80_7_100M_COMPARATIVE_REPORT.ru.md' : 'CA_80_7_100M_COMPARATIVE_REPORT.md',
    locale === 'ru' ? 'CA_90_3_6H_ACCEPTANCE_REPORT.ru.md' : 'CA_90_3_6H_ACCEPTANCE_REPORT.md',
    '23bf35df011aae6816d77578be96074b02bc363c',
    locale === 'ru' ? '11 870,421' : '11,870.421',
  ]) assert(benchmark.includes(marker), `${locale} benchmark is missing ${marker}`);
  for (const marker of [
    '1.2',
    'protocol 17',
    'Argon2id',
    'CONNECT',
    'TLS',
    '1.1.0',
    'protocol 14',
    '1.0.0',
  ]) assert(notes.includes(marker), `${locale} release notes are missing ${marker}`);
}

function read(relative) {
  return readFileSync(path.join(worktree, relative), 'utf8');
}

const reportContracts = [
  ['doc/public/evidence/performance/CA_80_7_100M_COMPARATIVE_REPORT.md', [
    'candidate source: `b648b2d3ea323cf5eb10417ab09d5eb3d5d01ecc`',
    'AMD Ryzen 9 7950X, Apacer AS2280Q4U 2 TB NVMe, Btrfs',
    'PostgreSQL `18.3`, on the same host and dataset',
    'one warm-up\nwas excluded and the median of five',
    '| `scan.full` | `100.299`',
    '| logical database | `1,964,220,021 B`',
  ]],
  ['doc/public/evidence/reliability/CA_90_3_6H_ACCEPTANCE_REPORT.md', [
    '| Engine/soak Git SHA | `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |',
    '| Workload duration | `21 600 000 ms` |',
    '| Active rows | `100 000 000` |',
    '| Client ladder | `16, 32, 64, 128, 256` |',
    '| Storage | Toshiba MQ01ABD050, 5400 rpm HDD |',
    '| Operations | `2 351 035` |',
    '| Peak server RSS from `samples.jsonl` | `1 060 020 224` bytes',
    'Latency and throughput in this run are not a product SLA',
  ]],
  ['doc/public/evidence/performance/RADIXDB_1_2_100M_VALIDATION.md', [
    '`23bf35df011aae6816d77578be96074b02bc363c`',
    '`100000000:49734600639880`',
    '11,870.421 ms',
    '565.97 MiB',
    '1.83 GiB',
  ]],
];

let reportMarkers = 0;
for (const [relative, markers] of reportContracts) {
  const report = read(relative);
  for (const marker of markers) {
    assert(report.includes(marker), `${relative} is missing ${marker}`);
    reportMarkers += 1;
  }
}

const changelog = git(['show', `${release}:CHANGELOG.md`]);
const manifest = git(['show', `${release}:Cargo.toml`]);
const publishedBenchmark = content('en', 'benchmarks');

const releaseMarkers = [
  [publishedBenchmark, '`12ef5963` performance code tip'],
  [publishedBenchmark, '| PERF-01 | Aggregate | 10.150 ms | 9.907 ms | +2.45% |'],
  [publishedBenchmark, '| PERF-04 | Full scan | 105.224 ms | 100.299 ms | +4.91% |'],
  [publishedBenchmark, '563,662,848, 566,497,280 and 551,399,424 bytes'],
  [changelog, '## 1.1.0 - 2026-09-08'],
  [changelog, '## 1.0.0 - 2026-09-07'],
  [changelog, 'Added durable catalog 6.1 objects'],
  [manifest, 'version = "1.1.0"'],
];
for (const [source, marker] of releaseMarkers) assert(source.includes(marker), `release evidence is missing ${marker}`);

assert.equal(git(['status', '--short']), '');

console.log(JSON.stringify({
  revision,
  release_tag: 'v1.1.0',
  release_commit: release,
  benchmark_rows: benchmarkIds.length,
  release_rows: releaseIds.length,
  report_markers: reportMarkers,
  release_markers: releaseMarkers.length,
  heavy_benchmarks_rerun: false,
  passed: true,
}, null, 2));
