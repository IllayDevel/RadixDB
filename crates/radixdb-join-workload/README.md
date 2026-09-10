# RadixDB heavy JOIN workload

This internal workspace crate freezes the Mozaic Messenger Q1-Q6 heavy-JOIN
corpus plus the C1 non-JOIN control before the JOIN/reference executor rebuild.

The fixture records the source commit, source document checksum, schema
generation and a checksum for every copied SQL file. It contains no production
data, application code or credentials. `WorkloadScale::smoke()` is used by the
fast correctness gate; `WorkloadScale::consumer_profile()` preserves the agreed
10k-user/1M-message benchmark cardinalities.

`for_each_seed_statement()` is deliberately streaming. A PostgreSQL adapter can
consume exactly the same deterministic INSERT stream without allocating the
entire dataset, while `seed_database()` applies it through the embedded RadixDB
API. `execute_case()` produces a typed, length-delimited result checksum used by
repeat and differential gates.

Q1-Q3 return the complete recipient set. They are never made finite by adding a
benchmark-only `LIMIT`; named values are bound as typed parameters, generated
SQL literals are fully escaped, and execution is bounded only by the external
query timeout/cancellation path.

Run the fast admission gate with:

```bash
cargo test -p radixdb-join-workload
```

Capture a fresh-file cold/warm baseline with sequential and four-client phases:

```bash
cargo run --release -p radixdb-join-workload \
  --bin radixdb-join-baseline -- \
  --output-dir /path/to/artifacts \
  --profile consumer \
  --warmup-runs 2 \
  --measured-runs 10 \
  --clients 4 \
  --case-timeout-secs 120
```

The runner never replaces an existing run directory. Before every cold case it
closes the previous embedded database owner and applies `POSIX_FADV_DONTNEED`
only to regular files below its newly-created database directory. The timed
windows retain latency percentiles, process CPU, result rows/bytes/checksum and
the complete engine counter snapshot. `EXPLAIN` runs outside timed windows.
`results.json`, the compact `report.md`, and a checksum-bearing `COMPLETE`
marker form one baseline artifact. `progress.json` is atomically refreshed
before and after every case. A pathological plan is cancelled after the
configured per-query deadline, so an interrupted run still identifies the
active case and preserves every completed measurement.

Run the exact PostgreSQL 18 differential gate only against the registered
dedicated benchmark cluster (never the system service on port 5432):

```bash
RADIXDB_JOIN_PG_DSN='host=/path/to/registered/socket port=55432 user=postgres dbname=postgres' \
  cargo test -p radixdb-join-workload --features postgres-oracle \
  --test postgres_oracle -- --ignored --exact \
  q1_q6_and_control_match_postgresql_rows_order_types_and_nulls
```

The gate creates its schema inside one PostgreSQL transaction and rolls it
back. It uses the same streaming seed statements, parameter values and
length-delimited typed checksum as RadixDB; no benchmark-only LIMIT is added.

Capture a PostgreSQL 18 warm p95 performance reference with the same complete
consumer corpus:

```bash
RADIXDB_JOIN_PG_DSN='host=/path/to/registered/socket port=55432 user=postgres dbname=postgres' \
  cargo run --release -p radixdb-join-workload \
  --features postgres-oracle --bin postgres-join-baseline -- \
  --output-dir /path/to/artifacts \
  --profile consumer --warmup-runs 2 --measured-runs 10
```

The runner creates one isolated schema, streams the same synthetic seed,
executes `ANALYZE`, records p50/p95/p99 and exact result checksums, then drops
the schema even when a case fails. The DSN is read only from the environment
and is not written to artifacts.
