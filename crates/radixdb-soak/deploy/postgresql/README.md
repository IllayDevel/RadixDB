# PostgreSQL 6h comparison profile

This profile runs the same logical soak workload as RadixDB: the same seed,
100M cold rows, schema, constraints, views, client ladder, transaction mix,
invariants, graceful recovery and `SIGKILL` recovery. PostgreSQL-specific
statistics remain separate in the raw diagnostic artifacts.

The tuning targets the dedicated two-core, 2-GiB, rotating-HDD stand. It keeps
`fsync`, `full_page_writes`, `synchronous_commit`, WAL and autovacuum enabled.
There are no unlogged tables or a PostgreSQL-only connection pooler.

Before installation, PostgreSQL server binaries must exist in `/usr/bin` and a
random cluster password must be installed outside Git:

```sh
openssl rand -base64 36 | tr -d '\n' \
  | sudo tee /etc/radixdb-soak/postgresql-password >/dev/null
sudo chown root:radixdb-soak /etc/radixdb-soak/postgresql-password
sudo chmod 0640 /etc/radixdb-soak/postgresql-password
```

Runs are sequential. Do not replace the unit or current release while the
RadixDB soak is active. Every fresh pair is ordered `PostgreSQL -> RadixDB`.
A failed PostgreSQL run invalidates the pair and the RadixDB half is not
started. Record the fixed order, same cache protocol, SMART state and ambient
workload; do not reuse the current standalone RadixDB run as half of a pair.
Start the PostgreSQL half with a pair identifier; the run manifest records it
through the mandatory `<pair-id>-postgresql` run ID:

```sh
sudo /storage/radixdb-soak/current/bin/prepare-run.sh pair-01
```

If it passes, start the RadixDB half using exactly `pair-01-radixdb`. The
comparison command rejects mismatched identities and a RadixDB manifest whose
start time precedes the PostgreSQL manifest.

After both completed runs, write reports outside their immutable evidence
directories:

```sh
radixdb-soak summarize --run-dir /storage/radixdb-soak/runs/pair-01-radixdb \
  --json /storage/radixdb-soak/reports/pair-01-radixdb.json \
  --markdown /storage/radixdb-soak/reports/pair-01-radixdb.md

radixdb-soak summarize --run-dir /storage/radixdb-soak/runs/pair-01-postgresql \
  --json /storage/radixdb-soak/reports/pair-01-postgresql.json \
  --markdown /storage/radixdb-soak/reports/pair-01-postgresql.md

radixdb-soak compare \
  --pair-id pair-01 \
  --radixdb-run /storage/radixdb-soak/runs/pair-01-radixdb \
  --postgresql-run /storage/radixdb-soak/runs/pair-01-postgresql \
  --json /storage/radixdb-soak/reports/pair-01.json \
  --markdown /storage/radixdb-soak/reports/pair-01.md
```
