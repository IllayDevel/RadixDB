# RadixTrade maintenance examples

Maintenance examples are intentionally placed after the schema import and query
tour in the learning path.

Run the smoke file from the repository root after importing the demo database:

```bash
examples/public/radixtrade/scripts/01-import-schema.sh
examples/public/radixtrade/scripts/02-run-query-tour.sh
source examples/public/radixtrade/scripts/common.sh
run_radixdb_cli_checked -f examples/public/radixtrade/maintenance/01-checkpoint-vacuum.sql
```

Or run it directly with the same DSN used by the import script:

```bash
/opt/radixdb/bin/radixdb-cli \
  -d "file://$PWD/examples/public/radixtrade/runtime/radixtrade-demo?sync_mode=none&checkpoint_interval=3600" \
  -f examples/public/radixtrade/maintenance/01-checkpoint-vacuum.sql
```

The public maintenance chapters are the
[English backup and restore guide](../../../../doc/src/content/docs/en/administration/backup-restore.md)
and its [Russian counterpart](../../../../doc/src/content/docs/ru/administration/backup-restore.md).
