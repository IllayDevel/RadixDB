# CA-90.3 — отчёт о шестичасовой приёмке

[English](CA_90_3_6H_ACCEPTANCE_REPORT.md)

Статус: **ACCEPTED / PASS**.

Дата: 2026-09-07 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Автоматический terminal result: `passed`.

Итог: CA-90.3 принят как успешный усиленный acceptance run.
Наблюдавшиеся storage starvation, ATA reset и временные watchdog stall являются
частью evidence, а не причиной отмены PASS: движок каждый раз самостоятельно
восстанавливал progress и завершил полный checkpoint/snapshot/restore/digest
oracle без invariant failure.

## 1. Неизменяемая идентичность

| Параметр | Значение |
|---|---|
| Engine/soak Git SHA | `dd0bf75c9176bceb70ce8f1d2a07057610ec381b` |
| Version | `radixdb-server/radixdb-soak 0.5.2` |
| Protocol | `14` |
| Build profile / target | `release / x86_64-unknown-linux-gnu` |
| `Cargo.lock` SHA-256 | `dadf5eee3abdf18536a1a23c7a86553b658a0c450c5c5bd87a10a9c0c8af2f2d` |
| Config SHA-256 | `1edf165c21651f9888b6f5257f62bb28f0fc29e5da524e7ea61330898f91e025` |
| Server config SHA-256 | `2cca6d86627f452f905a370a0d9d6d4aa77f648849917c387c24b36df830aada` |
| Manifest SHA-256 | `4cc2d80343aac1b40a520d22b8ad753b8de068a8458bed6e9b9f66effbbc9862` |
| Final `REPORT.json` SHA-256 | `9ae80e5ab496caa8dfe9beff83d224a0b0ce19226777c7c4667a27fb2f6da715` |
| Final `status.json` SHA-256 | `3c5ded5539513c2b863efdebf15273e1f8522e7320ab1daa4333c7882e42216c` |

Более новый documentation/evidence commit не является identity исполнявшегося
binary и не подменяет SHA в таблице.

## 2. Зафиксированный профиль

| Параметр | Значение |
|---|---:|
| Mode / profile | `full / 6h` |
| Workload duration | `21 600 000 ms` |
| Seed | `1 592 594 944` |
| Active rows | `100 000 000` |
| Client ladder | `16, 32, 64, 128, 256` |
| Checkpoint interval | `5m` |
| Invariant interval | `30s` |
| Graceful reopen | `2h` |
| SIGKILL/reopen | `4h` |
| Recovery timeout | `20m` |
| Effective watchdog | `21m` |

Настройки chunk/job geometry, I/O budget, L0 thresholds и worker count под
фактический HDD не подстраивались.

## 3. Оборудование и накопитель

| Параметр | Значение |
|---|---|
| CPU | Intel Celeron 847, 1.10 GHz |
| Logical CPU | `2` |
| RAM | `1 843 860 KiB`, около `1.76 GiB` |
| Storage | Toshiba MQ01ABD050, 5400 rpm HDD |
| Filesystem | `/dev/sda1`, ext4 `rw,relatime,errors=remount-ro` |
| Degraded transport | `1.5 Gbit/s + UDMA/33` |
| Surface counters | reallocated/pending/offline-uncorrectable `0/0/0` |
| Transport history | SMART UDMA CRC `3`; live kernel ATA bus error |

Полный паспорт и граница интерпретации находятся в
[`CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.ru.md`](CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.ru.md).

## 4. Итоговый результат

Фактическое полное wall time, включая seed и final evidence gates:
`27 427 411 ms`, то есть `7h 37m 07.411s`.

| Counter | Результат |
|---|---:|
| Transactions planned | `128 652` |
| Transactions committed | `98 668` |
| Transactions rolled back | `14 259` |
| Conflicts | `8 604` |
| Operations | `2 351 035` |
| Ambiguous outcomes resolved | `160` |
| Graceful/SIGKILL reopens | `2` |
| Successful checkpoints | `2` |
| Deferred checkpoints | `51` |
| Final snapshots | `1` |
| Final restores | `1` |
| Invariant passes / failures | `2 100 / 0` |
| Telemetry drops | `0` |
| Terminal failure | отсутствует |

Все 12 invariant families прошли по 175 проверок. В частности:

- `cold_fixture_cardinality`: `expected=100000000 actual=100000000`;
- `account_balance`: `balances=6842419 postings=6842419`;
- `view_cardinality`: `base=31727 view=31727`;
- `dependent_view_cardinality`: `base=31727 dependent_view=31727`;
- `navigation_classic_parity`: `classic=31727 navigation=31727`;
- graph/revision/master-detail invariants: `violating_rows=0`;
- snapshot repeatable read: `start=31727 end=31727`.

После `final-checkpoint` успешно выполнены:

1. source `final-invariants`;
2. `final-snapshot` publication;
3. независимое копирование `final-snapshot-evidence`;
4. `final-restore` в отдельную database root;
5. `final-restore-invariants`;
6. source/restore `final-digest` comparison.

Итоговый logical digest:

```text
59b56e6b7bdaf846dd167aa01185222c7af95aec54593b9a05927b4c0abda4b0
```

Agent unit завершился штатно с `Result=success`, `ExecMainStatus=0`.

## 5. Реальные осложнения прогона

### 5.1 Первый эпизод побочной I/O-нагрузки

В 16:39:16–16:46:21 +07 на том же filesystem выполнялась прямая запись
случайных данных:

- продолжительность `7m 05.471s`;
- записано `13 883 146 240` bytes;
- episode пересёкся с запланированным SIGKILL/reopen;
- reopen завершился за `23 043 ms`;
- все 12 немедленных recovery invariants прошли.

### 5.2 Ограниченная 15-минутная нагрузка

В 17:06:05–17:21:05 +07 применён сохранённый
[`side-io-pressure.sh`](../../../../crates/radixdb-soak/deploy/atom-hdd/side-io-pressure.sh):

- 900 секунд direct random write;
- записано `29 657 923 584` bytes;
- предусмотрено ещё 300 секунд recovery observation;
- progress возобновился ещё под прямой конкурирующей нагрузкой.

### 5.3 Реальный аппаратный инцидент

В 17:26:31 kernel зарегистрировал:

```text
failed command: FLUSH CACHE EXT
Emask 0x10 (ATA bus error)
hard resetting link
SATA link up 1.5 Gbps
configured for UDMA/33
retrying FLUSH 0xea Emask 0x10
EH complete
```

Это был физический transport failure, не failpoint и не device-mapper
injection. Ядро восстановило link и повторило flush; permanent `EIO`, ext4
error и read-only remount не возникли.

Полный timeline и raw excerpts:
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md).

### 5.4 Две остановки watchdog

На `clients-256` accumulated maintenance tail дважды вывел semantic silence за
21-минутный watchdog corridor. Оба раза run оставался жив и самостоятельно
возвращался в `healthy`:

```text
85 770 -> 91 021 -> 95 361 -> 98 668 commits
```

После последнего выхода движок завершил worker phase, final checkpoint,
snapshot, restore, restored invariants и digest. Поэтому промежуточный
`watchdog=stalled` является доказанным prolonged-latency episode, но не
terminal liveness failure.

## 6. Ресурсы

| Метрика | Значение |
|---|---:|
| Peak server RSS по `samples.jsonl` | `1 060 020 224` bytes, около `1 010.9 MiB` |
| Final server RSS | `206 327 808` bytes, около `196.8 MiB` |
| Минимальный наблюдавшийся RSS | `148 299 776` bytes, около `141.4 MiB` |
| Final database tree | `3 670 799 691` bytes |
| DATA | `3 349 117 547` bytes |
| INDEX | `193 301 356` bytes |
| Metadata | `9 963 824` bytes |
| WAL | `118 415 802` bytes |
| Final threads / open FDs | `8 / 44` |

Final tree включает source и отдельную restore database, поэтому его нельзя
сравнивать напрямую с размером одной рабочей базы на dashboard.

После завершения 256-client workload RSS вернулся примерно к `180–200 MiB`.
Память активной ступени была освобождена при quiescence; terminal evidence не
показывает сохранения гигабайтного peak как постоянного хвоста.

Latency и throughput этого run не являются product SLA: они намеренно
загрязнены деградировавшим HDD, двумя side-load episodes и реальным ATA reset.

## 7. Вердикт наблюдателя

Observer догнал terminal state после завершения agent:

| Метрика | Значение |
|---|---:|
| Terminal | `true` |
| Samples | `14 176` |
| Telemetry sequence | `7 166` |
| Telemetry gaps | `0` |
| Retention dropped records | `0` |
| Retention saturated | `false` |
| Last error | отсутствует |

Подтверждённый `disk_degradation-13014` закономерно остался active с
`severity=fatal`: физический storage path после поднятия link не стал здоровым.
Соответствующий `incident-000104` остаётся открытым и не содержит
`engine-after.json`/`threads-after.json`, потому что persistent hardware alert
не получил clean recovery edge. Это ограничение явно сохранено в результате:
kernel log, before snapshot, burst telemetry и terminal engine evidence сохранены, а
состояние БД отдельно подтверждено полным restore/digest oracle.

## 8. Расположение доказательств

Полный raw bundle хранится вне публичного Git-репозитория. Его локальный путь
на стенде намеренно исключён из опубликованной копии. Bundle содержит:

- `manifest.json`;
- `status.json`;
- `REPORT.json` и `REPORT.md`;
- `events.jsonl`, `samples.jsonl`, `semantic-progress.jsonl`;
- observer/engine/host/disk/process/kernel/SMART telemetry;
- `incidents/000104-disk-degradation/`;
- `backups/final-snapshot/`.

Выбранный incident log включён в публичный набор evidence:
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log).

## 9. Граница приёмки

CA-90.3 принимается как PASS для tested binary
`dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

Принятие означает:

- correctness/recovery на 100M и лестнице до 256 клиентов подтверждены;
- graceful и process-kill reopen подтверждены;
- реальный transient ATA reset не привёл к corruption;
- prolonged I/O starvation не превратился в необратимый livelock;
- final snapshot/restore/digest oracle зелёный.

Принятие не означает:

- SLA на данном HDD;
- гарантию при permanent потере устройства, false flush либо power loss;
- признание деградировавшего storage пригодным для production;
- обязательность повторения 24h/72h на этом стенде.
