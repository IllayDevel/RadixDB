---
title: Справочник конфигурации
description: Указатель server TOML, файлового DSN и параметров командной строки.
---

RadixDB 1.2 имеет три отдельных интерфейса конфигурации. Они не являются
псевдонимами друг друга.

| Интерфейс | Владелец | Применение |
| --- | --- | --- |
| `server.toml` | `radixdb-server` | Читается один раз при запуске процесса; server settings и optional allowlist native packages |
| `file://...?key=value` | Встроенный API и CLI | Читается при открытии файловой базы; принимается 39 ключей |
| Параметры CLI | `radixdb-cli` | Формируют часть переопределений файлового DSN для запуска |

Глава [«Конфигурация сервера»](../../administration/configuration/) является
полным справочником TOML settings, plugin allowlist, значений по умолчанию,
релизных значений, диапазонов и правил перезапуска.
[Справочник `radixdb-cli`](../programs/cli/) перечисляет все флаги CLI. Эта
страница определяет полный набор параметров файлового DSN.

## Синтаксис file DSN

```text
file:///absolute/path?sync_mode=full&checkpoint_interval=60
```

Keys являются case-sensitive ASCII names. Values не проходят percent-decoding.
Каждый key встречается один раз; duplicate и unknown keys являются ошибками.
Bare key имеет пустое value и обычно отклоняется value parser. `memory://` не
принимает file persistence options.

## Durability и WAL

| Key | Default | Единица или значения | Примечание |
| --- | ---: | --- | --- |
| `sync_mode` | `normal` | `none`, `normal`, `full`; numeric aliases `0..2` | Durability policy |
| `checkpoint_interval` | `60` | seconds, `u32`; `0` допустим | Periodic checkpoint interval |
| `checkpoint_on_close` | `on` | `on` или `off` | Отключать только в controlled crash tests |
| `wal_flush_trigger` | `32768` | bytes, `usize` | Buffered WAL flush threshold |
| `wal_buffer_size` | `65536` | bytes, `usize` | Initial WAL buffer |
| `wal_max_size` | `67108864` | bytes, `usize` | Rotation threshold |
| `sync_interval_ms` | `1000` | milliseconds, `u32` | Normal-mode interval field |
| `wal_compression` | `on` | `on` или `off` | WAL LZ4 |
| `volume_compression` | `on` | `on` или `off` | Data-artifact LZ4 |
| `compression` | `on` | `on` или `off` | В этой позиции задаёт оба compression switches |
| `keep_snapshots` | `3` | count, `u32` | Physical database snapshot retention |

`compression`, `wal_compression` и `volume_compression` применяются в query
order. Не объединяйте umbrella и individual keys в одном DSN.

## Seal, compaction и backpressure

| Key | Default | Единица или значения | Примечание |
| --- | ---: | --- | --- |
| `compact_threshold` | `4` | segment count, `u32` | Size-tier trigger input |
| `max_compaction_jobs` | `1` | `1..=8` jobs | Только distinct tables |
| `storage_cpu_workers` | `0` | workers, `usize`; `0` auto | Shared CPU-heavy pool |
| `max_compaction_input_segments` | `8` | count, minimum 1 | Per-job work bound |
| `max_compaction_input_bytes` | `536870912` | bytes, minimum 1 | Per-job input bound |
| `max_compaction_output_bytes` | `1073741824` | bytes, minimum 1 | Per-job output bound |
| `compaction_job_time_budget_ms` | `1200000` | milliseconds; `0` отключает | Wall-clock budget |
| `compaction_io_bytes_per_sec` | `0` | bytes/second; `0` unlimited | Aggregate average limit |
| `compaction_disk_reserve_bytes` | `1073741824` | bytes; `0` отключает | Free-space reserve |
| `compaction_retry_cooldown_ms` | `30000` | milliseconds; `0` отключает | Exact-input retry suppression |
| `l0_soft_limit_segments` | `16` | segments, positive | Меньше hard limit |
| `l0_hard_limit_segments` | `32` | segments, positive | Отклоняет commit до publication |
| `l0_soft_limit_bytes` | `1073741824` | bytes, positive | Меньше hard limit |
| `l0_hard_limit_bytes` | `2147483648` | bytes, positive | Отклоняет commit до publication |
| `l0_soft_backpressure_wait_ms` | `100` | milliseconds | Maximum cooperative wait per commit |
| `target_volume_rows` | `1048576` | rows, minimum 65536 | Output segment target |
| `seal_hot_bytes_threshold` | `67108864` | bytes, minimum 1 | First seal threshold |
| `seal_incremental_hot_bytes_threshold` | `16777216` | bytes, minimum 1 | Later seal threshold |

L0 pairs совместно проверяются как `0 < soft < hard`. Допустимое parser value
остаётся workload decision; сохраняйте defaults до измерения.

## Cache и read path

| Key | Default | Единица или значения | Примечание |
| --- | ---: | --- | --- |
| `page_cache_level` | `0` | `0..=10` | Proactive OS page-cache warmup |
| `page_cache_max_bytes` | `0` | bytes; `0` automatic | Warmup hard cap |
| `page_cache_memory_reserve` | `0` | bytes; `0` automatic | Memory left outside warmup |
| `volume_cache_bytes` | `1073741824` | bytes | Resident cold payload budget |
| `read_queue_depth` | `1` | requests, minimum 1 | Sequential artifact reads |
| `copy_max_transaction_bytes` | `536870912` | bytes, positive | One atomic COPY memory envelope |

`page_cache_*` управляет operating-system cache. Engine-owned
`volume_cache_bytes` является отдельным bounded budget и не образует полный
RSS limit. См.
[«Управление памятью»](../../administration/memory/).

## MVCC cleanup

| Key | Default | Единица или значения | Примечание |
| --- | ---: | --- | --- |
| `cleanup` | `on` | `on` или `off` | Background MVCC cleanup |
| `cleanup_interval` | `60` | seconds, `u64` | Cleanup cycle interval |
| `deleted_row_retention` | `300` | seconds, `u64` | Deleted-row retention |
| `transaction_retention` | `3600` | seconds, `u64` | Completed transaction retention |

`commit_batch_size`, `compression_threshold`, `scan_prefetch_cache_bytes` и
`block_cache_bytes` являются удалёнными и отклоняются. Они не входят в контракт
из 39 ключей. Для долговечности используйте `sync_mode`; движок не рекламирует
нереализованный threshold или cache budget.

Configuration привязывается при первом open database. Connections с одним
canonical file path разделяют engine owner и не должны предполагать, что
следующий DSN молча его переконфигурирует. Закройте всех owners перед сменой
persistent runtime settings и не редактируйте storage artifacts напрямую.
