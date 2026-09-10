# CA-90.3 — восстановление после реального сбоя ATA flush

[English](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.md)

Статус: зафиксированный инцидент реального оборудования с самостоятельным
восстановлением; полный 6h soak завершён и принят как PASS.

Дата события: 2026-09-07 17:06–17:34 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Tested binary: `dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

Полные выбранные выдержки без пересказа сохранены в
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log).

## 1. Что произошло

Во время живого CA-90.3 стенд получил 15 минут прямой конкурирующей записи
случайных данных на тот же ext4 filesystem. Нагрузка была намеренной, но отказ
storage path не эмулировался failpoint, device mapper или программной ошибкой:
ядро зарегистрировало настоящий ATA bus error при `FLUSH CACHE EXT`, изменение
состояния соединения и hard reset физического SATA link.

После reset ядро повторно подняло link на уже деградировавших
`1.5 Gbit/s + UDMA/33`, повторило `FLUSH` и завершило error handling. RadixDB не
завершился, filesystem не перешёл в read-only, watchdog не истёк, invariant
oracle не обнаружил расхождений, а workload самостоятельно возобновил progress
и перешёл на следующую ступень клиентов.

Это не синтетическая подстановка `EIO`. Это наблюдение реального transient
отвала накопителя/интерфейса в момент durability operation на работающей базе.

## 2. Точная шкала времени

| Время +07 | Событие |
|---|---|
| 17:06:05 | Старт bounded side-I/O pressure; CA-90.3 работает на `clients-128`, `79 491` commits, `0` invariant failures |
| 17:15:09 | После 9 минут конкурирующей записи semantic progress молчит `547 470 ms`, watchdog остаётся healthy |
| 17:19:10 | Ещё под нагрузкой движок самостоятельно возобновляет progress: `80 434` commits, то есть `+943` к началу окна |
| 17:21:05 | Direct writer завершён таймером через 900 секунд; создано `29 657 923 584` bytes случайных данных |
| 17:21:10 | Контрольный снимок после остановки writer: run жив, watchdog healthy, invariants `0` |
| 17:26:10 | Завершено предусмотренное 5-минутное recovery-наблюдение; side-load unit завершён успешно |
| 17:26:31.173 | `FLUSH CACHE EXT` получает ATA bus error, link заморожен, начинается hard reset |
| 17:26:31.905 | SATA link снова поднят на `1.5 Gbps` |
| 17:26:31.913 | Устройство повторно настроено как `UDMA/33` |
| 17:26:31.914 | Ядро повторяет `FLUSH`; ATA error handling завершается |
| 17:34:52 | Run уже на `clients-256`: `82 024` commits, TPS `2.707`, watchdog healthy, `0` invariant failures, все три soak unit active |

За интервал от начала pressure до снимка 17:34:52 число commits выросло на
`2 533`. Возобновление progress произошло как во время прямой конкурирующей
записи, так и после физического reset. Это исключает объяснение «процесс ещё
жив, но workload необратимо застрял» для наблюдаемого окна.

## 3. Доказательства ядра

Ключевой фрагмент kernel journal:

```text
2026-09-07T17:26:31.173789+07:00 debian kernel: ata3.00: exception Emask 0x10 SAct 0x0 SErr 0x4050000 action 0xe frozen
2026-09-07T17:26:31.174033+07:00 debian kernel: ata3.00: irq_stat 0x00000040, connection status changed
2026-09-07T17:26:31.174111+07:00 debian kernel: ata3: SError: { PHYRdyChg CommWake DevExch }
2026-09-07T17:26:31.174187+07:00 debian kernel: ata3.00: failed command: FLUSH CACHE EXT
2026-09-07T17:26:31.174281+07:00 debian kernel: ata3.00: cmd ea/00:00:00:00:00/00:00:00:00:00/a0 tag 29
2026-09-07T17:26:31.174350+07:00 debian kernel: ata3.00: status: { DRDY }
2026-09-07T17:26:31.174414+07:00 debian kernel: ata3: hard resetting link
2026-09-07T17:26:31.905837+07:00 debian kernel: ata3: SATA link up 1.5 Gbps (SStatus 113 SControl 310)
2026-09-07T17:26:31.913837+07:00 debian kernel: ata3.00: configured for UDMA/33
2026-09-07T17:26:31.914022+07:00 debian kernel: ata3.00: retrying FLUSH 0xea Emask 0x10
2026-09-07T17:26:31.914103+07:00 debian kernel: ata3: EH complete
```

В том же окне kernel journal не содержит `Buffer I/O error`, ext4 error,
critical medium error или read-only remount. После события root filesystem
`/dev/sda1` оставался смонтирован как
`ext4 rw,relatime,errors=remount-ro`.

## 4. Реакция RadixDB

Во время голодания движок не выдавал ложный успех maintenance. Он оставлял
незавершённую работу на bounded retry:

```text
2026-09-07T17:20:56.944842+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:20:59.129455+07:00 Warning: immutable-member retirement requires retry: artifact cleanup wall-time nanoseconds is 2061232602; configured limit is 2000000000
2026-09-07T17:25:23.631488+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:28:21.750294+07:00 Warning: checkpoint cycle failed: checkpoint timed out acquiring the commit fence
2026-09-07T17:28:23.852990+07:00 Warning: immutable-member retirement requires retry: artifact cleanup wall-time nanoseconds is 2000795172; configured limit is 2000000000
```

Снимок после физического reset:

| Метрика | Значение |
|---|---:|
| State / phase | `running / clients-256` |
| Clients | `256/256` |
| Current TPS | `2.707` |
| Transactions planned | `103 889` |
| Transactions committed | `82 024` |
| Transactions rolled back | `11 910` |
| Conflicts | `3 978` |
| Operations | `2 091 985` |
| Invariant passes / failures | `2 040 / 0` |
| Reopens | `2` |
| Watchdog | `healthy`, silence `131 271 / 1 260 000 ms` |
| Terminal failure | отсутствует |

Все три unit — agent, database server и observer — оставались active.

## 5. Доказательства наблюдателя

Observer сам обнаружил отказ, а не получил его из ручной интерпретации:

```json
{
  "id": "incident-000104",
  "alert_ids": ["disk-degradation-13014"],
  "candidate_cause": "disk_degradation",
  "confidence": "confirmed",
  "artifact_directory": "incidents/000104-disk-degradation",
  "closed_unix_millis": null
}
```

Alert имеет `severity=fatal` и остаётся active как подтверждённый дефект
hardware path. Это severity диагностического storage-события, а не terminal
failure процесса БД: `observer-status.json` одновременно сообщает
`terminal=false`, `telemetry_gaps=0` и отсутствие failure.

Полный incident bundle хранится вне публичного Git-репозитория. Выбранные
timestamped excerpts включены в опубликованный evidence log.

На момент первого снимка incident ещё не был закрыт и честно перечислял
отсутствующие post-trigger файлы `engine-after.json` и `threads-after.json`.
Это не мешает kernel evidence, но запрещает называть incident bundle полностью
завершённым до terminal сборки evidence.

## 6. SMART после события

Сразу после incident SMART по-прежнему показывал:

| Counter | RAW |
|---|---:|
| `Reallocated_Sector_Ct` | `0` |
| `Current_Pending_Sector` | `0` |
| `Offline_Uncorrectable` | `0` |
| `UDMA_CRC_Error_Count` | `3` |
| ATA Error Count | `3` |
| Power-on hours | `3 364` |
| Temperature | `43°C` |

Новый kernel ATA reset не увеличил SMART error log. Поэтому SMART остаётся
полезным, но недостаточным oracle: живой transport failure был виден ядру и
observer, хотя накопитель не записал новый SMART error.

## 7. Граница доказанного

Инцидент доказывает следующее:

- RadixDB продолжил работу при реальном кратковременном ATA transport failure
  на границе `FLUSH CACHE EXT`;
- kernel восстановил link и успешно выполнил retry без видимого приложению
  permanent `EIO`;
- bounded retry не превратился в ранее найденный бесконечный generation loop;
- после recovery продолжились commits, выросла ступень клиентов, сохранились
  `0` invariant failures и healthy watchdog;
- observer распознал деградацию и сохранил incident evidence.

Инцидент сам по себе не доказывает:

- сохранность при permanent потере устройства либо невосстановимом `EIO`;
- корректность устройства, которое ложно подтверждает flush;
- power-loss durability после физического отключения питания;
- успешный terminal verdict полного CA-90.3 до final quiescence/reopen;
- какую-либо нормативную производительность для HDD или NVMe.

Это редкое production-realistic событие: конкурентная I/O-нагрузка была
создана намеренно, но конкретный ATA bus error, hard reset и retry произошли на
настоящем деградирующем железе и не могли быть «подкручены» тестовым oracle.

## 8. Последующие остановки и восстановление прогресса

После восстановления от ATA reset движок перешёл с `clients-128` на
`clients-256` и продолжил работу до `85 770` commits. Последний зафиксированный
semantic progress произошёл в 17:42:29.628 +07. Затем накопленный хвост
maintenance на деградировавшем диске временно перестал укладываться в watchdog
corridor:

| Срез | Значение |
|---|---:|
| Время контрольного чтения | `18:07:39 +07` |
| Phase | `clients-256` |
| Commits | `85 770` |
| Operations | `2 150 604` |
| Silence | `1 509 571 ms`, около `25:09` |
| Watchdog limit | `1 260 000 ms`, `21:00` |
| Watchdog state | `stalled` |
| Invariant failures | `0` |
| Process / units | живы / active |
| Terminal failure | отсутствует |

Этот срез не оказался terminal liveness failure. Движок продолжил бороться за
I/O и дважды самостоятельно вернулся из `stalled` в `healthy`:

| Время +07 | Результат восстановления |
|---|---|
| 18:17:15 | `91 021` commits, TPS `5.042`, watchdog снова `healthy` |
| 18:29:56 | Повторный `stalled`: `91 021` commits, silence `1 286 635 ms` |
| 18:30:57 | `95 361` commits, то есть ещё `+4 340`; TPS `4.888`, silence `34 624 ms`, watchdog `healthy` |

Поэтому честная доказанная граница наблюдения такова:

- `clients-128` пережили 15-минутное прямое I/O starvation, настоящий ATA bus
  error на flush, hard reset link и последующее восстановление progress;
- `clients-256` были достигнуты, выполняли транзакции и дважды возобновляли
  progress после выхода за 21-минутный watchdog corridor;
- состояния `stalled` были реальными и должны оставаться в evidence, но не
  стали terminal failure или необратимой потерей progress;
- corruption не обнаружена: последние invariants зелёные, filesystem остаётся
  read-write, новых kernel I/O ошибок после reset нет;
- наиболее вероятный ограничитель — неспособность уже деградировавшего HDD/SATA
  path своевременно разобрать накопленный объём physical maintenance work. Пока
  run не завершён terminal evidence, это обоснованная атрибуция, а не
  доказанный единственный root cause.

Намеренно созданная боковая нагрузка и произошедший аппаратный reset означают,
что этот run является усиленным, а не обычным чистым 6h acceptance опытом.
Terminal checkpoint, source/restored invariants, snapshot/restore и logical
digest впоследствии прошли. Run принят как PASS; промежуточный
`stalled` не трактуется как провал `clients-256`, поскольку последующий progress
и полный terminal oracle наблюдались напрямую.

## 9. Итоговый результат

Run завершился `state=passed`:

- `98 668` commits и `2 351 035` operations;
- `2 100/0` invariant passes/failures;
- `2` успешных checkpoint, `1` snapshot и `1` restore;
- source и restored logical digest совпали;
- agent завершился с `Result=success`;
- observer догнал `terminal=true` без telemetry gaps.

Итоговый acceptance report:
[`CA_90_3_6H_ACCEPTANCE_REPORT.ru.md`](CA_90_3_6H_ACCEPTANCE_REPORT.ru.md).

## 10. Воспроизводимость нагрузки

Применённый bounded driver хранится в
[`side-io-pressure.sh`](../../../../crates/radixdb-soak/deploy/atom-hdd/side-io-pressure.sh).
Он задаёт 900 секунд direct random write и 300 секунд recovery-наблюдения,
проверяет watchdog budget, свободное место, state run и invariant failures.

Сам физический ATA failure воспроизводимым не считается: скрипт воспроизводит
только условия I/O starvation, но не обещает повторить отказ железа.
