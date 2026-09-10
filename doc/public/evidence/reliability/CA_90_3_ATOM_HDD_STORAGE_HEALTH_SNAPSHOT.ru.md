# CA-90.3 — снимок состояния накопителя Atom/HDD

[English](CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.md)

Статус: доказательство с реального оборудования; связанное шестичасовое
испытание завершено и принято как PASS.

Обновление после исходного среза: в 17:26:31 +07 этот же live run пережил
реальный ATA bus error на `FLUSH CACHE EXT`, hard reset и kernel retry. Полный
протокол с логами:
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md).

Дата среза: 2026-09-07 15:21–15:30 +07.

Run: `ca90-r4-dd0bf75-6h-20260907-113203`.

Tested binary: `dd0bf75c9176bceb70ce8f1d2a07057610ec381b`.

## 1. Назначение

Документ фиксирует фактическое состояние storage path, на котором выполняется
CA-90.3. Это позволяет отделить свойства движка от ограничения и деградации
стенда. Срез получен без остановки и перенастройки soak.

Производительность этого стенда не является NVMe baseline или product SLA.
Если run завершится успешно, correctness/recovery evidence допустимо трактовать
как испытание на существенно деградировавшем накопителе и интерфейсе.

## 2. Накопитель и файловая система

| Параметр | Значение |
|---|---:|
| Модель | Toshiba MQ01ABD050, 2.5-inch HDD |
| Год выпуска | 2013, по маркировке/паспорту стенда; SMART год не сообщает |
| Скорость вращения | 5400 rpm |
| Logical / physical sector | 512 / 4096 bytes |
| ATA/SATA | ATA8-ACS, SATA 2.6 |
| Заявленная SATA link speed | 3.0 Gbit/s |
| Link speed в момент среза | 1.5 Gbit/s |
| Negotiated transfer mode после деградации | UDMA/33 |
| Доступная ёмкость | 320,072,933,888 bytes, около 298.1 GiB |
| Native ёмкость | 976,773,168 sectors, около 500 GB |
| HPA | enabled: `625142449/976773168` sectors |
| Размещение `/storage` | `/dev/sda1`, общий root filesystem |
| Filesystem | ext4, `rw,relatime,errors=remount-ro` |

Таким образом, модель nominally относится к 500 GB классу, но BIOS/host HPA
ограничивает видимую ёмкость примерно до 320 GB. Это не результат разметки
раздела и не параметр RadixDB.

## 3. Счётчики SMART за срок службы

| Метрика | RAW | Оценка |
|---|---:|---|
| Overall health | `PASSED` | vendor threshold не пересечён |
| Power-on hours | `3 362` | 140 дней 2 часа |
| Power cycles | `642` | информационно |
| Start/stop count | `647` | информационно |
| Load cycle count | `51 906` | заметная механическая история, threshold не пересечён |
| Loaded hours | `913` | время с загруженными головками |
| Power-off retract count | `35` | аварийные/нештатные парковки |
| G-sense events | `2` | зарегистрированные удары/перегрузки |
| Reallocated sectors | `0` | поверхность без remap evidence |
| Reallocation events | `0` | поверхность без remap evidence |
| Current pending sectors | `0` | ожидающих переназначения нет |
| Offline uncorrectable | `0` | неисправимых offline sectors не зарегистрировано |
| Reported uncorrectable errors | `0` | media read/write failure не зарегистрирован |
| Spin retry / load retry | `0 / 0` | механический запуск не повторялся |
| UDMA CRC errors | `3` | transport/interface fault history |
| SMART device error log | `3` | три `ICRC, ABRT` на 3 135-м часу |
| Hardware resets | `6 746` | высокая lifetime reset history |
| PhyRdy → PhyNRdy transitions | `978 175` | аномально высокая link-transition history |
| Lifetime logical writes | `703 903 210` sectors | около 335.65 GiB |
| Lifetime logical reads | `529 079 240` sectors | около 252.28 GiB |
| Температура | `43°C` | lifetime min/max `14/47°C`, over-temperature `0` |

Последний записанный short self-test завершился без ошибки на 3 156-м часу.
Полный extended self-test в этом срезе не запускался: заявленная длительность
116 минут, а дополнительная I/O-нагрузка исказила бы активный soak.

## 4. Деградация текущей загрузки

Текущая загрузка ОС началась 2026-09-04 08:22:25 +07. В её kernel journal
зафиксировано:

| Kernel evidence | Количество/результат |
|---|---:|
| `ata3: SATA link up` | `21` |
| link up at 3.0 Gbit/s | `10` |
| link up at 1.5 Gbit/s | `11` |
| явные speed-limit events | `3` |
| media/filesystem I/O errors | `0` |

До исходного среза 07 сентября ядро последовательно деградировало transport:

```text
09:58:59  SATA link limited: 3.0 -> 1.5 Gbit/s
10:06:27  transfer mode limited: UDMA/100 -> UDMA/66
10:11:46  transfer mode limited: UDMA/66 -> UDMA/33
```

После каждого ограничения link поднимался повторно. На момент среза ext4
оставался read-write; `Buffer I/O`, uncorrectable media и filesystem errors в
kernel journal отсутствовали.

После среза, в 17:26:31, kernel journal впервые в наблюдаемом окне подтвердил
не только деградацию параметров, но и live ATA bus error с hard reset link.
Поэтому строка `media/filesystem I/O errors = 0` в таблице выше относится
строго к первоначальному интервалу 15:21–15:30 и не описывает весь последующий
run. После нового transport incident permanent media error и ext4 error
по-прежнему не наблюдались.

## 5. Вывод

Фраза «SMART overall-health PASSED» здесь не означает здоровый storage path.
SMART не показывает деградацию поверхности, но lifetime counters и текущий
kernel journal доказывают нестабильность SATA-тракта с автоматическим снижением
скорости и transfer mode. Для приложения несущественно, находится первичная
причина в самом HDD, разъёме, кабеле, питании или SATA-контроллере: весь durable
I/O path стенда является деградировавшим.

На момент среза проблемы делятся так:

- media integrity: подтверждённых bad/pending/uncorrectable sectors нет;
- transport stability: подтверждённая серьёзная деградация;
- filesystem integrity: ошибок и read-only remount нет;
- soak correctness: оценивается отдельно terminal oracle CA-90.3;
- performance: не переносится на другие платформы и не используется как
  нормативная цифра.

## 6. Контроль до итогового результата

После завершения soak повторно снимаются те же counters. Отдельным storage
failure считается любое из событий:

- рост `Reallocated_Sector_Ct`, `Current_Pending_Sector` или
  `Offline_Uncorrectable`;
- рост `UDMA_CRC_Error_Count` либо SMART error log;
- новый link reset/speed downgrade;
- kernel media I/O error;
- ext4 error или read-only remount;
- расхождение final crash/reopen oracle.

Источник среза: read-only `smartctl -x /dev/sda`, `hdparm -N /dev/sda`,
`lsblk`, `findmnt` и kernel journal текущей загрузки. Serial number и WWN
намеренно не включены в публичный документ.

## 7. Последующие доказательства

В 17:26:31 критерий «новый link reset» фактически сработал: ATA-команда
`FLUSH CACHE EXT` завершилась bus error, после чего ядро выполнило hard reset,
повторно подняло link и повторило flush. Observer открыл подтверждённый
`incident-000104` класса `disk_degradation`.

После восстановления run продолжил commits и перешёл с `clients-128` на
`clients-256`; watchdog остался healthy, invariant failures — `0`, а ext4 —
read-write. SMART media/CRC/error counters при этом не изменились, что отдельно
показывает необходимость kernel telemetry наряду со SMART.

Позднее, уже на `clients-256`, accumulated maintenance tail дважды превысил
21-минутный watchdog corridor. Оба раза процессы оставались живы, после чего
движок самостоятельно возвращался из `stalled` в `healthy` и продолжал commits:
сначала до `91 021`, затем до `95 361`. Invariant failures оставались `0`, новых
kernel storage errors не появилось. Поэтому промежуточные stall не считаются
провалом `256`; окончательный verdict ждёт terminal checkpoint/invariants.

Точный timeline, raw excerpts и граница доказанного зафиксированы в
[`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md`](CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md).

Terminal checkpoint, snapshot/restore и logical digest позднее прошли при
`2 100/0` invariant passes/failures. Итоговый verdict:
[`CA_90_3_6H_ACCEPTANCE_REPORT.ru.md`](CA_90_3_6H_ACCEPTANCE_REPORT.ru.md).
