# Публичные доказательства RadixDB

[English](README.md)

В этом каталоге собраны принятые доказательства для утверждений о
быстродействии, размере хранения, потреблении памяти и надёжности в руководстве
RadixDB. Каталог входит в публичные исходники документации и публикуется вместе
с руководством.

В набор входят только принятые финальные отчёты. Каждый отчёт доступен на
английском и русском языках: базовое имя `.md` обозначает английский вариант,
а `.ru.md` — русский. Измеренные значения и идентификаторы в каждой паре
равнозначны. Большие наборы исходных результатов остаются вне Git; где
возможно, отчёты сохраняют SHA-256 исходников, исполняемого файла, Cargo.lock и
результатов. Файл `.log` является непереводимым первичным машинным журналом,
общим для обоих языков.

## Быстродействие и ресурсы

- [`CA_80_5C_MICRO_DEVICE_20K_REPORT.ru.md`](performance/CA_80_5C_MICRO_DEVICE_20K_REPORT.ru.md): принятый профиль памяти и размера на 20 000 строк.
- [`CA_80_7_100M_COMPARATIVE_REPORT.ru.md`](performance/CA_80_7_100M_COMPARATIVE_REPORT.ru.md): принятое сравнение RadixDB и PostgreSQL на 100 миллионах строк.
- [`FINAL_100M_NVME_ACCEPTANCE_REPORT.ru.md`](performance/FINAL_100M_NVME_ACCEPTANCE_REPORT.ru.md): прежний baseline RadixDB 100M для сравнения.
- [`icp621-pg18-page-cache-matrix-100m-20260828.ru.md`](performance/icp621-pg18-page-cache-matrix-100m-20260828.ru.md): baseline PostgreSQL 18.3 для сравнения.
- [`RADIXDB_1_1_100M_VALIDATION.ru.md`](performance/RADIXDB_1_1_100M_VALIDATION.ru.md): принятая проверка запросов и памяти выпуска RadixDB 1.1.
- [`RADIXDB_1_2_100M_VALIDATION.ru.md`](performance/RADIXDB_1_2_100M_VALIDATION.ru.md): проверка корректности и ресурсов текущей версии 1.2 на канонической базе 100M.

## Надёжность

- [`CA_90_3_6H_ACCEPTANCE_REPORT.ru.md`](reliability/CA_90_3_6H_ACCEPTANCE_REPORT.ru.md): принятый отчёт шестичасового испытания на ограниченном железе.
- [`CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.ru.md`](reliability/CA_90_3_ATOM_HDD_STORAGE_HEALTH_SNAPSHOT.ru.md): состояние накопителя во время прогона.
- [`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md`](reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_INCIDENT.ru.md): наблюдавшийся сброс ATA и граница доказанного восстановления.
- [`CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log`](reliability/CA_90_3_LIVE_ATA_FLUSH_RECOVERY_EVIDENCE.log): выбранные записи ядра, процессов и финального состояния с временными метками.

Каждый результат относится только к указанным исполняемому файлу, набору
данных и машине. Сопоставимая сводка и ограничения приведены в приложении об
испытаниях.
