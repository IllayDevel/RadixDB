# Лицензирование RadixDB

Copyright (c) 2026 RadixDB contributors.

**Это справочное русское описание. При расхождении определяющим является
английский файл [LICENSING.md](LICENSING.md).**

RadixDB использует покомпонентное лицензирование:

- корневой пакет, embedded engine, сервер, storage, catalog, planner/executor,
  транзакции, восстановление и внутренние движковые компоненты:
  PolyForm Perimeter License 1.0.1;
- TCP-клиент, ORM, публичный wire protocol, SDK/ABI/macros/tooling расширений,
  reference extension `radixdb-spatial`, proof-plugin и публичные примеры:
  Apache License 2.0;
- `radixdb-cli` пока относится к Perimeter, поскольку является target
  корневого engine-пакета.

Отдельное письменное коммерческое соглашение требуется для разрешённой
перепродажи, ребрендинга, конкурирующего форка, OEM-дистрибуции RadixDB как СУБД
или конкурирующего DBaaS.

Licensing steward: **Ledenev Nikita**. Контакт: **dev@radixdb.org**.

Участники сохраняют авторские права на свои вклады и принимают
[CLA](CLA.md), позволяющий поддерживать эту лицензионную модель и выдавать
коммерческие лицензии.
