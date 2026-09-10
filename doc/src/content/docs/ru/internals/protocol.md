---
title: Wire protocol
description: Framing protocol 17, состояния session, cursors, external values и compatibility.
---

Server RadixDB 1.2 и TCP clients разделяют один binary contract, которым владеет
private crate `radixdb-protocol`. Это собственный protocol 17 RadixDB, а не
PostgreSQL wire protocol. Приложения используют `radixdb-client`; прямое
использование codec является internal integration boundary.

## Frame и negotiation

```text
4-byte unsigned big-endian payload length
        + one bincode-standard ClientMessage or ServerMessage payload
```

Нулевая длина недопустима. До allocation каждый peer отклоняет payload больше
negotiated frame limit. Client и server начинают с protocol cap 64 MiB;
handshake выбирает минимум из запроса client, настройки server и этого cap.
Decode имеет отдельный allocation ceiling 256 MiB и должен употребить весь
payload. Server также применяет настраиваемый process-wide budget байтов всех
in-flight frames.

Protocol enums используют standard configuration bincode и не являются
self-describing. Поэтому первое client message должно указывать ровно protocol
17. Другая версия отклоняется, а не декодируется как близкий layout.

## State machine session

```text
TCP connected
  -> Handshake / HandshakeAccepted
  -> Authenticate or AuthenticatePrincipal / AuthenticationAccepted
  -> Ready
       -> SelectDatabase when using bootstrap authentication
       -> Execute or Prepare + ExecutePrepared
       -> Fetch or FetchColumnBatch until EOF
       -> transaction and savepoint commands
       -> status, cancellation and close commands
```

Handshake согласует frame limit и capabilities. Authentication должна
предшествовать всем ready-state commands. `AuthenticatePrincipal` одной
операцией аутентифицирует subject в базе и выбирает её; такая session не может
переключиться на другую базу. После bootstrap `Authenticate` требуется отдельный
`SelectDatabase`. Повтор handshake или authentication после перехода в Ready
является protocol violation.

Session хранит не больше одного active cursor. До следующего statement, смены
database или конфликтующего state caller должен дочитать cursor до EOF, закрыть
или отменить его. IDs prepared statement и cursor принадлежат connection и не
переносятся между connections.

## Requests и responses

| Область | Client messages | Основные server outcomes |
| --- | --- | --- |
| Setup | `Handshake`, `Authenticate`, `AuthenticatePrincipal`, `SelectDatabase` | Accepted state или typed failure |
| SQL | `Execute`, `Prepare`, `ExecutePrepared`, `ClosePrepared` | `CommandComplete`, `CursorOpened`, prepared lifecycle |
| Results | `Fetch`, `FetchColumnBatch`, `CloseCursor`, `Cancel` | `RowBatch`, `ColumnBatch`, terminal close/failure |
| Transactions | Begin, commit, rollback и три savepoint operations | Explicit success или `TransactionFailed { active }` |
| Control | `CancelExecution`, `ServerStatus`, `CloseDatabase` | Cancellation result, bounded status или close result |

Каждое execution имеет ненулевой request ID. Out-of-band cancellation
отправляется по другому authenticated connection и адресует active request по
этому ID. Response сообщает, был ли request найден; cancellation остаётся
cooperative и сама по себе не доказывает, успел ли одновременно завершившийся
command выполнить publication.

## Rows и column batches

Каждый cursor начинается с ordered column metadata. Базовый result path имеет
row-major форму и переносит полный scalar wire domain: NULL, signed и unsigned
integers, floating point, DECIMAL, UTF-8 text, bytes, DATE, DATETIME,
nanosecond TIMESTAMP, JSON, packed `f32` VECTOR и UUID.

`ColumnBatchV1` является optional negotiated capability для подходящих
artifact-backed scans. Он переносит typed integer, float, boolean, timestamp,
dictionary-text, byte и JSON columns без allocation отдельного row object на
каждую cell. Filtering, MVCC overlays, schema mapping, ordering или
unsupported value types включают обычный `RowBatch`; этот fallback сохраняет
семантику query.

`BuildIdentityV1` добавляет semantic version, Git revision, protocol version,
profile и target в `ServerStatus`. Сбор status ограничен server limits и может
пометить artifact counts как incomplete вместо заявления о полном scan.

## External values

`ExternalValueV1` допускает values, созданные catalog-bound native extensions.
Row value переносит `type_object_id`, ненулевой `codec_version` и canonical
`payload`. External column batch переносит ту же type identity, packed data,
offsets и NULL markers. Column metadata повторяет identity, чтобы client
проверял каждое значение до выдачи приложению.

Type object ID выводится из package UUID и stable local ID. Он не является SQL
type name и не меняется при переименовании schema object. Предел payload равен
16 MiB и может быть снижен type descriptor. Protocol 17 никогда не заменяет
external value на `BYTES`; client без capability получает `UnsupportedType`.

Low-level Rust client повторно экспортирует `WireValue::External`. High-level
ORM не угадывает способ decoding extension bytes: приложению нужен
plugin-aware adapter, identity и codec revision которого совпадают с
`DESCRIBE DATABASE`.

## Failure и retry boundary

Protocol failures имеют устойчивые классы authentication, authorization, SQL,
cursor, transaction state, commands-out-of-sync и server error. Сейчас только
явный response `CompactionBackpressure` доказывает, что logical action не была
опубликована, и помечается retryable. Read/write timeout, EOF или cancelled
client future оставляет outcome неизвестным; client помечает connection как
poisoned, чтобы его нельзя было вернуть в pool.

`TransactionFailed` сообщает, остаётся ли transaction active. Client должен
следовать этому field, а не угадывать доступность rollback. Result batches
проверяются по shape и type относительно metadata из `CursorOpened` до выдачи
caller.

## Security и compatibility

Аутентификация протокола принимает субъекты выбранной базы и проверяет право
`CONNECT` до перехода session в состояние Ready. Stock server предоставляет
либо plaintext TCP, либо прямой TLS. TLS защищает протокол с первого байта и
проверяет центр сертификации и имя сервера; STARTTLS и downgrade на plaintext
не поддерживаются. Вход `root` без пароля остаётся восстановительным режимом
только для plaintext loopback, если server-side verifier root не настроен.
При наличии verifier пароль становится обязательным для `Authenticate` при
любом transport и bind address. Рекомендации по развёртыванию приведены в
[«Аутентификации»](../../administration/authentication/).

Client и server следует собирать из одной принятой revision. Совпадение protocol
version необходимо, но не обещает независимый lifecycle packages или forward
compatibility для несогласованных enum variants. При upgrade проверьте build
identity сервера и выполните client connection probe из главы
[«TCP-клиент Rust»](../../clients/rust-client/).
