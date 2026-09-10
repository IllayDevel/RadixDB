---
title: Аутентификация
description: Вход catalog Principal через обычный TCP или опциональный проверяемый TLS.
---

Аутентификация связывает server session с durable Principal одной базы.
Авторизация затем проверяет этот stable Principal ID, включая database
`CONNECT`, во всех точках выполнения.

## Вход catalog Principal

Создайте Principal с паролем и выдайте минимальные права базы и схемы:

```sql
CREATE PRINCIPAL application_reader PASSWORD 'replace-this-secret';
GRANT CONNECT ON DATABASE application TO application_reader;
GRANT USAGE ON SCHEMA public TO application_reader;
GRANT SELECT ON TABLE public.documents TO application_reader;
```

Пароль хранится как salted Argon2id PHC verifier, а не открытым текстом.
`ALTER PRINCIPAL ... PASSWORD` меняет или удаляет verifier; `DISABLE` запрещает
новые входы, `ENABLE` снова разрешает их.

Штатный клиент работает и через обычный TCP endpoint:

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate_database("application", "application_reader", "secret")?;
```

`authenticate_database` одной операцией аутентифицирует пользователя и выбирает
базу. Principal session не может переключиться на другую базу: для неё нужно
новое соединение и отдельная аутентификация. Неизвестный login, неверный пароль,
disabled Principal и отсутствие `CONNECT` намеренно дают одинаковый
`AuthenticationFailed`.

`DISABLE` действует на будущие входы. Уже открытая session сохраняет неизменный
Principal ID до отключения. Изменения прав действуют сразу: последующие запросы
увидят отзыв `CONNECT` или object privilege.

## Режимы транспорта

TLS опционален. Если `[server.transport]` отсутствует или явно содержит
`mode = "plaintext"`, сервер принимает тот же login/password protocol через
обычный TCP. Используйте этот режим только на доверенном host или в защищённой
сети: credentials и protocol frames не шифруются.

Для защиты транспорта включите direct TLS:

```toml
[server.transport]
mode = "tls"
certificate_chain = "/etc/radixdb/server-chain.pem"
private_key = "/etc/radixdb/server-key.pem"
```

Private key должен запрещать доступ group и other (`0600` или строже). В TLS
mode сервер загружает certificate/key при старте, не принимает plaintext
downgrade на этом endpoint и умеет атомарно перечитать материалы для новых
соединений. Ошибка reload сохраняет предыдущую in-memory generation.

Клиент проверяет одновременно CA и server name:

```rust
let tls = TlsClientConfig::from_ca_pem("/etc/radixdb/ca.pem", "db.example")?;
let mut connection = TlsConnection::connect_tls("db.example:15441", &tls)?;
connection.authenticate_database("application", "application_reader", "secret")?;
```

Expired certificate, неверное имя и неизвестный CA отклоняются до protocol
authentication. STARTTLS negotiation нет: client и server должны заранее
выбрать одинаковый transport mode.

## Административный вход root

Для штатного администрирования создайте salted Argon2id PHC verifier с помощью
установленной утилиты паролей. Она читает из перенаправленного стандартного
ввода ровно одну UTF-8 строку пароля длиной от 1 до 1024 байт. Пароль нельзя
передать аргументом процесса; интерактивный терминал с неконтролируемым echo
утилита также отклоняет.

```sh
read -rsp 'Root password: ' RADIXDB_ROOT_PASSWORD
printf '\n'
printf '%s\n' "$RADIXDB_ROOT_PASSWORD" |
  /opt/radixdb/bin/radixdb-password
unset RADIXDB_ROOT_PASSWORD
```

Скопируйте единственную строку результата в конфигурацию сервера, сохранив
полную PHC-строку в кавычках:

```toml
[server.authentication]
root_password_verifier = "$argon2id$..."
```

Перезапустите сервер для применения настройки. Наличие verifier отключает
беспарольный вход `root` на всех endpoints. Клиент обязан передать исходный
пароль; отсутствие или несовпадение пароля возвращает `AuthenticationFailed`
без уточнения причины.

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate("root", Some("administrative-secret".into()))?;
connection.select_database("application")?;
```

Вход `root` по паролю работает с plaintext и direct TLS listeners, в том числе
при non-loopback bind. Plaintext не защищает пароль при передаче, поэтому
используйте direct TLS, если весь сетевой путь не является доверенным. Считайте
verifier учётными данными, ограничьте доступ к `server.toml`, а для ротации
создайте новый verifier и перезапустите сервер. Некорректный PHC-синтаксис,
алгоритм не Argon2id или параметры вне поддерживаемых границ приводят к отказу
конфигурации до bind.

## Беспарольное восстановление

Если `server.authentication.root_password_verifier` отсутствует, беспарольный
`root` остаётся доступен только при loopback binding в plaintext mode:

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate("root", None)?;
connection.select_database("application")?;
```

На non-loopback и TLS endpoints этот fallback отклоняется. Он предназначен для
локального восстановления и первоначальной настройки, а не для обычного входа
приложения. После настройки verifier отсутствие пароля отклоняется и на
loopback. Модель Principal, Role и privileges описана в
[«Управлении доступом»](../access-control/).
