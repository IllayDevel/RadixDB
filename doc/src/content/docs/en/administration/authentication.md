---
title: Authentication
description: Catalog Principal login over ordinary TCP or optional verified TLS.
---

Authentication binds a server session to a durable Principal in one database.
Authorization then checks that stable Principal ID, including database
`CONNECT`, for every execution entry point.

## Catalog login

Create a Principal with a password and grant the minimum database and schema
privileges:

```sql
CREATE PRINCIPAL application_reader PASSWORD 'replace-this-secret';
GRANT CONNECT ON DATABASE application TO application_reader;
GRANT USAGE ON SCHEMA public TO application_reader;
GRANT SELECT ON TABLE public.documents TO application_reader;
```

The password is stored as a salted Argon2id PHC verifier, never as plaintext.
Rotate or remove it with `ALTER PRINCIPAL ... PASSWORD`; disabling a Principal
rejects new logins, while `ENABLE` admits them again.

The ordinary client path works over a plain TCP endpoint:

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate_database("application", "application_reader", "secret")?;
```

`authenticate_database` authenticates and selects the named database as one
operation. A Principal session cannot switch to another database; reconnect and
authenticate for that database instead. Unknown login, wrong password, disabled
Principal and missing `CONNECT` deliberately return the same
`AuthenticationFailed` surface.

`DISABLE` affects future authentication. An already authenticated session keeps
its immutable Principal ID until disconnect. Privilege changes remain live:
revoking `CONNECT` or an object privilege is observed by later requests on that
session.

## Transport modes

TLS is optional. If `[server.transport]` is omitted, or explicitly selects
`mode = "plaintext"`, the server accepts the same login/password protocol over
ordinary TCP. Use this only on a trusted host or protected network because the
credentials and frames are not encrypted.

Enable direct TLS when transport protection is required:

```toml
[server.transport]
mode = "tls"
certificate_chain = "/etc/radixdb/server-chain.pem"
private_key = "/etc/radixdb/server-key.pem"
```

The private key must deny group and other access (`0600` or stricter). TLS mode
loads the certificate and key at startup, rejects plaintext downgrade on that
endpoint and can atomically reload material for new connections. A failed
reload preserves the previous in-memory generation.

Clients validate both the issuing CA and server name:

```rust
let tls = TlsClientConfig::from_ca_pem("/etc/radixdb/ca.pem", "db.example")?;
let mut connection = TlsConnection::connect_tls("db.example:15441", &tls)?;
connection.authenticate_database("application", "application_reader", "secret")?;
```

Expired certificates, wrong names and unknown authorities fail before protocol
authentication. There is no STARTTLS downgrade negotiation: client and server
must choose the same transport mode.

## Root administration login

For routine administration, generate a salted Argon2id PHC verifier with the
installed password utility. The utility reads exactly one UTF-8 password line
of 1 to 1024 bytes from redirected standard input. It does not accept a password
as a process argument and refuses an interactive terminal whose echo state it
cannot control.

```sh
read -rsp 'Root password: ' RADIXDB_ROOT_PASSWORD
printf '\n'
printf '%s\n' "$RADIXDB_ROOT_PASSWORD" |
  /opt/radixdb/bin/radixdb-password
unset RADIXDB_ROOT_PASSWORD
```

Copy the single output line into the server configuration, preserving the full
quoted PHC string:

```toml
[server.authentication]
root_password_verifier = "$argon2id$..."
```

Restart the server to apply the setting. A configured verifier disables
passwordless `root` on every endpoint. The client must send the original
password; a missing or wrong password returns `AuthenticationFailed` without
revealing which condition occurred.

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate("root", Some("administrative-secret".into()))?;
connection.select_database("application")?;
```

Password-authenticated `root` works with plaintext and direct TLS listeners,
including non-loopback binds. Plaintext does not protect the password in
transit, so use direct TLS unless the complete network path is trusted. Treat
the verifier as credential material, keep `server.toml` access restricted, and
rotate it by generating a new verifier and restarting the server. Invalid PHC
syntax, a non-Argon2id algorithm or parameters outside the supported security
bounds reject the configuration before bind.

## Passwordless recovery

If `server.authentication.root_password_verifier` is absent, passwordless
`root` remains available only when the server is bound to a loopback address in
plaintext mode:

```rust
let mut connection = Connection::connect("127.0.0.1:15441")?;
connection.authenticate("root", None)?;
connection.select_database("application")?;
```

This fallback is rejected on non-loopback and TLS endpoints. It is intended for
local recovery and initial provisioning, not as the ordinary application login.
Once a verifier is configured, omitting the password is rejected on loopback as
well. Continue with [access control](../access-control/) for Principal, Role and
privilege semantics.
