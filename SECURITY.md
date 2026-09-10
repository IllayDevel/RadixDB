# Security Policy

[Русский](SECURITY.ru.md)

RadixDB deployments need an external backup, restore and recovery plan tested
on their own workload. Accepted release evidence is recorded in
[CHANGELOG.md](CHANGELOG.md).

## Reporting a vulnerability

For sensitive issues, use GitHub private vulnerability reporting if it is
enabled for the repository. If it is not enabled yet, contact the repository
maintainers out of band and avoid posting exploit details in a public issue.

Public GitHub issues are appropriate for non-sensitive bugs, documentation
errors and reproducible crashes that do not expose a security weakness.

When reporting, please include:

- RadixDB version and commit hash;
- operating system and CPU architecture;
- whether the issue affects embedded, CLI or TCP server mode;
- the smallest SQL/script/configuration that reproduces the issue;
- whether data corruption, data disclosure or denial of service is suspected.

## Supported versions

Include the exact release and commit in reports. The current accepted release
is 1.1.0. This policy does not promise a security support duration or automatic
backports to older revisions; support decisions are recorded per release.

## Scope notes

Known current limitations are documented in:

- [Current limitations](doc/src/content/docs/en/appendices/limits.md)
- [Текущие ограничения](doc/src/content/docs/ru/appendices/limits.md)

The 1.2 development server authenticates catalog Principals with Argon2id-backed
password verifiers. Plain TCP is supported for trusted hosts or protected
networks; direct TLS validates certificate chains and server names and rejects
plaintext downgrade on that endpoint. A configured Argon2id verifier requires
the original password for `root` on every endpoint and disables passwordless
login. Without a verifier, passwordless `root` remains available only as a
recovery path on a plaintext loopback listener. Follow the
[authentication guide](doc/src/content/docs/en/administration/authentication.md)
and grant each Principal only the required database, schema and object privileges.
