---
title: Installing the Server
description: Build a traceable release bundle and install the RadixDB TCP server as a Linux systemd service.
---

RadixDB can run inside an application or as the separate `radixdb-server`
process. This chapter installs the TCP server from a source checkout. The
supported service layout is a verified release bundle under `/opt/radixdb`;
copying one executable without its configuration, lifecycle scripts and
provenance record is not the same installation.

## Verified platform

The 1.2 procedure was executed on Fedora Linux 44, x86-64, with the GNU ABI and
Rust 1.97.0 selected by `rust-toolchain.toml`. The resulting binaries target
`x86_64-unknown-linux-gnu` and dynamically use glibc, `libm` and `libgcc_s`.

The source is portable Rust, but that does not make every operating system or
processor a verified server platform. Another target needs its own build,
filesystem, signal, recovery and service-manager checks. The supplied installer
and unit are specifically for a systemd-based Linux host.

Install these build and packaging prerequisites:

| Purpose | Required tool |
| --- | --- |
| Compile | Rust 1.97.0 with Cargo, a C compiler and linker |
| Resolve dependencies | Network access to the Cargo registry, or a populated cache |
| Split and inspect symbols | GNU `objcopy` and `readelf` |
| Verify the bundle | `sha256sum` |
| Install the service | `install`, `sed`, `getent`, `groupadd`, `useradd` and systemd |

## Build the release programs

Run the build at the source revision selected for deployment. `--locked`
prevents Cargo from silently changing dependency resolution.

```sh
cargo build --locked --release \
  --bin radixdb-server \
  --bin radixdb-password \
  --bin radixdb-cli \
  --bin radixdb-smoke-client \
  --features cli
```

Inspect all four identities before packaging:

```sh
./target/release/radixdb-server --version
./target/release/radixdb-password --version
./target/release/radixdb-cli --version
./target/release/radixdb-smoke-client --version
```

The Git revision, profile, target and Cargo lock digest must agree. The source
baseline used for this 1.2 manual currently prints package version `1.1.0`;
the full revision and protocol identify the exact checked artifact.

```text
radixdb-server 1.1.0 git=<40-hex-revision> protocol=17 profile=release target=x86_64-unknown-linux-gnu lock=<64-hex-sha256>
```

Do not deploy an identity ending in `-dirty` unless that uncommitted source state
is intentional and independently archived.

## Create and verify a bundle

The output directory must not already exist. The packager installs compact
executables, preserves separate debug files with matching ELF Build IDs, writes
`PROVENANCE.env`, and covers every file with `SHA256SUMS`.

```sh
test ! -e dist
release/package-artifacts.sh dist \
  target/release/radixdb-server \
  target/release/radixdb-cli \
  target/release/radixdb-smoke-client
(cd dist && sha256sum -c SHA256SUMS)
```

Keep the whole output together:

```text
dist/
  bin/
  debug/
  PROVENANCE.env
  SHA256SUMS
  server.toml
  radixdb.service
  install-systemd.sh
  uninstall-systemd.sh
  start.sh
  status.sh
  stop.sh
  smoke-client.sh
```

The bundle also contains the license, notices, backup/restore tools and the
scripts shared by the lifecycle commands. Transfer it as one artifact and run
the checksum verification on the target host before installation.

## Install under `/opt/radixdb`

The tracked installer must run as root for a system installation. It creates the
system account `radixdb`, copies the binaries, installs the unit, reloads systemd
and enables the service. Enabling does not start it.

```sh
cd dist
sha256sum -c SHA256SUMS
sudo ./install-systemd.sh
sudo systemctl start radixdb
sudo systemctl status radixdb --no-pager
```

The resulting layout is:

```text
/opt/radixdb/
  bin/
    radixdb-server
    radixdb-password
    radixdb-cli
    radixdb-smoke-client
  data/
  logs/
  run/
  server.toml
```

The service runs as `radixdb:radixdb`. Binaries remain owned by root. The
installer creates `server.toml` as mode `0640`, owned by `root:radixdb`, and the
data, log and run directories as mode `0750`. Never put a plaintext password in
the configuration. An optional root password verifier is still credential
material and relies on these restricted permissions.

To use another absolute prefix, pass it to both the installer and later
administrative commands. The path must not be `/` and must not contain `.` or
`..` components.

```sh
sudo RADIXDB_INSTALL_PREFIX=/srv/radixdb ./install-systemd.sh
```

The generated unit sets its working directory to the chosen prefix. Therefore
the packaged relative `data_dir = "data"` resolves below that prefix.

## Install native extension packages

Native extensions are separate immutable packages and are not embedded in the
server release bundle. Build them with `cargo radixdb-plugin package`, copy each
complete package directory to an operator-owned location such as
`/opt/radixdb/plugins/`, then list the exact directory under `[plugins]` in
`server.toml`. Restart admits every package before the listener opens.

Do not copy a bare `.so` into `bin/` or the data root. The package manifest,
codec vectors, compatibility report and provenance are part of admission and
backup. Follow [Installing and operating extensions](../extensions/) before
adding trusted native code to the service.

## Verify the installation

The default endpoint is loopback-only at `127.0.0.1:15441`. First check process
and protocol readiness:

```sh
sudo systemctl status radixdb --no-pager
./smoke-client.sh
journalctl -u radixdb -n 100 --no-pager
```

With the unmodified release template, the smoke client checks the handshake,
passwordless loopback `root` authentication, server status and build identity.
It does not accept a configured root password, open every database or prove
application-level readiness. Configure routine root access with the installed
`bin/radixdb-password` utility and verify it with a normal client as described
in [authentication](../authentication/). The [server operation](../server/)
chapter explains database selection and readiness. Review the complete
[server configuration](../configuration/) before changing the release template.

## Reinstall and remove binaries

An ordinary reinstall preserves an existing `server.toml`. Set
`RADIXDB_INSTALL_REPLACE_CONFIG=1` only when replacing it deliberately, after
retaining the previous file and reviewing the new values.

The ordinary uninstall disables the unit and removes the four executables, but
retains configuration and data:

```sh
cd dist
sudo ./uninstall-systemd.sh
```

`RADIXDB_UNINSTALL_PURGE=1` removes the complete installation prefix, including
the data. Do not use it as a routine upgrade command. Backup, restore and upgrade
procedures are documented separately from installation.

The release gate verifies a dry systemd installation in an isolated directory;
it does not replace an acceptance test on the target host's actual systemd,
filesystem and backup destination.
