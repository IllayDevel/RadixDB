---
title: cargo radixdb-plugin
description: Проверка, тестирование, инспекция, сборка и упаковка native extensions RadixDB.
---

`cargo radixdb-plugin` является поддерживаемым command-line tool для native
extension на Rust. До передачи artifact серверу он проверяет границу public
SDK, deterministic build profile, descriptor contract и package admission.

## Запуск

Установите или соберите binary `cargo-radixdb-plugin`, затем используйте любую
из равнозначных форм:

```sh
cargo radixdb-plugin check --manifest-path Cargo.toml
cargo-radixdb-plugin check --manifest-path Cargo.toml
```

## Команды

```text
cargo radixdb-plugin check
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin build
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin test-host
    [--manifest-path PATH]
    [--target-dir PATH]

cargo radixdb-plugin inspect
    (--library PATH | --package PATH)

cargo radixdb-plugin package
    [--manifest-path PATH]
    [--target-dir PATH]
    --output-dir PATH
    [--golden PATH]
    [--previous-package PATH]
```

По умолчанию `--manifest-path` равен `Cargo.toml`. Default target directory
состоит из Cargo target directory проекта и суффикса `radixdb-plugin`.

### check

Проверяет форму проекта и запускает locked release check для
`x86_64-unknown-linux-gnu`. Проект должен содержать ровно один `cdylib`, файл
`Cargo.lock`, `panic = "unwind"` для release build и обычную dependency на
public crate `radixdb-plugin`. Другие crates `radixdb-*` допустимы только как
development dependencies.

```sh
cargo radixdb-plugin check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

### build

Собирает locked release library поддерживаемым toolchain с deterministic link
settings, затем инспектирует exported descriptor в isolated child process.
Команда пишет путь `.so` в standard output, а descriptor identity в standard
error.

`build` удобен при разработке. Для server installation используйте полный
каталог из `package`, а не отдельную library.

### test-host

Запускает release tests extension, собирает и инспектирует library, затем
проверяет каждый external type по `radixdb-plugin-golden.toml` в isolated child
process.

```sh
cargo radixdb-plugin test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

Golden file должен перечислять каждый exported external type ровно один раз.
Каждый vector записывается canonical lowercase hexadecimal и после decoding и
повторного encoding должен дать те же bytes.

### inspect

`--library` загружает отдельный `.so` в isolated process и выводит normalized
descriptor как JSON. `--package` проверяет полный package layout, manifest,
checksums, provenance, compatibility report и codec vectors перед выводом того
же report.

```sh
cargo radixdb-plugin inspect --library target/plugin/lib/libexample.so
cargo radixdb-plugin inspect --package dist/example-1.0.0
```

Два input options взаимно исключают друг друга; один из них обязателен.

### package

Запускает tests и deterministic build, проверяет descriptor и codec evidence,
затем атомарно создает полный host-admissible package по пути `--output-dir`.
Этот final package directory не должен существовать заранее.

```sh
mkdir -p dist
RADIXDB_PLUGIN_BUILD_IMAGE=rust:1.97.0-bookworm \
cargo radixdb-plugin package \
  --manifest-path Cargo.toml \
  --output-dir dist/radixdb-pair-1.0.0
```

Release packaging принимается только внутри официального Debian bookworm build
image с точными версиями Rust и Cargo 1.97.0. Установка environment variable на
другом host не обходит проверки OS и toolchain.

`--golden` выбирает нестандартный файл codec vectors.
`--previous-package` сравнивает stable object identities, codecs и semantic
revisions с существующим полным package. Несовместимое сравнение останавливает
packaging. Этот report является evidence, но не мигрирует database binding.

## Результат package

Output directory сам является immutable package directory:

```text
radixdb-pair-1.0.0/
  radixdb-plugin.toml
  radixdb-plugin-golden.toml
  radixdb-plugin-provenance.toml
  radixdb-plugin-compatibility.toml
  lib/
    libradixdb_pair.so
```

Manifest записывает identity package, ABI range, descriptor fingerprint,
target, потолок glibc и library checksum. Provenance связывает library, golden
vectors и compatibility report с official build environment. Tool формирует
staging files, синхронизирует их и переименовывает каталог только после
успешного local admission.

## Требуемый toolchain

Development checks требуют Rust 1.97.0 с host target
`x86_64-unknown-linux-gnu`. Начальный ABI поддерживает ELF64 little-endian GNU
Linux и glibc не новее 2.36. Artifacts для другого target отклоняются.

## Exit status

Ноль означает завершение запрошенной проверки или сборки. Ненулевой status
сообщает об ошибке project, compiler, descriptor, codec, compatibility,
package layout или admission. Считайте любой ненулевой результат release
blocker и не переносите частично подготовленный каталог в server allowlist.

## См. также

См. [Разработка native extensions](../../../programming/native-extensions/),
[Установка и эксплуатация extensions](../../../administration/extensions/) и
[SQL native extensions](../../sql/extensions/).
