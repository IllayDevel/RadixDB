---
title: Нативные расширения
description: Создание типизированных Rust-расширений с безопасным SDK, сгенерированными ABI-адаптерами и воспроизводимыми пакетами.
---

SDK расширений RadixDB 1.2 позволяет автору писать обычный типизированный Rust,
а procedural macros генерируют C-compatible ABI boundary. В прикладном коде не
нужны raw pointers, numeric ABI tags, descriptor arrays или функции
`extern "C"`.

Нативные расширения подходят для ограниченных предметных типов,
вычислительных функций, операторов и индексируемых предикатов, которым нужна
скорость native-кода. Это доверенный код процесса, а не механизм исполнения
недоверенных модулей.

## Каркас проекта

Plugin является самостоятельным Cargo project с одним `cdylib`, lockfile и
одной normal dependency: публичным SDK `radixdb-plugin` из соответствующей
линии RadixDB. Текущее дерево исходников использует path dependency; заменяйте
путь только SDK artifact для того же plugin ABI.

```toml
[package]
name = "radixdb-pair-plugin"
version = "1.0.0"
edition = "2021"
rust-version = "1.97"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
radixdb-plugin = { path = "../../../crates/radixdb-plugin" }

[workspace]

[profile.release]
panic = "unwind"
```

Официальный tool отклоняет прямые normal или build dependencies на
`radixdb-plugin-abi`, crate макросов, engine, catalog, executor или storage.
Test-only development dependencies могут предоставлять отдельный integration
harness, но не линкуются в production `cdylib`.

Начальная структура:

```text
radixdb-pair-plugin/
  Cargo.toml
  Cargo.lock
  radixdb-plugin-golden.toml
  install.sql
  src/
    lib.rs
```

Полный пример находится в `examples/public/rust-plugin`. Более крупный
эталон `crates/radixdb-spatial` показывает external types point, box и polygon,
scalar и batch functions, operators, B-tree operator class и bounded planner
support.

## Минимальное расширение

```rust
use radixdb_plugin::prelude::*;

#[radixdb_plugin(
    id = "0199f8d2-7fb2-7c21-b5c1-6cb59c96b410",
    name = "radixdb_pair",
    version = "1.0.0"
)]
mod pair_plugin {
    use super::*;

    #[derive(Debug, Default, Clone, Copy, RadixType)]
    #[radix_type(
        id = "pair",
        name = "pair",
        codec = 1,
        semantic_revision = 1,
        storage = "fixed",
        max_bytes = 16,
        equality = pair_equal,
        hash = pair_hash,
        ordering = pair_compare
    )]
    struct Pair {
        #[radix_field(codec = "i64-le")]
        left: i64,
        #[radix_field(codec = "i64-le")]
        right: i64,
    }

    fn pair_equal(left: &Pair, right: &Pair) -> bool {
        left.left == right.left && left.right == right.right
    }

    fn pair_hash(value: &Pair, sink: &mut HashSink<'_>) -> PluginResult<()> {
        sink.i64(value.left)?;
        sink.i64(value.right)
    }

    fn pair_compare(left: &Pair, right: &Pair) -> std::cmp::Ordering {
        (left.left, left.right).cmp(&(right.left, right.right))
    }

    #[radixdb_scalar(
        id = "pair_sum",
        name = "pair_sum",
        semantic_revision = 1,
        immutable,
        strict,
        parallel_safe,
        cost = 1,
        cancellation = "bounded",
        max_output_bytes = 8
    )]
    fn pair_sum(value: Pair) -> PluginResult<i64> {
        value
            .left
            .checked_add(value.right)
            .ok_or_else(|| PluginError::domain("pair sum overflow"))
    }
}
```

Внешний module должен быть inline. UUID пакета постоянен и не зависит от его
Cargo- или SQL-имени. Каждый экспортируемый объект имеет stable local `id`; SDK
выводит object identity из UUID пакета и этого ID. Сохраняйте оба значения
неизменными всё время жизни совместимых persisted data.

## Карта макросов

| Макрос | Что объявляет автор | Зачем это требуется |
| --- | --- | --- |
| `#[radixdb_plugin]` | UUID пакета, canonical name и SemVer | Генерирует единственный экспортируемый ABI entrypoint, immutable descriptor graph, negotiation и panic barriers |
| `#[derive(RadixType)]` с `#[radix_type]` | Identity внешнего типа, ревизии codec и semantics, storage shape, границы и semantic callbacks | Отделяет persisted bytes от Rust memory layout и делает compatibility checks детерминированными |
| `#[radix_field]` | Canonical little-endian field codec и границы sequence | Не позволяет native-endian или unbounded serialization стать долговечным форматом |
| `#[radixdb_scalar]` | Типизированную сигнатуру и свойства выполнения функции | Генерирует argument validation, typed conversion, result accounting, diagnostics и unwind containment |
| `#[radixdb_batch]` | Column callback, связанный с одной scalar function | Ускоряет горячий путь, сохраняя scalar function как контракт корректности |
| `#[radixdb_operator]` | SQL symbol, типы аргументов/результата и backing function | Создаёт явную и проверяемую правами operator binding в catalog |
| `#[radixdb_operator_class]` | Core access method, input type, key type и key codec revision | Позволяет plugin задать семантику, пока RadixDB владеет storage и recovery индекса |
| `#[radixdb_planner_support]` | Target function/class, границы результата и recheck policy | Передаёт bounded candidate ranges без владения optimizer или storage |

Экспортируемые имена `radixdb_aggregate`, `radixdb_window` и `radixdb_tvf`
зарезервированы и намеренно прекращают компиляцию в 1.2. Native aggregate,
window и table-valued functions расширений не входят в эту authoring revision.

## Внешние типы и codecs

`#[radix_type]` требует ненулевые `codec` и `semantic_revision`, объявление
storage `fixed` либо `variable` и `max_bytes` в диапазоне `1..=16777216`. Для
фиксированного derived type `max_bytes` должен точно совпасть с шириной
закодированных полей.

Derived codecs поддерживают signed и unsigned integers, `f32`, `f64`, `bool`,
fixed arrays и bounded `Vec<T>`. Primitive codecs задаются явными
little-endian именами, например `i64-le`, `u32-le`, `f64-le` и `bool-u8`.
Sequence обязательно задаёт `max_items` и `max_bytes`:

```rust
#[derive(Debug, Default, Clone, RadixType)]
#[radix_type(
    id = "sample_set",
    name = "sample_set",
    codec = 1,
    semantic_revision = 1,
    storage = "variable",
    max_bytes = 260
)]
struct SampleSet {
    #[radix_field(codec = "i64-le", max_items = 32, max_bytes = 260)]
    values: Vec<i64>,
}
```

Не копируйте память Rust struct как persisted bytes. Rust layout, padding и
native endian не являются storage contract. Float field codecs сохраняют
точные IEEE bits, но не определяют сравнение NaN или signed zero.

Для сложной bounded shape задайте `manual = CodecType` и реализуйте
`ManualCodec<T>` через `CodecReader`, `CodecWriter` и детерминированный corpus.
Reader должен поглотить полное canonical representation, а оба пути остаются
ограничены `max_bytes`.

```rust
impl ManualCodec<Shape> for ShapeCodec {
    fn encode(value: &Shape, output: &mut CodecWriter) -> PluginResult<()> {
        if value.items.len() > 32 {
            return Err(PluginError::limit_exceeded("shape exceeds 32 items"));
        }
        output.write(&(value.items.len() as u32).to_le_bytes())?;
        for item in &value.items {
            output.write(&item.to_le_bytes())?;
        }
        Ok(())
    }

    fn decode(input: &mut CodecReader<'_>) -> PluginResult<Shape> {
        let bytes = input.read(4)?;
        let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if count > 32 {
            return Err(PluginError::limit_exceeded("shape exceeds 32 items"));
        }
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let bytes = input.read(8)?;
            items.push(i64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]));
        }
        Ok(Shape { items })
    }

    fn corpus() -> Vec<Shape> {
        vec![Shape { items: vec![] }, Shape { items: vec![0, i64::MAX] }]
    }
}
```

Объявляйте equality, hash и ordering только когда тип их поддерживает. Hash
требует equality. Hash callback пишет semantic components в `HashSink` и не
выбирает hash algorithm движка или physical token. Equality-equivalent values
должны выводить одинаковые компоненты, ordering должен быть total, а оба
отношения согласованными. Local test host проверяет эти законы на generated или
manual corpus.

Увеличивайте `codec`, когда меняются canonical bytes. Увеличивайте
`semantic_revision`, когда меняется semantics equality, hash, ordering,
function, operator class или planner без изменения bytes. Смена package SemVer
не заменяет ни одну из этих ревизий.

## Scalar и batch functions

Scalar принимает owned values поддержанных типов и возвращает
`PluginResult<T>`. К built-in относятся integer и floating Rust primitives,
`bool`, `BoundedText<N>` и `BoundedBytes<N>`; local `RadixType` и `Option<T>`
выражают внешние типы и nullability.

Каждая scalar function объявляет ровно одно из `immutable`, `stable` или
`volatile`. `strict` означает, что NULL input даёт NULL без входа в callback.
`parallel_safe`, `cost`, `cancellation = "bounded"` и предел result bytes
являются обещаниями host. Задавайте их по поведению, а не по желаемому plan.

Явный batch adapter принимает по одному `ColumnView` на scalar argument,
типизированный `ColumnBuilder` и `CallContext`. Он должен сохранять scalar
семантику NULL, порядка и ошибок и проверять cancellation с объявленным
интервалом:

```rust
#[radixdb_batch(for_scalar = "distance", rows_per_cancel_check = 64)]
fn distance_batch(
    left: ColumnView<'_, Point>,
    right: ColumnView<'_, Point>,
    output: &mut ColumnBuilder<'_, '_, f64>,
    context: &CallContext<'_>,
) -> PluginResult<()> {
    for (row, (left, right)) in left.zip(right).enumerate() {
        if row % 64 == 0 {
            context.check_cancelled()?;
        }
        output.push(distance(left?, right?)?)?;
    }
    Ok(())
}
```

После ошибки host не публикует частичный batch. Форма scalar callback в 1.2 не
открывает `CallContext`, поэтому scalar work должна оставаться ограниченной
своей декларацией. Batch callback использует `check_cancelled()` и может
применять `charge_work()` для дополнительного учета бюджета. Output всегда
должен оставаться в объявленной границе.

Возвращайте подходящую категорию `PluginError::invalid_input`, `domain`,
`limit_exceeded`, `cancelled` или `internal`. Отображением в SQL diagnostics
владеет host. Generated wrappers перехватывают обычный Rust unwind, но не могут
удержать `abort`, segmentation fault, undefined behavior или вредоносный код.

## Operators, operator classes и planner support

Operator является metadata над уже объявленной native scalar. Rust macro 1.2
объявляет binary operators с явными left, right и result types. Затем operator
class связывает полный набор strategies и canonical key encoder с core-owned
B-tree, hash или bitmap index. B-tree требует `<`, `<=`, `=`, `>=` и `>`; hash
и bitmap требуют `=`. External HNSW operator classes в SDK 1.2 недоступны.

```rust
#[radixdb_operator(
    id = "point_eq_operator",
    symbol = "=",
    semantic_revision = 1,
    function = "point_eq",
    left = Point,
    right = Point,
    result = bool
)]
fn point_eq_operator() {}

#[radixdb_operator_class(
    id = "point_morton_btree",
    semantic_revision = 1,
    access_method = "btree",
    input = Point,
    key = BoundedBytes::<16>,
    key_codec_revision = 1
)]
fn point_morton_key(point: Point) -> PluginResult<BoundedBytes<16>> {
    BoundedBytes::new(morton_bytes(point).to_vec())
}
```

Planner support читает normalized predicate и выдаёт bounded key spans. Он
обязан задать estimate и объявить ровно одно из `exact` или `always_recheck`.
Используйте `always_recheck` для approximate cover: executor вычисляет исходный
predicate для каждого candidate. Host canonicalize и merge spans, удаляет
повторные rows и выбирает conservative fallback, если support не построил
корректный plan.

```rust
#[radixdb_planner_support(
    id = "within_box_support",
    name = "st_within_box_support",
    semantic_revision = 1,
    for_function = "within_box",
    operator_class = "point_morton_btree",
    always_recheck,
    max_spans = 1024,
    max_output_bytes = 49152
)]
fn within_box_support(
    predicate: PredicateView<'_>,
    output: &mut CandidatePlanBuilder<'_, '_>,
) -> PluginResult<()> {
    let bounds = predicate.constant::<Box2d>(1)?
        .ok_or_else(|| PluginError::domain("NULL bounds"))?;
    output.set_estimate(100, 10)?;
    for span in candidate_spans(bounds)? {
        output.push_span(span)?;
    }
    Ok(())
}
```

Ни один макрос не предоставляет доступ к WAL, pages, MVCC, compaction,
transactions, catalog mutation или physical index formats. Если возможность
требует такого владения, она должна стать универсальным engine API, а не plugin
callback.

## Тесты, golden vectors и упаковка

Используйте `radixdb_plugin::testing` в unit tests для проверки descriptor
graph, codec roundtrip, malformed bytes, scalar/batch parity, законов
equality/hash/order, strategies operator class и planner spans. Каждому
external type также нужен хотя бы один canonical golden vector в
`radixdb-plugin-golden.toml`.

```toml
format = 1
package_id = "0199f8d2-7fb2-7c21-b5c1-6cb59c96b410"

[[types]]
object_id = "daa5e83d0ea33c36cade62862baeef54"
codec_version = 1
codec_fingerprint = "4899dde00ce9b0810701ad36df11a10910d4b46356295f7c8d2f5317effcc998"
vectors = [
    "00000000000000000000000000000000",
    "0100000000000000ffffffffffffffff",
    "0000000000000080ffffffffffffff7f",
]
```

Запускайте безопасный цикл автора из checkout RadixDB:

```sh
cargo run --locked -p cargo-radixdb-plugin -- check \
  --manifest-path examples/public/rust-plugin/Cargo.toml
cargo run --locked -p cargo-radixdb-plugin -- test-host \
  --manifest-path examples/public/rust-plugin/Cargo.toml
```

`test-host` запускает тесты plugin, выполняет official-shape build, исследует
descriptor в isolated child и проверяет все golden vectors. Переходите к
package только после этих проверок. [Справочник инструмента](../../reference/programs/cargo-radixdb-plugin/)
описывает official builds, сравнение совместимости и инспекцию artifact;
[глава администрирования](../../administration/extensions/) — установку на
сервер и привязку к базе.

SDK 1.2 не генерирует text input/output callbacks для SQL literals и не даёт
универсальное high-level ORM representation внешних значений. Клиенты
передают canonical bytes по protocol 17 и должны сгенерировать plugin-aware
adapter из metadata `DESCRIBE DATABASE`. Не подменяйте их raw `BYTES`: при этом
теряются identity типа и codec admission.
