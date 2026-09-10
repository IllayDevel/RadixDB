use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};

fn lower_contract_probe(criterion: &mut Criterion) {
    criterion.bench_function("executor-shell/lower-contracts", |bencher| {
        bencher.iter(|| {
            let statements = radixdb_sql::parse_sql(black_box("SELECT 1"))
                .expect("parse executor shell benchmark probe");
            black_box(statements);
            black_box(radixdb_core::Value::Integer(1));
            black_box(radixdb_functions::FunctionRegistry::new());
            black_box(radixdb_storage::Config::in_memory());
        });
    });
}

criterion_group!(benches, lower_contract_probe);
criterion_main!(benches);
