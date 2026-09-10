//! Deterministic allocation gate for canonical ORM JSON round trips.
//!
//! Run with:
//! `cargo run --locked --release --features dhat-heap --example orm_allocations`

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use std::hint::black_box;

use radixdb_orm::{table_as, Expr, IrDocument, OrmBuilder, QueryBuilder};

const ROUNDS: u64 = 1_000;
const MAX_BLOCKS_PER_ROUND: u64 = 256;
const MAX_BYTES_PER_ROUND: u64 = 128 * 1024;

fn main() {
    let document = QueryBuilder::from_relation(table_as("people", "p"))
        .select([
            Expr::qualified("p", "id"),
            Expr::qualified("p", "name"),
            Expr::navigation("p", ["fio_id", "name"]),
        ])
        .filter(
            Expr::qualified("p", "active")
                .eq(true)
                .and(Expr::qualified("p", "score").between(100_i64, 200_i64))
                .and(Expr::qualified("p", "name").like("person-%")),
        )
        .order_by([Expr::qualified("p", "score").desc().nulls_last()])
        .limit(100)
        .document()
        .unwrap();

    // Warm serialization code and allocator metadata before the measured
    // phase. The document itself is deliberately outside the allocation gate.
    let warm = document.to_json().unwrap();
    black_box(IrDocument::from_json(&warm).unwrap());

    let profiler = dhat::Profiler::builder().testing().build();
    for _ in 0..ROUNDS {
        let json = black_box(&document).to_json().unwrap();
        black_box(IrDocument::from_json(black_box(&json)).unwrap());
    }
    let stats = dhat::HeapStats::get();
    drop(profiler);

    let blocks_per_round = stats.total_blocks / ROUNDS;
    let bytes_per_round = stats.total_bytes / ROUNDS;
    println!(
        "orm_json_round_trip allocations: rounds={ROUNDS} total_blocks={} total_bytes={} blocks_per_round={} bytes_per_round={}",
        stats.total_blocks, stats.total_bytes, blocks_per_round, bytes_per_round
    );
    assert!(
        blocks_per_round <= MAX_BLOCKS_PER_ROUND,
        "allocation count regressed: {blocks_per_round} > {MAX_BLOCKS_PER_ROUND}"
    );
    assert!(
        bytes_per_round <= MAX_BYTES_PER_ROUND,
        "allocated bytes regressed: {bytes_per_round} > {MAX_BYTES_PER_ROUND}"
    );
}
