//! Benchmarks for ClickHouse Rust client

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use clickhouse_rs::types::{Block, Column, ColumnData, UInt8, String as ClickHouseString};

fn create_test_block() -> Block {
    let mut block = Block::new();
    
    // Add ID column
    let id_column = Column::new(
        "id".to_string(),
        ColumnData::UInt8(vec![1, 2, 3, 4, 5]),
    );
    block.add_column(id_column);
    
    // Add name column
    let name_column = Column::new(
        "name".to_string(),
        ColumnData::String(vec![
            ClickHouseString::new("Alice"),
            ClickHouseString::new("Bob"),
            ClickHouseString::new("Charlie"),
            ClickHouseString::new("Diana"),
            ClickHouseString::new("Eve"),
        ]),
    );
    block.add_column(name_column);
    
    block
}

fn block_creation_benchmark(c: &mut Criterion) {
    c.bench_function("block_creation", |b| {
        b.iter(|| create_test_block())
    });
}

fn block_serialization_benchmark(c: &mut Criterion) {
    let block = create_test_block();
    
    c.bench_function("block_serialization", |b| {
        b.iter(|| {
            let _ = serde_json::to_string(&block);
        })
    });
}

fn block_deserialization_benchmark(c: &mut Criterion) {
    let block = create_test_block();
    let json = serde_json::to_string(&block).unwrap();
    
    c.bench_function("block_deserialization", |b| {
        b.iter(|| {
            let _: Block = serde_json::from_str(&json).unwrap();
        })
    });
}

criterion_group!(
    benches,
    block_creation_benchmark,
    block_serialization_benchmark,
    block_deserialization_benchmark
);
criterion_main!(benches);
