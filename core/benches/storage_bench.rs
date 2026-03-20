use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use fluxmq::storage::InMemoryStorage;
use std::collections::HashMap;

fn make_message(size: usize) -> fluxmq::protocol::Message {
    fluxmq::protocol::Message {
        key: Some(Bytes::from_static(b"bench-key")),
        value: Bytes::from(vec![0x42u8; size]),
        timestamp: 1_700_000_000_000,
        headers: HashMap::new(),
    }
}

fn make_batch(count: usize, msg_size: usize) -> Vec<fluxmq::protocol::Message> {
    (0..count).map(|_| make_message(msg_size)).collect()
}

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_messages");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let storage = InMemoryStorage::new();
                b.iter_batched(
                    || make_batch(size, 256),
                    |messages| {
                        storage
                            .append_messages("bench-topic", 0, messages)
                            .unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch_messages_arc");

    for msg_count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(msg_count),
            &msg_count,
            |b, &count| {
                let storage = InMemoryStorage::new();
                let batch = make_batch(count, 256);
                storage
                    .append_messages("bench-topic", 0, batch)
                    .unwrap();

                b.iter(|| {
                    storage
                        .fetch_messages_arc("bench-topic", 0, 0, 64 * 1024 * 1024)
                        .unwrap()
                });
            },
        );
    }
    group.finish();
}

fn bench_concurrent_append_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_append_fetch");
    group.throughput(Throughput::Elements(100));

    group.bench_function("append_100_then_fetch", |b| {
        let storage = InMemoryStorage::new();
        // Pre-populate
        let prepop = make_batch(1000, 256);
        storage
            .append_messages("bench-topic", 0, prepop)
            .unwrap();

        b.iter(|| {
            let batch = make_batch(100, 256);
            storage
                .append_messages("bench-topic", 0, batch)
                .unwrap();
            storage
                .fetch_messages_arc("bench-topic", 0, 900, 1024 * 1024)
                .unwrap()
        });
    });

    group.finish();
}

fn bench_multi_partition(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_partition_append");
    group.throughput(Throughput::Elements(800)); // 8 partitions x 100 msgs

    group.bench_function("8_partitions_100_msgs_each", |b| {
        let storage = InMemoryStorage::new();
        b.iter(|| {
            for partition in 0..8u32 {
                let batch = make_batch(100, 256);
                storage
                    .append_messages("bench-topic", partition, batch)
                    .unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_fetch,
    bench_concurrent_append_fetch,
    bench_multi_partition,
);
criterion_main!(benches);
