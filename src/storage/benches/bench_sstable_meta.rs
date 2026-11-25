// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Run with `cargo bench -p risingwave_storage --bench bench_sstable_meta --features test`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use foyer::Code;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::sstable::{
    BlockedXor16FilterBuilder, FilterBuilder, InMemWriter, Sstable, SstableBuilder,
    SstableBuilderOptions, SstableMeta,
};
use risingwave_storage::hummock::value::HummockValue;

async fn encoded_sstable(key_count: usize, key_len: usize) -> Vec<u8> {
    let options = SstableBuilderOptions {
        capacity: 128 << 20,
        block_capacity: 64 << 10,
        estimated_output_key_count: Some(key_count),
        ..Default::default()
    };
    let mut builder = SstableBuilder::new(
        1_u64,
        InMemWriter::from(&options),
        BlockedXor16FilterBuilder::create(options.filter_builder_options()),
        options,
        CompactionCatalogAgent::for_test(vec![TableId::default()]),
        None,
    );
    for index in 0..key_count {
        let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
        key.resize(key_len - 8, b'k');
        key.extend_from_slice(&(index as u64).to_be_bytes());
        let key = FullKey::for_test(TableId::default(), key, test_epoch(1));
        builder
            .add(key.to_ref(), HummockValue::put(&[42; 16]))
            .await
            .unwrap();
    }
    let (bytes, meta) = builder.finish().await.unwrap().writer_output;
    let meta = SstableMeta::decode(&bytes[meta.meta_offset as usize..]).unwrap();
    let sst = Box::new(Sstable::new(1.into(), meta, false));
    assert!(sst.has_filter());
    let mut payload = Vec::new();
    sst.encode(&mut payload).unwrap();
    let decoded = Box::<Sstable>::decode(&mut &payload[..]).unwrap();
    assert_eq!(decoded.id, sst.id);
    assert_eq!(decoded.meta, sst.meta);
    assert_eq!(
        decoded.filter_reader.encode_to_bytes(),
        sst.filter_reader.encode_to_bytes()
    );
    payload
}

fn bench_sstable_meta(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("sstable_meta_decode");
    for (name, key_count, key_len) in [
        ("filter_heavy", 1_100_000, 64),
        ("long_keys", 160_000, 4096),
    ] {
        // Build, native-decode, encode and check the fixture before Criterion starts timing.
        let payload = runtime.block_on(encoded_sstable(key_count, key_len));
        eprintln!(
            "fixture={name} encoded_bytes={} xxh64={:016x}",
            payload.len(),
            xxhash_rust::xxh64::xxh64(&payload, 0)
        );
        group.throughput(Throughput::Bytes(payload.len() as u64));
        group.bench_function(BenchmarkId::new(name, payload.len()), |b| {
            // Match SstableStore's HybridCache value type, including filter reconstruction.
            // PerIteration drops each decoded SST after timing, bounding live output memory.
            // This measures warm-input decoding, excluding cache lookup, I/O and checksums.
            b.iter_batched(
                || (),
                |()| Box::<Sstable>::decode(&mut black_box(&payload[..])).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_sstable_meta
}
criterion_main!(benches);
