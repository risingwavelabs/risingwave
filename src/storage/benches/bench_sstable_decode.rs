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

//! Criterion for native metadata and Block restore; same final owned objects.

#[path = "sstable_decode_fixture/resources.rs"]
mod resources;
#[allow(dead_code)]
mod sstable_meta_fixture;

use std::hint::black_box;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use foyer::Code;
use resources::measure;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::sstable::{
    Block, BlockBuilder, BlockBuilderOptions, BlockedXor8FilterBuilder, BlockedXor16FilterBuilder,
    CompressionAlgorithm, Sstable, SstableMeta, Xor16FilterBuilder,
};
use serde::{Deserialize, Deserializer};

struct LegacyBlock(Block);
impl<'de> Deserialize<'de> for LegacyBlock {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let data: Vec<u8> = serde_bytes::deserialize(d)?;
        Ok(Self(Block::decode_from_raw_legacy_for_bench(Bytes::from(
            data,
        ))))
    }
}
fn assert_block(a: &Block, b: &Block) {
    assert_eq!(a.raw(), b.raw());
    assert_eq!(a.table_id(), b.table_id());
    assert_eq!(a.data(), b.data());
    assert_eq!(a.restart_point_len(), b.restart_point_len());
    for i in 0..a.restart_point_len() {
        assert_eq!(a.restart_point(i), b.restart_point(i));
    }
}
fn block_cases(c: &mut Criterion, label: &str, raw: Bytes) {
    let old = Block::decode_from_raw_legacy_for_bench(raw.clone());
    let new = Block::decode_from_raw(raw.clone());
    assert_block(&old, &new);
    let wire = bincode::serialize(&old).unwrap();
    assert_block(
        &new,
        &bincode::deserialize_from::<_, LegacyBlock>(&mut &wire[..])
            .unwrap()
            .0,
    );
    eprintln!(
        "FIXTURE name={label} raw_bytes={} restarts={} xxh64={:016x}",
        raw.len(),
        old.restart_point_len(),
        xxhash_rust::xxh64::xxh64(&raw, 0)
    );
    let mut group = c.benchmark_group("rw_block_restore");
    measure(&mut group, &format!("{label}_raw_old"), || {
        Block::decode_from_raw_legacy_for_bench(black_box(raw.clone()))
    });
    measure(&mut group, &format!("{label}_raw_new"), || {
        Block::decode_from_raw(black_box(raw.clone()))
    });
    measure(&mut group, &format!("{label}_code_old"), || {
        bincode::deserialize_from::<_, LegacyBlock>(&mut black_box(&wire[..])).unwrap()
    });
    measure(&mut group, &format!("{label}_code_new"), || {
        <Block as Code>::decode(&mut black_box(&wire[..])).unwrap()
    });
    group.finish();
}
fn producer_block(target: usize, mixed: bool) -> Bytes {
    let mut builder = BlockBuilder::new(BlockBuilderOptions {
        capacity: target,
        compression_algorithm: CompressionAlgorithm::None,
        restart_interval: 16,
    });
    for i in 0_u64.. {
        let mut key = i.to_be_bytes().to_vec();
        key.resize(if mixed && (i / 16) % 2 == 1 { 320 } else { 64 }, b'k');
        let key = FullKey::for_test(TableId::default(), key, 1);
        let value = vec![42; if mixed && (i / 16) % 2 == 1 { 300 } else { 16 }];
        builder.add_for_test(key.to_ref(), &value);
        if builder.approximate_len() >= target {
            break;
        }
    }
    let native = Bytes::copy_from_slice(builder.build());
    let block = Block::decode(native.clone(), native.len()).unwrap();
    Bytes::copy_from_slice(block.raw())
}
fn meta_case(c: &mut Criterion, label: &str, data: Bytes, meta: SstableMeta, blocks: bool) {
    let native = data.slice(meta.meta_offset as usize..);
    let baseline = Sstable::new(1_u64.into(), SstableMeta::decode(&native).unwrap(), false);
    let candidate = Sstable::from_meta_bytes(1_u64.into(), &native, false).unwrap();
    assert_eq!(baseline.id, candidate.id);
    assert_eq!(baseline.meta, candidate.meta);
    assert_eq!(
        baseline.filter_reader.encode_to_bytes(),
        candidate.filter_reader.encode_to_bytes()
    );
    assert_eq!(
        bincode::serialize(&baseline).unwrap(),
        bincode::serialize(&candidate).unwrap()
    );
    eprintln!(
        "FIXTURE name={label} keys={} key_len={} blocks={} filter_bytes={} native_meta_bytes={} filter_xxh64={:016x} native_xxh64={:016x}",
        meta.key_count,
        meta.smallest_key.len(),
        meta.block_metas.len(),
        meta.bloom_filter.len(),
        native.len(),
        xxhash_rust::xxh64::xxh64(&meta.bloom_filter, 0),
        xxhash_rust::xxh64::xxh64(&native, 0)
    );
    let mut group = c.benchmark_group("rw_native_meta");
    measure(&mut group, &format!("{label}_old"), || {
        Sstable::new(
            black_box(1_u64.into()),
            SstableMeta::decode(black_box(&native)).unwrap(),
            false,
        )
    });
    measure(&mut group, &format!("{label}_new"), || {
        Sstable::from_meta_bytes(black_box(1_u64.into()), black_box(&native), false).unwrap()
    });
    group.finish();
    if blocks {
        let b = &meta.block_metas[0];
        let b = Block::decode(
            data.slice(b.offset as usize..(b.offset + b.len) as usize),
            b.uncompressed_size as usize,
        )
        .unwrap();
        block_cases(c, label, Bytes::copy_from_slice(b.raw()));
    }
}
fn bench_sstable_decode(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for (scenario, count, key_len) in [
        ("ordinary", 16_000, 64),
        ("large_filter", 1_100_000, 64),
        ("large_key", 160_000, 4096),
    ] {
        let (data, meta) = runtime.block_on(sstable_meta_fixture::sstable_fixture::<
            Xor16FilterBuilder,
        >(count, key_len));
        meta_case(c, &format!("{scenario}_plain16"), data, meta, false);
        let (data, meta) = runtime.block_on(sstable_meta_fixture::sstable_fixture::<
            BlockedXor16FilterBuilder,
        >(count, key_len));
        meta_case(
            c,
            &format!("{scenario}_blocked16"),
            data,
            meta,
            scenario != "large_filter",
        );
        let (data, meta) = runtime.block_on(sstable_meta_fixture::sstable_fixture::<
            BlockedXor8FilterBuilder,
        >(count, key_len));
        meta_case(c, &format!("{scenario}_blocked8"), data, meta, false);
    }
    block_cases(c, "small4k", producer_block(4 << 10, false));
    block_cases(c, "mixed64k", producer_block(64 << 10, true));
}
criterion_group! {
    name=benches;
    config=Criterion::default().sample_size(20).warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets=bench_sstable_decode
}
criterion_main!(benches);
