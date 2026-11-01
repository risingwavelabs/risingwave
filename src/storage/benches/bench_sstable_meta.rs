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

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use bytes::Buf;
use foyer::Code;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::sstable::{
    BlockedXor16FilterBuilder, FilterBuilder, InMemWriter, Sstable, SstableBuilder,
    SstableBuilderOptions, SstableMeta,
};
use risingwave_storage::hummock::value::HummockValue;
use twox_hash::XxHash64;

// Recompile only this bench with the variable set for a separate allocation pass.
// The false constant removes all counter/TLS operations from the timing binary.
const ALLOCATION_PASS: bool = option_env!("RW_META_BENCH_ALLOC").is_some();

#[derive(Clone, Copy, Default)]
struct Allocations {
    calls: u64,
    reallocs: u64,
    requested: u64,
    live: i64,
    peak: i64,
    minimum: i64,
}

thread_local! {
    static ALLOCATIONS: Cell<Option<Allocations>> = const { Cell::new(None) };
}

fn count(update: impl FnOnce(&mut Allocations)) {
    if ALLOCATION_PASS {
        let _ = ALLOCATIONS.try_with(|slot| {
            if let Some(mut counts) = slot.get() {
                update(&mut counts);
                counts.peak = counts.peak.max(counts.live);
                counts.minimum = counts.minimum.min(counts.live);
                slot.set(Some(counts));
            }
        });
    }
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            count(|c| {
                c.calls += 1;
                c.requested += layout.size() as u64;
                c.live += layout.size() as i64;
            });
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            count(|c| {
                c.calls += 1;
                c.requested += layout.size() as u64;
                c.live += layout.size() as i64;
            });
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        count(|c| c.live -= layout.size() as i64);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            count(|c| {
                c.calls += 1;
                c.reallocs += 1;
                c.requested += size as u64;
                c.live += size as i64 - layout.size() as i64;
            });
        }
        ptr
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn start_allocations() {
    ALLOCATIONS.with(|slot| {
        assert!(slot.replace(Some(Allocations::default())).is_none());
    });
}

fn finish_allocations() -> Allocations {
    let counts = ALLOCATIONS.with(|slot| slot.take().unwrap());
    assert_eq!(counts.live, 0, "decoded objects must be dropped in scope");
    assert_eq!(counts.minimum, 0, "deallocation crossed measurement scope");
    counts
}

fn check_allocator() {
    // An allocating worker must not contaminate the measuring thread's counters.
    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = barrier.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        worker_barrier.wait();
        for _ in 0..32 {
            drop(black_box(vec![0_u8; 4096]));
        }
        worker_barrier.wait();
    });
    // Initialize any OS synchronization state before enabling measurement.
    barrier.wait();
    start_allocations();
    barrier.wait();
    drop(black_box(Vec::<u8>::with_capacity(128)));
    barrier.wait();
    let counts = finish_allocations();
    worker.join().unwrap();
    assert_eq!((counts.calls, counts.requested, counts.peak), (1, 128, 128));

    start_allocations();
    let mut bytes = Vec::<u8>::with_capacity(64);
    black_box(&mut bytes);
    bytes.resize(4096, 1);
    black_box(&bytes);
    drop(bytes);
    let counts = finish_allocations();
    assert_eq!((counts.calls, counts.reallocs), (2, 1));
    assert_eq!((counts.requested, counts.peak), (64 + 4096, 4096));
    eprintln!("allocator_preflight=passed thread_isolation=true realloc=true");
}

fn thread_cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // CLOCK_THREAD_CPUTIME_ID excludes other threads and time descheduled.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    assert_eq!(result, 0, "thread CPU clock unavailable");
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

struct Input {
    name: &'static str,
    payload: Vec<u8>,
    value_len: usize,
    checksum: u64,
}

fn key(index: usize, key_len: usize) -> FullKey<Vec<u8>> {
    let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
    key.resize(key_len - 8, b'k');
    key.extend_from_slice(&(index as u64).to_be_bytes());
    FullKey::for_test(TableId::default(), key, 1 << 16)
}

async fn build_sst(path: &Path, key_count: usize, key_len: usize) {
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
        builder
            .add(key(index, key_len).to_ref(), HummockValue::put(&[42; 16]))
            .await
            .unwrap();
    }
    let output = builder.finish().await.unwrap();
    let (bytes, meta) = output.writer_output;
    assert_eq!(
        meta,
        SstableMeta::decode(&bytes[meta.meta_offset as usize..]).unwrap()
    );
    std::fs::write(path, bytes).unwrap();
}

fn load_sst(path: &Path, name: &'static str, key_count: usize, key_len: usize) -> Input {
    let bytes = std::fs::read(path).unwrap();
    let offset = (&bytes[bytes.len() - 24..bytes.len() - 16]).get_u64_le() as usize;
    let meta = SstableMeta::decode(&bytes[offset..]).unwrap();
    let filter_bytes = meta.bloom_filter.len();
    let key_bytes: usize = meta.block_metas.iter().map(|b| b.smallest_key.len()).sum();
    let blocks = meta.block_metas.len();
    let sst = Sstable::new(1.into(), meta, false);
    for index in [0, key_count / 2, key_count - 1] {
        let key = key(index, key_len);
        let hash = Sstable::hash_for_filter(&key.user_key.encode(), 0);
        let range = (
            Bound::Included(key.user_key.as_ref()),
            Bound::Included(key.user_key.as_ref()),
        );
        assert!(sst.may_match_hash(&range, hash));
    }
    let mut payload = Vec::new();
    sst.encode(&mut payload).unwrap();
    let value_len = payload.len();
    sst.id.encode(&mut payload).unwrap();
    let cache_path = path.with_extension("meta-cache");
    if cache_path.exists() {
        assert_eq!(
            payload,
            std::fs::read(cache_path).unwrap(),
            "cache wire changed"
        );
    } else {
        std::fs::write(cache_path, &payload).unwrap();
    }
    let decoded = Sstable::decode(&mut &payload[..value_len]).unwrap();
    assert_eq!(decoded.meta, sst.meta);
    assert_eq!(
        decoded.filter_reader.encode_to_bytes(),
        sst.filter_reader.encode_to_bytes()
    );
    eprintln!(
        "fixture={name} sst_bytes={} payload_bytes={} filter_bytes={filter_bytes} block_key_bytes={key_bytes} blocks={blocks} roundtrip=passed",
        bytes.len(),
        payload.len()
    );
    let checksum = XxHash64::oneshot(0, &payload);
    Input {
        name,
        payload,
        value_len,
        checksum,
    }
}

// Match SstableStore's actual HybridCache<ObjectId, Box<Sstable>> value type.
// Uncompressed entry CPU path: checksum, real SST Code decoder, cache key.
fn decode(input: &Input) -> (Box<Sstable>, HummockSstableObjectId) {
    assert_eq!(
        XxHash64::oneshot(0, black_box(&input.payload)),
        input.checksum
    );
    let sst = Box::<Sstable>::decode(&mut &input.payload[..input.value_len]).unwrap();
    let key = HummockSstableObjectId::decode(&mut &input.payload[input.value_len..]).unwrap();
    (sst, key)
}

fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

fn main() {
    if ALLOCATION_PASS {
        check_allocator();
    }
    let cpu_start = thread_cpu_ns();
    assert!(thread_cpu_ns() >= cpu_start);
    let dir = std::env::var_os("RW_META_BENCH_FIXTURE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/sstable-meta-fixtures"));
    std::fs::create_dir_all(&dir).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let inputs: Vec<_> = [
        ("short_1100k", 1_100_000, 64),
        ("short_1300k", 1_300_000, 64),
        ("long_160k", 160_000, 4096),
    ]
    .into_iter()
    .map(|(name, count, len)| {
        let path = dir.join(format!("{name}.sst"));
        if !path.exists() {
            runtime.block_on(build_sst(&path, count, len));
        }
        load_sst(&path, name, count, len)
    })
    .collect();
    drop(runtime);
    if std::env::var_os("RW_META_BENCH_CHECK_ONLY").is_some() {
        return;
    }
    let n = env_number("RW_META_BENCH_ITERATIONS", 64);
    let rounds = env_number("RW_META_BENCH_ROUNDS", 7);
    let first_round = env_number("RW_META_BENCH_FIRST_ROUND", 0);
    let variant = std::env::var("RW_META_BENCH_VARIANT").unwrap_or_else(|_| "current".into());
    println!(
        "fixture,variant,pass,round,n,payload_bytes,wall_ns,thread_cpu_ns,alloc_calls,realloc_calls,requested_bytes,peak_live_bytes,returned_live_bytes"
    );
    for input in &inputs {
        for _ in 0..8 {
            drop(black_box(decode(input)));
        }
        for round in first_round..first_round + rounds {
            let (mut wall, mut cpu) = (0_u64, 0_u64);
            let (mut calls, mut reallocs, mut requested, mut peak, mut returned) =
                (0_u64, 0_u64, 0_u64, 0_i64, 0_i64);
            for _ in 0..n {
                if ALLOCATION_PASS {
                    start_allocations();
                    let output = black_box(decode(input));
                    returned += ALLOCATIONS.with(|slot| slot.get().unwrap().live);
                    drop(output);
                    let counts = finish_allocations();
                    calls += counts.calls;
                    reallocs += counts.reallocs;
                    requested += counts.requested;
                    peak += counts.peak;
                } else {
                    let cpu_start = thread_cpu_ns();
                    let start = Instant::now();
                    let output = black_box(decode(input));
                    wall += start.elapsed().as_nanos() as u64;
                    cpu += thread_cpu_ns() - cpu_start;
                    drop(output);
                }
            }
            if ALLOCATION_PASS {
                println!(
                    "{},{},allocation,{round},{n},{},,,{:.3},{:.3},{:.3},{:.3},{:.3}",
                    input.name,
                    variant,
                    input.payload.len(),
                    calls as f64 / n as f64,
                    reallocs as f64 / n as f64,
                    requested as f64 / n as f64,
                    peak as f64 / n as f64,
                    returned as f64 / n as f64
                );
            } else {
                println!(
                    "{},{},timing,{round},{n},{},{:.3},{:.3},,,,,",
                    input.name,
                    variant,
                    input.payload.len(),
                    wall as f64 / n as f64,
                    cpu as f64 / n as f64
                );
            }
        }
    }
}
