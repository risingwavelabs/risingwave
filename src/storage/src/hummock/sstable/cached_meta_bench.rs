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

//! A local, explicitly ignored benchmark of the actual cache representation and partition code.
//! Optimize the storage crate when running this test, and retain stdout as the round record.

use std::hint::black_box;
use std::io::Write;
use std::ops::Bound;
use std::path::PathBuf;
use std::time::Instant;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::FullKey;

use super::{BlockMeta, SerdeSstable, Sstable, SstableMeta, VERSION, xxhash64_checksum};

fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state ^ (*state >> 33)
}

fn mock_blocks() -> Vec<BlockMeta> {
    let lengths = |count: usize, total: usize, first: usize| {
        let base = (total - first) / (count - 1);
        let remainder = (total - first) % (count - 1);
        std::iter::once(first).chain((0..count - 1).map(move |i| base + usize::from(i < remainder)))
    };
    let sizes: Vec<usize> = lengths(2187, 462231, 52)
        .chain(lengths(9596, 1486418441, 165652))
        .collect();
    let mut result = Vec::with_capacity(sizes.len());
    let mut rng = 7505;
    let mut run = 0u32;
    while result.len() < sizes.len() {
        let start = result.len();
        let mut end = (start + 97 + random(&mut rng) as usize % 161).min(sizes.len());
        if start < 2187 && end > 2187 {
            end = 2187;
        }
        let mut group: Vec<_> = sizes[start..end]
            .iter()
            .enumerate()
            .map(|(row, &len)| {
                let mut key = vec![b'x'; len - 12];
                key[..2].copy_from_slice(&[0, 1]);
                key[2..6].copy_from_slice(&run.to_be_bytes());
                let suffix = key.len() - 4;
                key[suffix..].copy_from_slice(&(row as u32).to_be_bytes());
                FullKey::for_test(TableId::new(7505), key, test_epoch(100)).encode()
            })
            .collect();
        group.sort(); // Every fabricated key has the same epoch.
        result.extend(group.into_iter().map(|smallest_key| BlockMeta {
            smallest_key,
            len: 128,
            uncompressed_size: 256,
            total_key_count: 1,
            ..Default::default()
        }));
        run += 1;
    }
    assert_eq!(
        result.iter().map(|b| b.smallest_key.len()).sum::<usize>(),
        1486880672
    );
    result
}

#[test]
#[ignore = "local 1.49GB synthetic cache/lookup benchmark; optimize risingwave_storage"]
fn benchmark_cached_meta_k16() {
    let output_dir = PathBuf::from(
        std::env::var_os("RW_META_BENCH_OUTPUT_DIR")
            .expect("set RW_META_BENCH_OUTPUT_DIR to retain raw latency samples"),
    );
    std::fs::create_dir_all(&output_dir).unwrap();
    let blocks = mock_blocks();
    let raw_bytes = blocks.capacity() * std::mem::size_of::<BlockMeta>()
        + blocks
            .iter()
            .map(|b| b.smallest_key.capacity())
            .sum::<usize>()
        + blocks.first().unwrap().smallest_key.len()
        + blocks.last().unwrap().smallest_key.len();
    let input = SstableMeta {
        block_metas: blocks.clone(),
        smallest_key: blocks.first().unwrap().smallest_key.clone(),
        largest_key: blocks.last().unwrap().smallest_key.clone(),
        ..Default::default()
    };
    let start = Instant::now();
    let sstable = Sstable::new(1.into(), input, false);
    let construct_ns = start.elapsed().as_nanos();
    println!(
        "cache_construct_ns={construct_ns} baseline_meta_heap_bytes={raw_bytes} cached_meta_heap_bytes={} cache_weight_bytes={}",
        sstable.meta.estimated_heap_size(),
        sstable.estimated_meta_cache_memory_weight()
    );
    // A separate compressed source avoids warming either candidate's key storage while
    // constructing targets. Its memory and all target construction are outside seek timing.
    let source = sstable.meta.clone();
    let mut target = Vec::with_capacity(165652);
    let mut scratch = Vec::with_capacity(165652);
    for (index, block) in blocks.iter().enumerate() {
        assert_eq!(
            source.block_smallest_key(index, &mut target),
            block.smallest_key
        );
        assert_eq!(
            sstable.meta.partition_point(
                0..blocks.len(),
                Bound::Included(FullKey::decode(&target)),
                &mut scratch
            ),
            index + 1
        );
    }
    let mut rng = 457158129;
    let mut queries = Vec::with_capacity(20000);
    for class in 0..2 {
        for _ in 0..10000 {
            let index = random(&mut rng) as usize % blocks.len();
            source.block_smallest_key(index, &mut target);
            if class == 1 {
                let epoch = target.len() - 8;
                let newer = u64::from_be_bytes(target[epoch..].try_into().unwrap()) + 1;
                target[epoch..].copy_from_slice(&newer.to_be_bytes());
            }
            let expected = blocks
                .partition_point(|b| FullKey::decode(&b.smallest_key) <= FullKey::decode(&target));
            if class == 1 && expected > 0 {
                assert_ne!(
                    FullKey::decode(&blocks[expected - 1].smallest_key),
                    FullKey::decode(&target)
                );
            }
            queries.push((index, class, expected));
        }
    }
    // Fisher-Yates shuffling keeps the fixed exact/missing query counts and sequence shared.
    for i in (1..queries.len()).rev() {
        let j = random(&mut rng) as usize % (i + 1);
        queries.swap(i, j);
    }
    for round in 0..3 {
        let order = if round == 1 {
            [true, false]
        } else {
            [false, true]
        };
        for cached in order {
            let mut samples = Vec::with_capacity(queries.len());
            for (iteration, &(index, class, expected)) in
                queries.iter().cycle().take(22000).enumerate()
            {
                source.block_smallest_key(index, &mut target);
                if class == 1 {
                    let epoch = target.len() - 8;
                    let newer = u64::from_be_bytes(target[epoch..].try_into().unwrap()) + 1;
                    target[epoch..].copy_from_slice(&newer.to_be_bytes());
                }
                let key = black_box(FullKey::decode(&target));
                let start = Instant::now();
                let found = if cached {
                    sstable.meta.partition_point(
                        0..blocks.len(),
                        Bound::Included(key),
                        &mut scratch,
                    )
                } else {
                    blocks.partition_point(|b| FullKey::decode(&b.smallest_key) <= key)
                };
                black_box(found);
                let ns = start.elapsed().as_nanos() as u64;
                assert_eq!(found, expected);
                if iteration >= 2000 {
                    samples.push((index, class, target.len(), ns));
                }
            }
            let mut output = std::io::BufWriter::new(
                std::fs::File::create(
                    output_dir.join(format!("lookup_cached_{cached}_round_{round}.csv")),
                )
                .unwrap(),
            );
            writeln!(output, "source_index,class,target_bytes,ns").unwrap();
            for &(index, class, len, ns) in &samples {
                writeln!(output, "{index},{class},{len},{ns}").unwrap();
            }
            output.flush().unwrap();
            for class in [0, 1, 2] {
                let mut ns: Vec<_> = samples
                    .iter()
                    .filter(|s| class == 2 || s.1 == class)
                    .map(|s| s.3)
                    .collect();
                ns.sort_unstable();
                let p99 = ns[(ns.len() * 99).div_ceil(100) - 1];
                println!(
                    "lookup round={round} cached={cached} class={class} count={} p99_ns={p99} scratch_capacity={}",
                    ns.len(),
                    if cached { scratch.capacity() } else { 0 }
                );
            }
        }
    }
}

// Generate fixtures in a separate process from benchmark_decode_cached_meta_k16. In
// particular, constructing the raw key collection must not affect measured RSS peaks.
#[test]
#[ignore = "writes two 1.49GB synthetic old-wire fixtures for the cold-decode experiment"]
fn generate_cached_meta_decode_fixtures() {
    let dir = PathBuf::from(std::env::var_os("RW_META_DECODE_DIR").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let blocks = mock_blocks();
    let key_digest = blocks.iter().fold(0u64, |digest, block| {
        digest.rotate_left(1) ^ xxhash64_checksum(&block.smallest_key)
    });
    let input = SstableMeta {
        smallest_key: blocks.first().unwrap().smallest_key.clone(),
        largest_key: blocks.last().unwrap().smallest_key.clone(),
        block_metas: blocks,
        version: VERSION,
        ..Default::default()
    };
    std::fs::write(dir.join("native.bin"), input.encode_to_bytes()).unwrap();
    let mut output = std::io::BufWriter::new(std::fs::File::create(dir.join("foyer.bin")).unwrap());
    bincode::serialize_into(
        &mut output,
        &SerdeSstable {
            id: 1.into(),
            meta: input,
        },
    )
    .unwrap();
    output.flush().unwrap();
    std::fs::write(
        dir.join("expected.txt"),
        format!("11783\n1486880672\n{key_digest}\n"),
    )
    .unwrap();
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied rusage on success, checked below.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    // SAFETY: the successful call above initialized usage.
    let usage = unsafe { usage.assume_init() };
    let scale = if cfg!(target_os = "macos") { 1 } else { 1024 };
    usage.ru_maxrss as u64 * scale
}

#[cfg(unix)]
fn current_rss_bytes() -> u64 {
    let pid = std::process::id().to_string();
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .unwrap();
    assert!(output.status.success());
    std::str::from_utf8(&output.stdout)
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap()
        * 1024
}

/// Execute this exact ignored test once per fresh process and mode. The parent runner
/// selects order/rounds and retains stdout; no fixture generation occurs in these children.
#[cfg(unix)]
#[test]
#[ignore = "isolated-process 1.49GB synthetic native/Foyer cold-decode benchmark"]
fn benchmark_decode_cached_meta_k16() {
    let dir = PathBuf::from(std::env::var_os("RW_META_DECODE_DIR").unwrap());
    let mode = std::env::var("RW_META_DECODE_MODE").unwrap();
    let fixture = match mode.as_str() {
        "native-baseline" | "native-direct" => "native.bin",
        "foyer-baseline" | "foyer-direct" => "foyer.bin",
        _ => panic!("unknown decode mode"),
    };
    let expected: Vec<u64> = std::fs::read_to_string(dir.join("expected.txt"))
        .unwrap()
        .split_whitespace()
        .map(|value| value.parse().unwrap())
        .collect();
    assert_eq!(expected.len(), 3);
    let startup_peak = peak_rss_bytes();
    let read_start = Instant::now();
    let input = std::fs::read(dir.join(fixture)).unwrap();
    // Touch the resident input outside the decode timer, including the final partial page.
    for page in input.chunks(4096) {
        black_box(page[0]);
    }
    let read_ns = read_start.elapsed().as_nanos();
    let pre_rss = current_rss_bytes();
    let pre_peak = peak_rss_bytes();
    let start = Instant::now();
    let sstable = match mode.as_str() {
        "native-baseline" => Box::new(Sstable::new(
            1.into(),
            SstableMeta::decode(&input).unwrap(),
            false,
        )),
        "native-direct" => Box::new(Sstable::from_meta_bytes(1.into(), &input, false).unwrap()),
        "foyer-baseline" => {
            let legacy: SerdeSstable = bincode::deserialize_from(&mut &input[..]).unwrap();
            Box::new(Sstable::new(legacy.id, legacy.meta, false))
        }
        "foyer-direct" => {
            use foyer::Code;
            Box::<Sstable>::decode(&mut &input[..]).unwrap()
        }
        _ => unreachable!(),
    };
    black_box(&sstable);
    let decode_ns = start.elapsed().as_nanos();
    let decode_peak = peak_rss_bytes();
    let post_rss = current_rss_bytes();

    // Check all reconstructed keys with bounded scratch after capturing measurement peaks.
    // Do not reserialize or build another full metadata representation in the measured child.
    let mut scratch = Vec::new();
    let mut key_bytes = 0usize;
    let mut key_digest = 0u64;
    for index in 0..sstable.block_count() {
        let key = sstable.meta.block_smallest_key(index, &mut scratch);
        key_bytes += key.len();
        key_digest = key_digest.rotate_left(1) ^ xxhash64_checksum(key);
    }
    assert_eq!(sstable.block_count() as u64, expected[0]);
    assert_eq!(key_bytes as u64, expected[1]);
    assert_eq!(key_digest, expected[2]);
    assert!(!sstable.has_filter());
    println!(
        "decode_measurement mode={mode} input_bytes={} read_and_touch_ns={read_ns} \
         decode_ns={decode_ns} startup_peak_rss_bytes={startup_peak} \
         pre_decode_rss_bytes={pre_rss} pre_decode_peak_rss_bytes={pre_peak} \
         decode_peak_rss_bytes={decode_peak} post_decode_rss_bytes={post_rss} \
         cached_meta_heap_bytes={} cache_value_weight_bytes={} verification=passed",
        input.len(),
        sstable.meta.estimated_heap_size(),
        sstable.estimated_meta_cache_memory_weight(),
    );
    black_box(&input);
    black_box(&sstable);
}
