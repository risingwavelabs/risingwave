// Copyright 2025 RisingWave Labs
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

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use itertools::Itertools;
use lru::LruCache;
use risingwave_common::lru::LruCache as RwLruCache;
use risingwave_common::sequence::SEQUENCE_GLOBAL;

fn lru(loops: usize, _evict_ratio: u64) -> (usize, Duration) {
    let mut lru = LruCache::unbounded();
    let evicted = 0;
    let now = Instant::now();
    for i in 0..loops as u64 {
        // Changed back to use the official lru-rs crate, so there is no update_epoch anymore.
        // Keep the code for reference.
        // if i % evict_ratio == 0 && i != 0 {
        //     lru.update_epoch(i);
        //     while lru.pop_lru_by_epoch(i).is_some() {
        //         evicted += 1;
        //     }
        // }
        lru.put(i, i);
    }

    (evicted, now.elapsed())
}

fn rw_lru(loops: usize, evict_ratio: u64) -> (usize, Duration) {
    let mut lru = RwLruCache::unbounded();
    let mut evicted = 0;
    let now = Instant::now();
    for i in 0..loops as u64 {
        if i % evict_ratio == 0 {
            let sequence = SEQUENCE_GLOBAL.load(Ordering::Relaxed);
            while lru.pop_with_sequence(sequence).is_some() {
                evicted += 1;
            }
        }
        lru.put(i, i);
    }

    (evicted, now.elapsed())
}

fn benchmark<F>(name: &str, threads: usize, loops: usize, f: F)
where
    F: Fn() -> (usize, Duration) + Clone + Send + 'static,
{
    let handles = (0..threads)
        .map(|_| std::thread::spawn(black_box(f.clone())))
        .collect_vec();
    let mut dur = Duration::from_nanos(0);
    let mut evicted = 0;
    for handle in handles {
        let (e, d) = handle.join().unwrap();
        evicted += e;
        dur += d;
    }
    println!(
        "{:20} {} threads {} loops: {:?} per iter, total evicted: {}",
        name,
        threads,
        loops,
        Duration::from_nanos((dur.as_nanos() / threads as u128 / loops as u128) as u64),
        evicted,
    );
}

fn main() {
    for threads in [1, 4, 8, 16, 32, 64] {
        println!();
        benchmark("lru - 1024", threads, 1000000, || lru(1000000, 1024));
        benchmark("rw  - 1024", threads, 1000000, || rw_lru(1000000, 1024));
    }
}
