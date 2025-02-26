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

use std::cell::RefCell;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use itertools::Itertools;
use risingwave_common::sequence::*;

thread_local! {
    pub static SEQUENCER_64_8: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(64, 64 * 8)) };
    pub static SEQUENCER_64_16: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(64, 64 * 16)) };
    pub static SEQUENCER_64_32: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(64, 64 * 32)) };
    pub static SEQUENCER_128_8: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(128, 128 * 8)) };
    pub static SEQUENCER_128_16: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(128, 128 * 16)) };
    pub static SEQUENCER_128_32: RefCell<Sequencer> = const { RefCell::new(Sequencer::new(128, 128 * 32)) };
}

fn coarse(loops: usize) -> Duration {
    let now = Instant::now();
    for _ in 0..loops {
        let _ = coarsetime::Instant::now();
    }
    now.elapsed()
}

#[expect(clippy::explicit_counter_loop)]
fn primitive(loops: usize) -> Duration {
    let mut cnt = 0usize;
    let now = Instant::now();
    for _ in 0..loops {
        cnt += 1;
        let _ = cnt;
    }
    now.elapsed()
}

fn atomic(loops: usize, atomic: Arc<AtomicUsize>) -> Duration {
    let now = Instant::now();
    for _ in 0..loops {
        let _ = atomic.fetch_add(1, Ordering::Relaxed);
    }
    now.elapsed()
}

fn atomic_skip(loops: usize, atomic: Arc<AtomicUsize>, skip: usize) -> Duration {
    let mut cnt = 0usize;
    let now = Instant::now();
    for _ in 0..loops {
        cnt += 1;
        let _ = cnt;
        if cnt % skip == 0 {
            let _ = atomic.fetch_add(skip, Ordering::Relaxed);
        } else {
            let _ = atomic.load(Ordering::Relaxed);
        }
    }
    now.elapsed()
}

fn sequencer(loops: usize, step: Sequence, lag_amp: Sequence) -> Duration {
    let sequencer = match (step, lag_amp) {
        (64, 8) => &SEQUENCER_64_8,
        (64, 16) => &SEQUENCER_64_16,
        (64, 32) => &SEQUENCER_64_32,
        (128, 8) => &SEQUENCER_128_8,
        (128, 16) => &SEQUENCER_128_16,
        (128, 32) => &SEQUENCER_128_32,
        _ => unimplemented!(),
    };
    let now = Instant::now();
    for _ in 0..loops {
        let _ = sequencer.with(|s| s.borrow_mut().alloc());
    }
    now.elapsed()
}

fn benchmark<F>(name: &str, threads: usize, loops: usize, f: F)
where
    F: Fn() -> Duration + Clone + Send + 'static,
{
    let handles = (0..threads)
        .map(|_| std::thread::spawn(black_box(f.clone())))
        .collect_vec();
    let mut dur = Duration::from_nanos(0);
    for handle in handles {
        dur += handle.join().unwrap();
    }
    println!(
        "{:20} {} threads {} loops: {:?} per iter",
        name,
        threads,
        loops,
        Duration::from_nanos((dur.as_nanos() / threads as u128 / loops as u128) as u64)
    );
}

fn main() {
    for (threads, loops) in [
        (1, 10_000_000),
        (4, 10_000_000),
        (8, 10_000_000),
        (16, 10_000_000),
        (32, 10_000_000),
    ] {
        println!();

        benchmark("primitive", threads, loops, move || primitive(loops));

        let a = Arc::new(AtomicUsize::new(0));
        benchmark("atomic", threads, loops, move || atomic(loops, a.clone()));

        let a = Arc::new(AtomicUsize::new(0));
        benchmark("atomic skip 8", threads, loops, move || {
            atomic_skip(loops, a.clone(), 8)
        });

        let a = Arc::new(AtomicUsize::new(0));
        benchmark("atomic skip 16", threads, loops, move || {
            atomic_skip(loops, a.clone(), 16)
        });

        let a = Arc::new(AtomicUsize::new(0));
        benchmark("atomic skip 32", threads, loops, move || {
            atomic_skip(loops, a.clone(), 32)
        });

        let a = Arc::new(AtomicUsize::new(0));
        benchmark("atomic skip 64", threads, loops, move || {
            atomic_skip(loops, a.clone(), 64)
        });

        benchmark("sequencer(64,8)", threads, loops, move || {
            sequencer(loops, 64, 8)
        });
        benchmark("sequencer(64,16)", threads, loops, move || {
            sequencer(loops, 64, 16)
        });
        benchmark("sequencer(64,32)", threads, loops, move || {
            sequencer(loops, 64, 32)
        });
        benchmark("sequencer(128,8)", threads, loops, move || {
            sequencer(loops, 128, 8)
        });
        benchmark("sequencer(128,16)", threads, loops, move || {
            sequencer(loops, 128, 16)
        });
        benchmark("sequencer(128,32)", threads, loops, move || {
            sequencer(loops, 128, 32)
        });

        benchmark("coarse", threads, loops, move || coarse(loops));
    }
}
