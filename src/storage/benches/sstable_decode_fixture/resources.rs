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
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{BatchSize, BenchmarkGroup};

struct CountedSystem;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
fn count(size: usize) {
    if COUNT.load(Ordering::Relaxed) {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(size as u64, Ordering::Relaxed);
    }
}
unsafe impl GlobalAlloc for CountedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count(size);
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[global_allocator]
static ALLOCATOR: CountedSystem = CountedSystem;

fn cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    ((usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as u64) * 1_000_000_000
        + ((usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as u64) * 1000
}

pub fn measure<T>(group: &mut BenchmarkGroup<'_, WallTime>, name: &str, mut f: impl FnMut() -> T) {
    for _ in 0..32 {
        drop(black_box(f()));
    }
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    COUNT.store(true, Ordering::Relaxed);
    let value = black_box(f());
    COUNT.store(false, Ordering::Relaxed);
    let allocs = ALLOCS.load(Ordering::Relaxed);
    let bytes = BYTES.load(Ordering::Relaxed);
    drop(value);
    let cpu_start = cpu_ns();
    let start = Instant::now();
    let mut iterations = 0_u64;
    loop {
        for _ in 0..64 {
            drop(black_box(f()));
        }
        iterations += 64;
        if start.elapsed() >= Duration::from_millis(250) {
            break;
        }
    }
    let wall = start.elapsed().as_nanos();
    let cpu = cpu_ns() - cpu_start;
    eprintln!(
        "RESOURCE case={name} iterations={iterations} wall_ns={wall} cpu_ns={cpu} allocations={allocs} allocated_bytes={bytes}"
    );
    group.bench_function(name, |b| {
        b.iter_batched(|| (), |()| f(), BatchSize::PerIteration);
    });
}
