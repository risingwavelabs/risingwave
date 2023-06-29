// Copyright 2023 RisingWave Labs
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

/// If <https://github.com/tikv/jemallocator/issues/22> is resolved, we may inline this
#[macro_export]
macro_rules! enable_jemalloc_on_unix {
    () => {
        #[cfg(unix)]
        #[global_allocator]
        static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
    };
}

#[macro_export]
macro_rules! enable_task_local_jemalloc_on_unix {
    () => {
        #[cfg(unix)]
        #[global_allocator]
        static GLOBAL: task_stats_alloc::TaskLocalAlloc<tikv_jemallocator::Jemalloc> =
            task_stats_alloc::TaskLocalAlloc(tikv_jemallocator::Jemalloc);
    };
}

/// Estimate the aligned size for a struct allocated by jemalloc.
///
/// The estimated result is expected to be equal with `tikv_jemallocator::usable_size`. It's
/// designed as a substitute of `tikv_jemallocator::usable_size` because the latter one would panic
/// if the passed pointer is not in heap.
///
/// This estimation assumes 4 KB page size, 16-byte quantum in 64-bit system, which is the most
/// common cases in 64-bit Linux.
///
/// See also: https://jemalloc.net/jemalloc.3.html#size_classes
pub fn aligned_size(size: usize) -> usize {
    sz_s2u_compute(size)
}

// Below are copied from Jemalloc source code: jemalloc/include/jemalloc/internal/sz.h
//
// See also:
// https://github.com/jemalloc/jemalloc/blob/521970fb2e5278b7b92061933cbacdbb9478998a/include/jemalloc/internal/sz.h#L261

fn pow2_ceil_u64(x: u64) -> u64 {
    if x <= 1 {
        return x;
    }
    let msb_on_index = fls_u64(x - 1);
    // Range-check; it's on the callers to ensure that the result of this
    // call won't overflow.
    assert!(msb_on_index < 63);
    return 1u64 << (msb_on_index + 1);
}

fn fls_u64(x: u64) -> usize {
    assert!(x != 0);
    return (8 * 8 - 1) ^ x.leading_zeros() as usize;
}

fn lg_floor(x: u64) -> usize {
    assert!(x != 0);
    return fls_u64(x);
}

fn sz_s2u_compute(mut size: usize) -> usize {
    if size == 0 {
        size = 1;
    }

    if size <= (1 << 3) {
        let lg_tmin = 3 - 1 + 1;
        let lg_ceil = lg_floor(pow2_ceil_u64(size as u64));
        return if lg_ceil < lg_tmin {
            1 << lg_tmin
        } else {
            1 << lg_ceil
        };
    } else {
        let x = lg_floor(((size << 1 as i64) - 1) as u64);
        let lg_delta = if x < 2 + 4 + 1 { 4 } else { x - 2 - 1 };
        let delta = 1 << lg_delta;
        let delta_mask = delta - 1;
        return (size + delta_mask) & !delta_mask;
    }
}

// TODO: add tests to compare between jemalloc and this
