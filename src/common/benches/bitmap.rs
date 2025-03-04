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

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapIter};

fn bench_bitmap(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    let bytes = vec![0b00110011; CHUNK_SIZE / 8];
    let zeros = Bitmap::zeros(CHUNK_SIZE);
    let ones = Bitmap::ones(CHUNK_SIZE);
    let sparse = Bitmap::from_bytes(&[0x01; CHUNK_SIZE / 8]); // 1/16 set
    let dense = Bitmap::from_bytes(&[0x7f; CHUNK_SIZE / 8]); // 15/16 set
    let x = sparse.clone();
    let y = dense.clone();
    let i = 0x123;
    c.bench_function("zeros", |b| b.iter(|| Bitmap::zeros(CHUNK_SIZE)));
    c.bench_function("ones", |b| b.iter(|| Bitmap::ones(CHUNK_SIZE)));
    c.bench_function("from_bytes", |b| b.iter(|| Bitmap::from_bytes(&bytes)));
    c.bench_function("get_from_ones", |b| b.iter(|| ones.is_set(i)));
    c.bench_function("get", |b| b.iter(|| x.is_set(i)));
    c.bench_function("and", |b| b.iter(|| &x & &y));
    c.bench_function("or", |b| b.iter(|| &x | &y));
    c.bench_function("not", |b| b.iter(|| !&x));
    c.bench_function("eq", |b| b.iter(|| x == sparse));
    c.bench_function("iter", |b| b.iter(|| iter_all(x.iter())));
    c.bench_function("iter_on_ones", |b| b.iter(|| iter_all(ones.iter())));
    c.bench_function("iter_ones_on_zeros", |b| {
        b.iter(|| iter_all(zeros.iter_ones()))
    });
    c.bench_function("iter_ones_on_ones", |b| {
        b.iter(|| iter_all(ones.iter_ones()))
    });
    c.bench_function("iter_ones_on_sparse", |b| {
        b.iter(|| iter_all(sparse.iter_ones()))
    });
    c.bench_function("iter_ones_on_dense", |b| {
        b.iter(|| iter_all(dense.iter_ones()))
    });
    // target for iter_ones_on_ones
    c.bench_function("iter_range", |b| b.iter(|| iter_all(0..CHUNK_SIZE)));
}

/// Bench ~10 million records
fn bench_bitmap_iter(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    const N_CHUNKS: usize = 10_000;
    fn make_iterators(bitmaps: &[Bitmap]) -> Vec<BitmapIter<'_>> {
        bitmaps.iter().map(|bitmap| bitmap.iter()).collect_vec()
    }
    fn bench_bitmap_iter_inner(bench_id: &str, bitmap: Bitmap, c: &mut Criterion) {
        let bitmaps = vec![bitmap; N_CHUNKS];
        let make_iters = || make_iterators(&bitmaps);
        c.bench_function(bench_id, |b| {
            b.iter_batched(
                make_iters,
                |iters| {
                    for iter in iters {
                        for bit_flag in iter {
                            black_box(bit_flag);
                        }
                    }
                },
                BatchSize::SmallInput,
            )
        });
    }
    let zeros = Bitmap::zeros(CHUNK_SIZE);
    bench_bitmap_iter_inner("zeros_iter", zeros, c);
    let ones = Bitmap::ones(CHUNK_SIZE);
    bench_bitmap_iter_inner("ones_iter", ones, c);
}

/// Consume the iterator with blackbox.
fn iter_all(iter: impl Iterator) {
    for x in iter {
        black_box(x);
    }
}

criterion_group!(benches, bench_bitmap, bench_bitmap_iter);
criterion_main!(benches);
