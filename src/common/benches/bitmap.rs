// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::buffer::Bitmap;

fn bench_bitmap(c: &mut Criterion) {
    const CHUNK_SIZE: usize = 1024;
    let x = Bitmap::zeros(CHUNK_SIZE);
    let y = Bitmap::ones(CHUNK_SIZE);
    let i = 0x123;
    c.bench_function("zeros", |b| b.iter(|| Bitmap::zeros(CHUNK_SIZE)));
    c.bench_function("ones", |b| b.iter(|| Bitmap::ones(CHUNK_SIZE)));
    c.bench_function("get", |b| b.iter(|| x.is_set(i)));
    c.bench_function("and", |b| b.iter(|| &x & &y));
    c.bench_function("or", |b| b.iter(|| &x | &y));
    c.bench_function("not", |b| b.iter(|| !&x));
}

criterion_group!(benches, bench_bitmap);
criterion_main!(benches);
