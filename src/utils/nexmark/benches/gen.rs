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
use nexmark::event::EventType;
use nexmark::EventGenerator;

criterion_group!(benches, nexmark_generator);
criterion_main!(benches);

fn nexmark_generator(c: &mut Criterion) {
    c.bench_function("nexmark_generate_event", |bencher| {
        let mut gen = EventGenerator::default();
        bencher.iter(|| gen.next())
    });
    c.bench_function("nexmark_generate_person", |bencher| {
        let mut gen = EventGenerator::default().with_type_filter(EventType::Person);
        bencher.iter(|| gen.next())
    });
    c.bench_function("nexmark_generate_auction", |bencher| {
        let mut gen = EventGenerator::default().with_type_filter(EventType::Auction);
        bencher.iter(|| gen.next())
    });
    c.bench_function("nexmark_generate_bid", |bencher| {
        let mut gen = EventGenerator::default().with_type_filter(EventType::Bid);
        bencher.iter(|| gen.next())
    });
}
