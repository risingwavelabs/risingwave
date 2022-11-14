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

use std::io::Write;

use bytes::BufMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

const TABLES_PER_SSTABLE: u32 = 10;
const KEYS_PER_TABLE: u64 = 100;

fn gen_dataset(vsize: usize) -> Vec<Vec<u8>> {
    let mut dataset = vec![];
    let mut rng = StdRng::seed_from_u64(0);
    for t in 1..=TABLES_PER_SSTABLE {
        for i in 1..=KEYS_PER_TABLE {
            let mut v = vec![0; vsize];
            rng.fill(&mut v[..]);
            let mut buf = vec![];
            buf.put_u32(t); // table id
            buf.put_u64(i); // key
            buf.put_u32(0); // cell idx
            buf.put_slice(&v); // value
            dataset.push(buf)
        }
    }
    dataset
}

fn gen_data(dataset: &[Vec<u8>]) -> Vec<u8> {
    let mut data = vec![];
    for entry in dataset.iter() {
        data.put_slice(entry);
    }
    data
}

fn block_compression(data: Vec<u8>) -> Vec<u8> {
    let mut encoder = lz4::EncoderBuilder::new().level(4).build(vec![]).unwrap();
    encoder.write_all(&data).unwrap();
    let (buf, result) = encoder.finish();
    result.unwrap();
    buf
}

fn stream_compression(dataset: Vec<Vec<u8>>) -> Vec<u8> {
    let buf = vec![];
    let mut encoder = lz4::EncoderBuilder::new().level(4).build(buf).unwrap();
    for entry in dataset {
        encoder.write_all(&entry).unwrap();
    }
    let (buf, result) = encoder.finish();
    result.unwrap();
    buf
}

fn bench_compression(c: &mut Criterion) {
    for vsize in [8, 16, 32, 64] {
        let dataset = gen_dataset(vsize);
        let data = gen_data(&dataset);

        c.bench_with_input(
            BenchmarkId::new(format!("buffer - vsize: {}B", vsize), ""),
            &dataset,
            |b, dataset| b.iter(|| gen_data(dataset)),
        );

        c.bench_with_input(
            BenchmarkId::new(format!("block compression - vsize: {}B", vsize), ""),
            &data,
            |b, data| b.iter(|| block_compression(data.clone())),
        );

        c.bench_with_input(
            BenchmarkId::new(format!("stream compression - vsize: {}B", vsize), ""),
            &dataset,
            |b, dataset| b.iter(|| stream_compression(dataset.clone())),
        );

        let uncompressed = data.len();
        let block_compressed = block_compression(data).len();
        let stream_compressed = stream_compression(dataset).len();

        println!("uncompressed size: {}", uncompressed);
        println!(
            "block compressed size: {}, rate: {:.3}",
            block_compressed,
            block_compressed as f64 / uncompressed as f64
        );
        println!(
            "stream compressed size: {}, rate: {:.3}",
            stream_compressed,
            stream_compressed as f64 / uncompressed as f64
        );
    }
}

criterion_group!(benches, bench_compression);
criterion_main!(benches);
