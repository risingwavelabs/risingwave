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

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::io::AsyncReadExt;

const BENCH_SIZE: usize = 100;
fn gen_test_payload() -> Vec<u8> {
    let mut ret = Vec::new();
    for i in 0..10000 {
        ret.extend(format!("{:05}", i).as_bytes());
    }
    ret
}

async fn run_tokio_fs(payload: &[u8]) {
    use tempfile::tempdir;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let temp_dir = tempdir().unwrap();

    for i in 0..BENCH_SIZE {
        let mut path = PathBuf::from(temp_dir.path());
        path.push(format!("{}.txt", i));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(payload).await.unwrap();
        file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut buffer = Vec::with_capacity(payload.len());
        file.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, payload);
    }
}

fn run_std_fs(payload: &[u8]) {
    use tempfile::tempdir;
    let temp_dir = tempdir().unwrap();

    for i in 0..BENCH_SIZE {
        let mut path = PathBuf::from(temp_dir.path());
        path.push(format!("{}.txt", i));
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.write_all(payload).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut buffer = Vec::with_capacity(payload.len());
        file.read_to_end(&mut buffer).unwrap();
        assert_eq!(buffer, payload);
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let payload = gen_test_payload();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    c.bench_with_input(
        BenchmarkId::new("fs_operation", "tokio"),
        &payload,
        |b, payload| b.iter(|| runtime.block_on(run_tokio_fs(&payload))),
    );
    c.bench_with_input(
        BenchmarkId::new("fs_operation", "std"),
        &payload,
        |b, payload| b.iter(|| run_std_fs(&payload)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
