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

use std::fmt::{Display, Formatter};
use std::future::Future;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
#[cfg(target_os = "macos")]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::BoxFuture;
use futures::FutureExt;
#[cfg(target_os = "macos")]
use libc::F_NOCACHE;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::spawn_blocking;

#[derive(Default)]
struct BenchStats {
    total_time_millis: u128,
    total_count: usize,
}

impl Display for BenchStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "op {} times, avg time: {} ms",
            self.total_count,
            self.total_time_millis as f64 / self.total_count as f64
        )
    }
}

struct Timer<'a> {
    stats: &'a mut BenchStats,
    start_time: SystemTime,
}

impl Drop for Timer<'_> {
    fn drop(&mut self) {
        let end_time = SystemTime::now();
        let duration = end_time
            .duration_since(self.start_time)
            .unwrap()
            .as_millis();
        self.stats.total_time_millis += duration;
        self.stats.total_count += 1;
    }
}

impl BenchStats {
    fn start_timer(&mut self) -> Timer<'_> {
        Timer {
            stats: self,
            start_time: SystemTime::now(),
        }
    }
}

const BENCH_SIZE: usize = 10;
const PAYLOAD_SIZE: usize = 1_000_000;
fn gen_test_payload() -> Vec<u8> {
    let mut ret = Vec::new();
    for i in 0..PAYLOAD_SIZE {
        ret.extend(format!("{:08}", i).as_bytes());
    }
    ret
}

fn gen_tokio_files(path: &Path) -> impl IntoIterator<Item = impl Future<Output = tokio::fs::File>> {
    let path = path.to_path_buf();
    (0..BENCH_SIZE).map(move |i| {
        let file_path = path.join(format!("{}.txt", i));
        async move {
            #[cfg(target_os = "macos")]
            let ret = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .custom_flags(F_NOCACHE)
                .open(file_path)
                .await
                .unwrap();

            #[cfg(not(target_os = "macos"))]
            let ret = tokio::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path)
                .await
                .unwrap();
            ret
        }
    })
}

fn run_tokio_bench<R>(c: &mut Criterion, name: &str, path: &Path, mut func: R)
where
    R: for<'b> FnMut(tokio::fs::File, &'b mut BenchStats, &'b Vec<u8>) -> BoxFuture<'b, ()>,
{
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut stats = BenchStats::default();
    let payload = Arc::new(gen_test_payload());

    c.bench_function(format!("fs_operation/tokio/{}", name).as_str(), |b| {
        b.iter(|| {
            runtime.block_on(async {
                for file_future in gen_tokio_files(path) {
                    let file = file_future.await;
                    func(file, &mut stats, payload.as_ref()).await;
                }
            });
        })
    });

    println!("Bench tokio {}: {}", name, stats);
}

fn run_tokio_bench_read<R>(c: &mut Criterion, name: &str, path: &Path, func: R)
where
    R: Fn(tokio::fs::File) -> BoxFuture<'static, Vec<u8>> + Sync + Send + Clone + 'static,
{
    run_tokio_bench(c, name, path, move |file, stats, payload| {
        let func = func.clone();
        async move {
            let buffer = {
                let _timer = stats.start_timer();
                func(file).await
            };
            assert_eq!(buffer, *payload);
        }
        .boxed()
    })
}

fn criterion_tokio(c: &mut Criterion) {
    let payload_size = gen_test_payload().len();
    let tokio_path = TempDir::new().unwrap().into_path();
    run_tokio_bench(c, "write", &tokio_path, |mut file, stats, payload| {
        async move {
            let _timer = stats.start_timer();
            file.write_all(payload).await.unwrap();
        }
        .boxed()
    });
    run_tokio_bench_read(c, "read", &tokio_path, move |mut file| {
        async move {
            let mut buffer = Vec::with_capacity(payload_size);
            file.read_to_end(&mut buffer).await.unwrap();
            buffer
        }
        .boxed()
    });
    run_tokio_bench_read(c, "blocking-read", &tokio_path, move |file| {
        async move {
            let mut file = file.into_std().await;
            spawn_blocking(move || {
                let mut buffer = Vec::with_capacity(payload_size);
                file.read_to_end(&mut buffer).unwrap();
                buffer
            })
            .await
            .unwrap()
        }
        .boxed()
    });
    run_tokio_bench_read(c, "blocking-pread", &tokio_path, move |file| {
        async move {
            let file = file.into_std().await;
            spawn_blocking(move || {
                let mut buffer = vec![0; payload_size];
                file.read_exact_at(&mut buffer, 0).unwrap();
                buffer
            })
            .await
            .unwrap()
        }
        .boxed()
    });
}

fn gen_std_files(path: &Path) -> impl IntoIterator<Item = std::fs::File> {
    let path = path.to_path_buf();
    (0..BENCH_SIZE).map(move |i| {
        let file_path = path.join(format!("{}.txt", i));
        #[cfg(target_os = "macos")]
        let ret = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(F_NOCACHE)
            .open(file_path)
            .unwrap();

        #[cfg(not(target_os = "macos"))]
        let ret = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .unwrap();
        ret
    })
}

fn run_std_bench<R>(c: &mut Criterion, name: &str, path: &Path, mut func: R)
where
    R: FnMut(std::fs::File, &mut BenchStats, &Vec<u8>),
{
    let mut stats = BenchStats::default();
    let payload = Arc::new(gen_test_payload());

    c.bench_function(format!("fs_operation/std/{}", name).as_str(), |b| {
        b.iter(|| {
            for file in gen_std_files(path) {
                func(file, &mut stats, payload.as_ref());
            }
        });
    });

    println!("Bench std {}: {}", name, stats);
}

fn criterion_std(c: &mut Criterion) {
    let std_path = TempDir::new().unwrap().into_path();
    run_std_bench(c, "write", &std_path, |mut file, stats, payload| {
        let _timer = stats.start_timer();
        file.write_all(payload).unwrap();
    });
    run_std_bench(c, "read", &std_path, |mut file, stats, payload| {
        let mut buffer = Vec::with_capacity(payload.len());
        {
            let _timer = stats.start_timer();
            file.read_to_end(&mut buffer).unwrap();
        }
        assert_eq!(buffer, *payload);
    });
    run_std_bench(c, "pread", &std_path, |file, stats, payload| {
        let mut buffer = vec![0; payload.len()];
        {
            let _timer = stats.start_timer();
            file.read_exact_at(&mut buffer, 0).unwrap();
        }
        assert_eq!(buffer, *payload);
    });
}

criterion_group!(benches, criterion_tokio, criterion_std);
criterion_main!(benches);
