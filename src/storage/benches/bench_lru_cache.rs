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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use moka::future::Cache;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use risingwave_storage::hummock::{HummockError, HummockResult, LruCache};
use tokio::runtime::{Builder, Runtime};

pub struct Block {
    sst: u64,
    offset: u64,
}

fn make_key(sst_object_id: u64, block_idx: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(16);
    key.put_u64_le(sst_object_id);
    key.put_u64_le(block_idx);
    key.freeze()
}

#[async_trait]
pub trait CacheBase: Sync + Send {
    async fn try_get_with(&self, sst_object_id: u64, block_idx: u64) -> HummockResult<Arc<Block>>;
}

pub struct MokaCache {
    inner: Cache<Bytes, Arc<Block>>,
    fake_io_latency: Duration,
}

impl MokaCache {
    pub fn new(capacity: usize, fake_io_latency: Duration) -> Self {
        let cache: Cache<Bytes, Arc<Block>> = Cache::builder()
            .initial_capacity(capacity / 16)
            .max_capacity(capacity as u64)
            .build();
        Self {
            inner: cache,
            fake_io_latency,
        }
    }
}

#[async_trait]
impl CacheBase for MokaCache {
    async fn try_get_with(&self, sst_object_id: u64, block_idx: u64) -> HummockResult<Arc<Block>> {
        let k = make_key(sst_object_id, block_idx);
        let latency = self.fake_io_latency;
        self.inner
            .try_get_with(k, async move {
                match get_fake_block(sst_object_id, block_idx, latency).await {
                    Ok(ret) => Ok(Arc::new(ret)),
                    Err(e) => Err(e),
                }
            })
            .await
            .map_err(HummockError::other)
    }
}

pub struct LruCacheImpl {
    inner: Arc<LruCache<(u64, u64), Arc<Block>>>,
    fake_io_latency: Duration,
}

impl LruCacheImpl {
    pub fn new(capacity: usize, fake_io_latency: Duration) -> Self {
        Self {
            inner: Arc::new(LruCache::new(3, capacity)),
            fake_io_latency,
        }
    }
}

#[async_trait]
impl CacheBase for LruCacheImpl {
    async fn try_get_with(&self, sst_object_id: u64, block_idx: u64) -> HummockResult<Arc<Block>> {
        let mut hasher = DefaultHasher::new();
        let key = (sst_object_id, block_idx);
        sst_object_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        let h = hasher.finish();
        let latency = self.fake_io_latency;
        let entry = self
            .inner
            .lookup_with_request_dedup(h, key, || async move {
                get_fake_block(sst_object_id, block_idx, latency)
                    .await
                    .map(|block| (Arc::new(block), 1))
            })
            .await?;
        Ok(entry.value().clone())
    }
}

static IO_COUNT: AtomicUsize = AtomicUsize::new(0);

async fn get_fake_block(sst: u64, offset: u64, io_latency: Duration) -> HummockResult<Block> {
    if !io_latency.is_zero() {
        let mut min_interval = tokio::time::interval(io_latency);
        IO_COUNT.fetch_add(1, Ordering::Relaxed);
        min_interval.tick().await;
    }
    Ok(Block { sst, offset })
}

fn bench_cache<C: CacheBase + 'static>(block_cache: Arc<C>, c: &mut Criterion, key_count: u64) {
    IO_COUNT.store(0, Ordering::Relaxed);
    let pool = Builder::new_multi_thread()
        .enable_time()
        .worker_threads(8)
        .build()
        .unwrap();
    let current = Runtime::new().unwrap();
    let mut handles = vec![];
    for i in 0..8 {
        let cache = block_cache.clone();
        let handle = pool.spawn(async move {
            let seed = 10244021u64 + i;
            let mut rng = SmallRng::seed_from_u64(seed);
            let t = Instant::now();
            for _ in 0..key_count {
                let sst_object_id = rng.next_u64() % 8;
                let block_offset = rng.next_u64() % key_count;
                let block = cache
                    .try_get_with(sst_object_id, block_offset)
                    .await
                    .unwrap();
                assert_eq!(block.offset, block_offset);
                assert_eq!(block.sst, sst_object_id);
            }
            t.elapsed()
        });
        handles.push(handle);
    }
    current.block_on(async move {
        let mut output = format!("{} keys cost time: [", key_count);
        for h in handles {
            use std::fmt::Write;
            let t = h.await.unwrap();
            write!(output, "{:?}, ", t).unwrap();
        }
        println!("{}", output);
    });
    println!("io count: {}", IO_COUNT.load(Ordering::Relaxed));

    c.bench_function("block-cache", |bencher| {
        bencher.iter(|| {
            let cache = block_cache.clone();
            let f = async move {
                let seed = 10244021u64;
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..(key_count / 100) {
                    let sst_object_id = rng.next_u64() % 1024;
                    let block_offset = rng.next_u64() % 1024;
                    let block = cache
                        .try_get_with(sst_object_id, block_offset)
                        .await
                        .unwrap();
                    assert_eq!(block.offset, block_offset);
                    assert_eq!(block.sst, sst_object_id);
                }
            };
            current.block_on(f);
        })
    });
}

fn bench_block_cache(c: &mut Criterion) {
    let block_cache = Arc::new(MokaCache::new(2048, Duration::from_millis(0)));
    bench_cache(block_cache, c, 10000);
    let block_cache = Arc::new(LruCacheImpl::new(2048, Duration::from_millis(0)));
    bench_cache(block_cache, c, 10000);

    let block_cache = Arc::new(MokaCache::new(2048, Duration::from_millis(1)));
    bench_cache(block_cache, c, 1000);
    let block_cache = Arc::new(LruCacheImpl::new(2048, Duration::from_millis(1)));
    bench_cache(block_cache, c, 1000);

    let block_cache = Arc::new(MokaCache::new(256, Duration::from_millis(10)));
    bench_cache(block_cache, c, 200);
    let block_cache = Arc::new(LruCacheImpl::new(256, Duration::from_millis(10)));
    bench_cache(block_cache, c, 200);
}

criterion_group!(benches, bench_block_cache);
criterion_main!(benches);
