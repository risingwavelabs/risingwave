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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

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

fn make_key(sst_id: u64, block_idx: u64) -> Bytes {
    let mut key = BytesMut::with_capacity(16);
    key.put_u64_le(sst_id);
    key.put_u64_le(block_idx);
    key.freeze()
}

#[async_trait]
pub trait CacheBase: Sync + Send {
    async fn try_get_with(&self, sst_id: u64, block_idx: u64) -> HummockResult<Arc<Block>>;
}

pub struct MokaCache {
    inner: Cache<Bytes, Arc<Block>>,
}

impl MokaCache {
    pub fn new(capacity: usize) -> Self {
        let cache: Cache<Bytes, Arc<Block>> = Cache::builder()
            .initial_capacity(capacity / 16)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }
}

#[async_trait]
impl CacheBase for MokaCache {
    async fn try_get_with(&self, sst_id: u64, block_idx: u64) -> HummockResult<Arc<Block>> {
        let k = make_key(sst_id, block_idx);
        self.inner
            .try_get_with(k, async move {
                match get_fake_block(sst_id, block_idx).await {
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
}

impl LruCacheImpl {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(LruCache::new(3, capacity, 1024, false)),
        }
    }
}

#[async_trait]
impl CacheBase for LruCacheImpl {
    async fn try_get_with(&self, sst_id: u64, block_idx: u64) -> HummockResult<Arc<Block>> {
        let mut hasher = DefaultHasher::new();
        let key = (sst_id, block_idx);
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        let h = hasher.finish();
        if let Some(entry) = self.inner.lookup(h, &key) {
            let block = entry.value().clone();
            return Ok(block);
        }
        let block = Arc::new(get_fake_block(sst_id, block_idx).await?);
        self.inner.insert(key, h, 1, block.clone());
        Ok(block)
    }
}

lazy_static::lazy_static! {
    static ref IO_COUNT: AtomicUsize = AtomicUsize::new(0);
}

async fn get_fake_block(sst: u64, offset: u64) -> HummockResult<Block> {
    // use std::time::Duration;
    // let stream_retry_interval = Duration::from_millis(10);
    // let mut min_interval = tokio::time::interval(stream_retry_interval);
    // IO_COUNT.fetch_add(1, Ordering::Relaxed);
    // min_interval.tick().await;
    Ok(Block { sst, offset })
}

fn bench_cache<C: CacheBase + 'static>(block_cache: Arc<C>, c: &mut Criterion) {
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
            for _ in 0..10000 {
                let sst_id = rng.next_u64() % 4096;
                let block_offset = rng.next_u64() % 2048;
                let block = cache.try_get_with(sst_id, block_offset).await.unwrap();
                assert_eq!(block.offset, block_offset);
                assert_eq!(block.sst, sst_id);
            }
            println!("100000 keys cost: {:?}", t.elapsed());
        });
        handles.push(handle);
    }
    current.block_on(async move {
        for h in handles {
            h.await.unwrap();
        }
    });
    println!("io count: {}", IO_COUNT.load(Ordering::Relaxed));

    c.bench_function("block-cache", |bencher| {
        bencher.iter(|| {
            let cache = block_cache.clone();
            let f = async move {
                let seed = 10244021u64;
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..1000 {
                    let sst_id = rng.next_u64() % 4096;
                    let block_offset = rng.next_u64() % 4096 * 4096;
                    let block = cache.try_get_with(sst_id, block_offset).await.unwrap();
                    assert_eq!(block.offset, block_offset);
                    assert_eq!(block.sst, sst_id);
                }
            };
            current.block_on(f);
        })
    });
}

fn bench_block_cache(c: &mut Criterion) {
    let block_cache = Arc::new(MokaCache::new(2048));
    bench_cache(block_cache, c);
    let block_cache = Arc::new(LruCacheImpl::new(2048));
    bench_cache(block_cache, c);
}

criterion_group!(benches, bench_block_cache);
criterion_main!(benches);
