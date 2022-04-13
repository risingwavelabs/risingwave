use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use std::time::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use moka::future::Cache;
use tokio::runtime::Builder;
use std::future::Future;
use async_trait::async_trait;
use risingwave_storage::hummock::{HummockError, HummockResult};
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

pub struct MokaCache {
    inner: Cache<Bytes, Arc<Block>>,
}

impl MokaCache {
    pub fn new (capacity: usize) -> Self {
        let cache: Cache<Bytes, Arc<Block>> = Cache::builder()
            .weigher(|_k, v: &Arc<Block>| v.len() as u32)
            .initial_capacity(capacity / 16)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }
}

#[async_trait]
pub trait CacheBase {
    async fn try_get_with(&self, sst_id: u64, block_idx: u64) -> HummockResult<Arc<Block>>;
}

#[async_trait]
impl CacheBase for MokaCache {
    async fn try_get_with(&self, sst_id: u64, block_idx: u64) -> HummockResult<Arc<Block>> {
        let k = make_key(sst_id, block_idx);
        self.inner
            .try_get_with(k, async move {
                get_fake_block(sst_id, block_idx)
            })
            .await
            .map_err(HummockError::other)
    }
}

async fn get_fake_block(sst: u64,  offset: u64) -> Block {
    let stream_retry_interval = Duration::from_millis(1);
    let mut min_interval = tokio::time::interval(stream_retry_interval);
    min_interval.tick().await;
    Block { sst, offset }
}

fn bench_block_cache(c: &mut Criterion) {
    let mut pool = Builder::new_multi_thread().worker_threads(4).build().unwrap();
}


criterion_group!(benches, bench_block_cache);
criterion_main!(benches);