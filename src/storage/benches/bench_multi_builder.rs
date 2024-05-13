// Copyright 2024 RisingWave Labs
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

use std::env;
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{EvictionConfig, MetricLevel, ObjectStoreConfig};
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_object_store::object::{ObjectStore, ObjectStoreImpl, S3ObjectStore};
use risingwave_storage::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    BatchSstableWriterFactory, CachePolicy, FileCache, HummockResult, MemoryLimiter,
    SstableBuilder, SstableBuilderOptions, SstableStore, SstableStoreConfig, SstableWriterFactory,
    SstableWriterOptions, StreamingSstableWriterFactory, Xor16FilterBuilder,
};
use risingwave_storage::monitor::{global_hummock_state_store_metrics, ObjectStoreMetrics};

const RANGE: Range<u64> = 0..1500000;
const VALUE: &[u8] = &[0; 400];
const SAMPLE_COUNT: usize = 10;
const ESTIMATED_MEASUREMENT_TIME: Duration = Duration::from_secs(60);

struct LocalTableBuilderFactory<F: SstableWriterFactory> {
    next_id: AtomicU64,
    writer_factory: F,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    limiter: MemoryLimiter,
}

impl<F: SstableWriterFactory> LocalTableBuilderFactory<F> {
    pub fn new(next_id: u64, writer_factory: F, options: SstableBuilderOptions) -> Self {
        Self {
            next_id: AtomicU64::new(next_id),
            writer_factory,
            options,
            policy: CachePolicy::NotFill,
            limiter: MemoryLimiter::new(1000000),
        }
    }
}

#[async_trait::async_trait]
impl<F: SstableWriterFactory> TableBuilderFactory for LocalTableBuilderFactory<F> {
    type Filter = Xor16FilterBuilder;
    type Writer = <F as SstableWriterFactory>::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self.limiter.require_memory(1).await;
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .writer_factory
            .create_sst_writer(id, writer_options)
            .await
            .unwrap();
        let builder = SstableBuilder::for_test(id, writer, self.options.clone());

        Ok(builder)
    }
}

fn get_builder_options(capacity_mb: usize) -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: capacity_mb * 1024 * 1024,
        block_capacity: 1024 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    }
}

fn test_user_key_of(idx: u64) -> UserKey<Vec<u8>> {
    UserKey::for_test(TableId::default(), idx.to_be_bytes().to_vec())
}

async fn build_tables<F: SstableWriterFactory>(
    mut builder: CapacitySplitTableBuilder<LocalTableBuilderFactory<F>>,
) {
    for i in RANGE {
        builder
            .add_full_key_for_test(
                FullKey::from_user_key(test_user_key_of(i).as_ref(), 1),
                HummockValue::put(VALUE),
                true,
            )
            .await
            .unwrap();
    }
    let split_table_outputs = builder.finish().await.unwrap();
    let join_handles = split_table_outputs
        .into_iter()
        .map(|o| o.upload_join_handle)
        .collect_vec();
    try_join_all(join_handles).await.unwrap();
}

fn bench_builder(
    c: &mut Criterion,
    bucket: &str,
    capacity_mb: usize,
    enable_streaming_upload: bool,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let metrics = Arc::new(ObjectStoreMetrics::unused());
    let default_config = Arc::new(ObjectStoreConfig::default());
    let object_store = runtime.block_on(async {
        S3ObjectStore::new_with_config(bucket.to_string(), metrics.clone(), default_config.clone())
            .await
            .monitored(metrics, default_config)
    });
    let object_store = Arc::new(ObjectStoreImpl::S3(object_store));
    let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
        store: object_store,
        path: "test".to_string(),
        block_cache_capacity: 64 << 20,
        meta_cache_capacity: 128 << 20,
        meta_cache_shard_num: 2,
        block_cache_shard_num: 2,
        block_cache_eviction: EvictionConfig::for_test(),
        meta_cache_eviction: EvictionConfig::for_test(),
        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,
        data_file_cache: FileCache::none(),
        meta_file_cache: FileCache::none(),
        recent_filter: None,
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Disabled)),
    }));

    let mut group = c.benchmark_group("bench_multi_builder");
    group
        .sample_size(SAMPLE_COUNT)
        .measurement_time(ESTIMATED_MEASUREMENT_TIME);
    if enable_streaming_upload {
        group.bench_function(format!("bench_streaming_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(CapacitySplitTableBuilder::for_test(
                    LocalTableBuilderFactory::new(
                        1,
                        StreamingSstableWriterFactory::new(sstable_store.clone()),
                        get_builder_options(capacity_mb),
                    ),
                ))
            })
        });
    } else {
        group.bench_function(format!("bench_batch_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(CapacitySplitTableBuilder::for_test(
                    LocalTableBuilderFactory::new(
                        1,
                        BatchSstableWriterFactory::new(sstable_store.clone()),
                        get_builder_options(capacity_mb),
                    ),
                ))
            })
        });
    }
    group.finish();
}

// SST size: 4, 32, 64, 128, 256MiB
fn bench_multi_builder(c: &mut Criterion) {
    let sst_capacities = vec![4, 32, 64, 128, 256];
    let bucket = env::var("S3_BUCKET").unwrap();
    for capacity in sst_capacities {
        bench_builder(c, &bucket, capacity, false);
        bench_builder(c, &bucket, capacity, true);
    }
}

criterion_group!(benches, bench_multi_builder);
criterion_main!(benches);
