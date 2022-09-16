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

use std::env;
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_object_store::object::{ObjectStore, ObjectStoreImpl, S3ObjectStore};
use risingwave_storage::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    BatchSstableWriterFactory, CachePolicy, CompressionAlgorithm, HummockResult, MemoryLimiter,
    SstableBuilder, SstableBuilderOptions, SstableStore, SstableWriterFactory,
    SstableWriterOptions, StreamingSstableWriterFactory, TieredCache,
};
use risingwave_storage::monitor::ObjectStoreMetrics;

const RANGE: Range<u64> = 0..2500000;
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
    type Writer = <F as SstableWriterFactory>::Writer;

    async fn open_builder(&self) -> HummockResult<SstableBuilder<Self::Writer>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self.limiter.require_memory(1).await.unwrap();
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .writer_factory
            .create_sst_writer(id, writer_options)
            .unwrap();
        let builder = SstableBuilder::new_for_test(id, writer, self.options.clone());

        Ok(builder)
    }
}

fn get_builder_options(capacity_mb: usize) -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: capacity_mb * 1024 * 1024,
        block_capacity: 1024 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.01,
        compression_algorithm: CompressionAlgorithm::None,
    }
}

async fn build_tables<F: SstableWriterFactory>(
    mut builder: CapacitySplitTableBuilder<LocalTableBuilderFactory<F>>,
) {
    for i in RANGE {
        builder
            .add_user_key(i.to_be_bytes().to_vec(), HummockValue::put(VALUE), 1)
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
    let object_store = runtime.block_on(async {
        S3ObjectStore::new(bucket.to_string(), metrics.clone())
            .await
            .monitored(metrics)
    });
    let object_store = Arc::new(ObjectStoreImpl::S3(object_store));
    let sstable_store = Arc::new(SstableStore::new(
        object_store,
        "test".to_string(),
        64 << 20,
        128 << 20,
        TieredCache::none(),
    ));

    let mut group = c.benchmark_group("bench_multi_builder");
    group
        .sample_size(SAMPLE_COUNT)
        .measurement_time(ESTIMATED_MEASUREMENT_TIME);
    if enable_streaming_upload {
        group.bench_function(format!("bench_streaming_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(CapacitySplitTableBuilder::new_for_test(
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
                build_tables(CapacitySplitTableBuilder::new_for_test(
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
