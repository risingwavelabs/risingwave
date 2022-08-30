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
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_object_store::object::{ObjectStore, ObjectStoreImpl, S3ObjectStore};
use risingwave_storage::hummock::multi_builder::{
    CapacitySplitTableBuilder, LocalTableBuilderFactory,
};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    CompressionAlgorithm, SstableBuilderOptions, SstableStore, SstableStoreWrite, TieredCache,
};
use risingwave_storage::monitor::ObjectStoreMetrics;

const RANGE: Range<u64> = 0..2500000;
const VALUE: &[u8] = &[0; 400];
const SAMPLE_COUNT: usize = 10;
const ESTIMATED_MEASUREMENT_TIME: Duration = Duration::from_secs(60);

fn get_builder_options(
    capacity_mb: usize,
    enable_sst_streaming_upload: bool,
) -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: capacity_mb * 1024 * 1024,
        block_capacity: 1024 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.01,
        compression_algorithm: CompressionAlgorithm::None,
        estimate_bloom_filter_capacity: 1024 * 1024,
        enable_sst_streaming_upload,
    }
}

fn get_builder(
    sstable_store: Arc<dyn SstableStoreWrite>,
    options: SstableBuilderOptions,
) -> CapacitySplitTableBuilder<LocalTableBuilderFactory> {
    CapacitySplitTableBuilder::new_for_test(LocalTableBuilderFactory::new(
        1,
        sstable_store,
        options,
    ))
}

async fn build_tables(mut builder: CapacitySplitTableBuilder<LocalTableBuilderFactory>) {
    for i in RANGE {
        builder
            .add_user_key(i.to_be_bytes().to_vec(), HummockValue::put(VALUE), 1)
            .await
            .unwrap();
    }
    let split_table_outputs = builder.finish().unwrap();
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
        S3ObjectStore::new(bucket.to_string(), 256, metrics.clone())
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
                build_tables(get_builder(
                    sstable_store.clone(),
                    get_builder_options(capacity_mb, true),
                ))
            })
        });
    } else {
        group.bench_function(format!("bench_batch_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(get_builder(
                    sstable_store.clone(),
                    get_builder_options(capacity_mb, false),
                ))
            })
        });
    }
    group.finish();
}

// SST size: 32, 64, 128, 256MiB
fn bench_multi_builder(c: &mut Criterion) {
    let sst_capacities = vec![32, 64, 128, 256];
    let bucket = env::var("S3_BUCKET").unwrap();
    for capacity in sst_capacities {
        bench_builder(c, &bucket, capacity, false);
        bench_builder(c, &bucket, capacity, true);
    }
}

criterion_group!(benches, bench_multi_builder);
criterion_main!(benches);
