// Copyright 2026 RisingWave Labs
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

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, stream};
use risingwave_batch_executors::error::{BatchError, Result};
use risingwave_batch_executors::task::ShutdownToken;
use risingwave_batch_executors::{
    BatchPipelineOperator, BatchPipelineOperatorFactory, BoxedDataChunkStream,
    MorselPipelineDriver, ParallelMorselPipelineDriver, PipelineOperatorOutput, PushContext,
    PushSink, PushStatus, StreamMorselSource,
};
use risingwave_common::array::DataChunk;
use risingwave_common::enable_jemalloc;
use tokio::runtime::Runtime;

enable_jemalloc!();

#[derive(Default)]
struct BlackBoxSink {
    rows: usize,
    chunks: usize,
}

impl PushSink for BlackBoxSink {
    fn push<'a>(&'a mut self, chunk: DataChunk) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            self.rows += chunk.cardinality();
            self.chunks += 1;
            black_box((self.rows, self.chunks));
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<PushStatus>> {
        async move {
            black_box((self.rows, self.chunks));
            Ok(PushStatus::NeedMoreInput)
        }
        .boxed()
    }
}

struct IdentityOperator;

impl BatchPipelineOperator for IdentityOperator {
    fn execute<'a>(
        &'a mut self,
        chunk: DataChunk,
    ) -> BoxFuture<'a, Result<PipelineOperatorOutput>> {
        async move { Ok(PipelineOperatorOutput::one(chunk)) }.boxed()
    }
}

struct IdentityOperatorFactory;

impl BatchPipelineOperatorFactory for IdentityOperatorFactory {
    fn create(&self) -> Box<dyn BatchPipelineOperator> {
        Box::new(IdentityOperator)
    }
}

fn create_stream(chunk_size: usize, chunk_num: usize) -> BoxedDataChunkStream {
    stream::iter((0..chunk_num).map(move |_| Ok::<_, BatchError>(DataChunk::new_dummy(chunk_size))))
        .boxed()
}

fn create_context(parallelism: usize) -> PushContext {
    PushContext::new(ShutdownToken::empty())
        .with_morsel_budget(64)
        .with_morsel_queue_capacity(64)
        .with_morsel_parallelism(parallelism)
}

fn create_source_only_driver(
    chunk_size: usize,
    chunk_num: usize,
) -> MorselPipelineDriver<StreamMorselSource> {
    let source = StreamMorselSource::new(create_stream(chunk_size, chunk_num));
    MorselPipelineDriver::source_only(source, create_context(1))
}

fn create_identity_driver(
    chunk_size: usize,
    chunk_num: usize,
) -> MorselPipelineDriver<StreamMorselSource> {
    let source = StreamMorselSource::new(create_stream(chunk_size, chunk_num));
    MorselPipelineDriver::new(source, vec![Box::new(IdentityOperator)], create_context(1))
}

fn create_parallel_identity_driver(
    chunk_size: usize,
    chunk_num: usize,
    parallelism: usize,
) -> ParallelMorselPipelineDriver<StreamMorselSource> {
    let source = StreamMorselSource::new(create_stream(chunk_size, chunk_num));
    ParallelMorselPipelineDriver::new(
        source,
        vec![Arc::new(IdentityOperatorFactory)],
        create_context(parallelism),
    )
}

fn bench_morsel_pipeline_driver(c: &mut Criterion) {
    const TOTAL_SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("morsel_pipeline_driver");

    for chunk_size in [128, 1024, 4096] {
        let chunk_num = TOTAL_SIZE / chunk_size;

        group.bench_with_input(
            BenchmarkId::new("source_only", chunk_size),
            &chunk_size,
            |b, &chunk_size| {
                b.to_async(&rt).iter_batched(
                    || create_source_only_driver(chunk_size, chunk_num),
                    |mut driver| async move {
                        let mut sink = BlackBoxSink::default();
                        black_box(driver.execute(&mut sink).await.unwrap());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("identity_operator", chunk_size),
            &chunk_size,
            |b, &chunk_size| {
                b.to_async(&rt).iter_batched(
                    || create_identity_driver(chunk_size, chunk_num),
                    |mut driver| async move {
                        let mut sink = BlackBoxSink::default();
                        black_box(driver.execute(&mut sink).await.unwrap());
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("parallel_identity_operator", chunk_size),
            &chunk_size,
            |b, &chunk_size| {
                b.to_async(&rt).iter_batched(
                    || create_parallel_identity_driver(chunk_size, chunk_num, 4),
                    |driver| async move {
                        let mut sink = BlackBoxSink::default();
                        black_box(driver.execute(&mut sink).await.unwrap());
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_morsel_pipeline_driver);
criterion_main!(benches);
