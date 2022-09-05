use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use futures::StreamExt;
use risingwave_batch::executor::bench_utils::create_input;
use risingwave_batch::executor::{BoxedExecutor, ExpandExecutor};
use risingwave_common::types::DataType;
use tikv_jemallocator::Jemalloc;
use tokio::runtime::Runtime;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_expand_executor(
    column_subsets: Vec<Vec<usize>>,
    chunk_size: usize,
    chunk_num: usize,
) -> BoxedExecutor {
    let input_types = &[DataType::Int32, DataType::Int64, DataType::Varchar];
    let input = create_input(input_types, chunk_size, chunk_num);
    ExpandExecutor::create(input, column_subsets)
}

async fn execute_expand_executor(executor: BoxedExecutor) {
    let mut stream = executor.execute();
    while let Some(ret) = stream.next().await {
        black_box(ret.unwrap());
    }
}

fn bench_expand(c: &mut Criterion) {
    const SIZE: usize = 1024 * 1024;
    let rt = Runtime::new().unwrap();
    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("ExpandExecutor", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_expand_executor(vec![vec![0, 1], vec![2]], chunk_size, chunk_num),
                    |e| execute_expand_executor(e),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_expand);
criterion_main!(benches);
