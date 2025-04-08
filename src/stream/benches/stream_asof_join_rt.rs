use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use risingwave_stream::executor::AsOfJoinType;
use risingwave_stream::executor::test_utils::asof_join_executor::*;
use tokio::runtime::Runtime;

risingwave_expr_impl::enable!();

fn bench_asof_join(c: &mut Criterion) {
    let mut group: criterion::BenchmarkGroup<'_, criterion::measurement::WallTime> =
        c.benchmark_group("benchmark_asof_join");
    group.sample_size(15);

    let rt = Runtime::new().unwrap();
    let params = [
        (20, 10000, 2, 4),
        (20, 10000, 100, 4),
        (2, 100000, 2, 4),
        (2, 100000, 10000, 4),
        (200000, 4, 2, 4),
        // (2000, 10000, 2, 4),
    ];

    for (jk_card, upper_bound, step_size_l, step_size_r) in params {
        for join_type in [AsOfJoinType::Inner, AsOfJoinType::LeftOuter] {
            let workload = AsOfJoinWorkload::new(jk_card, upper_bound, step_size_l, step_size_r);
            let name = format!("asof_join_rt_{}_{}_{}_{:#?}", jk_card, upper_bound, step_size_l, join_type);

            group.bench_function(&name, |b| {
                b.to_async(&rt).iter_batched(
                    || block_on(setup_bench_stream_asof_join(join_type)),
                    |(tx_l, tx_r, out)| {
                        handle_streams(workload.clone(), join_type, tx_l, tx_r, out)
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

criterion_group!(benches, bench_asof_join);
criterion_main!(benches);
