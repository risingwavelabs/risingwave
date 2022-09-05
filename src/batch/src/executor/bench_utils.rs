use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::test_utils::{gen_data, MockExecutor};
use super::BoxedExecutor;

#[macro_export]
macro_rules! bench_join {
    // TODO: change the type of $join.
    ($join:expr) => {
        paste! {
            fn [<bench_ $join:snake _internal>](c: &mut Criterion, with_conds: Vec<bool>, join_types: Vec<JoinType>) {
                const LEFT_SIZE: usize = 2 * 1024;
                const RIGHT_SIZE: usize = 2 * 1024;
                let rt = Runtime::new().unwrap();
                for with_cond in with_conds {
                    for join_type in join_types.clone() {
                        for chunk_size in &[32, 128, 512, 1024] {
                            c.bench_with_input(
                                BenchmarkId::new(
                                    format!("{}Executor", $join),
                                    format!(
                                        "{}({:?})(join: {})",
                                        chunk_size, join_type, with_cond
                                    ),
                                ),
                                chunk_size,
                                |b, &chunk_size| {
                                    let left_chunk_num = LEFT_SIZE / chunk_size;
                                    let right_chunk_num = RIGHT_SIZE / chunk_size;
                                    b.to_async(&rt).iter_batched(
                                        || {
                                            [<create_ $join:snake _executor>](
                                                join_type,
                                                with_cond,
                                                chunk_size,
                                                left_chunk_num,
                                                chunk_size,
                                                right_chunk_num,
                                            )
                                        },
                                        |e| async move {
                                            let mut stream = e.execute();
                                            while let Some(ret) = stream.next().await {
                                                black_box(ret.unwrap());
                                            }
                                        },
                                        BatchSize::SmallInput,
                                    );
                                },
                            );
                        }
                    }
                }
            }
        }
    };
}

pub fn create_input(
    input_types: &[DataType],
    chunk_size: usize,
    chunk_num: usize,
) -> BoxedExecutor {
    let mut input = MockExecutor::new(Schema {
        fields: input_types
            .iter()
            .map(Clone::clone)
            .map(Field::unnamed)
            .collect(),
    });
    for c in gen_data(chunk_size, chunk_num, input_types) {
        input.add(c);
    }
    Box::new(input)
}
