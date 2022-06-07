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

use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    Array, ArrayBuilder, DataChunk, I32Array, IntervalArray, NaiveDateTimeArray,
};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{CheckedAdd, DataType, Scalar};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{BoxedDataChunkStream, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct GenerateSeriesExecutor<T: Array, S: Array> {
    start: T::OwnedItem,
    stop: T::OwnedItem,
    step: S::OwnedItem,

    schema: Schema,
    identity: String,
}

impl<T: Array, S: Array> GenerateSeriesExecutor<T, S> {
    pub fn new(
        start: T::OwnedItem,
        stop: T::OwnedItem,
        step: S::OwnedItem,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            start,
            stop,
            step,
            schema,
            identity,
        }
    }
}

impl<T: Array, S: Array> Executor for GenerateSeriesExecutor<T, S>
where
    T::OwnedItem: PartialOrd<T::OwnedItem>,
    T::OwnedItem: for<'a> CheckedAdd<S::RefItem<'a>>,
{
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<T, S> GenerateSeriesExecutor<T, S>
where
    T: Array,
    S: Array,
    T::OwnedItem: PartialOrd<T::OwnedItem>,
    T::OwnedItem: for<'a> CheckedAdd<S::RefItem<'a>>,
{
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            start, stop, step, ..
        } = *self;

        // let mut rest_rows = ((stop - start) / step + 1) as usize;
        let mut cur = start;

        // Simulate a do-while loop.
        while cur <= stop {
            let chunk_size = DEFAULT_CHUNK_BUFFER_SIZE;
            let mut builder = T::Builder::new(chunk_size)?;

            for _ in 0..chunk_size {
                if cur > stop {
                    break;
                }
                builder.append(Some(cur.as_scalar_ref())).unwrap();
                cur = cur.checked_add(step.as_scalar_ref())?;
            }

            let arr = builder.finish()?;
            let len = arr.len();
            let columns = vec![Column::new(Arc::new(arr.into()))];
            let chunk: DataChunk = DataChunk::new(columns, len);

            yield chunk;
        }
    }
}

pub struct GenerateSeriesExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for GenerateSeriesExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "GenerateSeriesExecutor should not have child!"
        );
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GenerateSeries
        )?;

        let identity = source.plan_node().get_identity().clone();

        let dummy_chunk = DataChunk::new_dummy(1);
        let start_expr = build_from_prost(node.get_start()?)?;
        let stop_expr = build_from_prost(node.get_stop()?)?;
        let step_expr = build_from_prost(node.get_step()?)?;

        let start = start_expr.eval(&dummy_chunk)?;
        let stop = stop_expr.eval(&dummy_chunk)?;
        let step = step_expr.eval(&dummy_chunk)?;

        match start_expr.return_type() {
            DataType::Timestamp => {
                let start = start.as_naivedatetime().value_at(0);
                let stop = stop.as_naivedatetime().value_at(0);
                let step = step.as_interval().value_at(0);

                if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                    let schema = Schema::new(vec![Field::unnamed(DataType::Timestamp)]);

                    Ok(Box::new(GenerateSeriesExecutor::<
                        NaiveDateTimeArray,
                        IntervalArray,
                    >::new(
                        start, stop, step, schema, identity
                    )))
                } else {
                    Err(ErrorCode::InternalError(
                        "the parameters of Generate SeriesFunction are incorrect".to_string(),
                    )
                    .into())
                }
            }
            DataType::Int32 => {
                let start = start.as_int32().value_at(0);
                let stop = stop.as_int32().value_at(0);
                let step = step.as_int32().value_at(0);

                if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                    let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);

                    Ok(Box::new(GenerateSeriesExecutor::<I32Array, I32Array>::new(
                        start, stop, step, schema, identity,
                    )))
                } else {
                    Err(ErrorCode::InternalError(
                        "the parameters of Generate Series Function are incorrect".to_string(),
                    )
                    .into())
                }
            }
            _ => Err(ErrorCode::InternalError(
                "the parameters of Generate Series Function are incorrect".to_string(),
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{Array, ArrayImpl, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::try_match_expand;
    use risingwave_common::types::{DataType, IntervalUnit, NaiveDateTimeWrapper};
    use risingwave_expr::vector_op::cast::str_to_timestamp;

    use super::*;

    #[tokio::test]
    async fn test_generate_i32_series() {
        generate_series_test_case(2, 4, 1).await;
        generate_series_test_case(0, 9, 2).await;
        generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        let executor = Box::new(GenerateSeriesExecutor::<I32Array, I32Array> {
            start,
            stop,
            step,
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: "GenerateSeriesI32Executor2".to_string(),
        });
        let mut remained_values = ((stop - start) / step + 1) as usize;
        let mut stream = executor.execute();
        while remained_values > 0 {
            let chunk = stream.next().await.unwrap().unwrap();
            let col = chunk.column_at(0);
            let arr = try_match_expand!(col.array_ref(), ArrayImpl::Int32).unwrap();

            if remained_values > DEFAULT_CHUNK_BUFFER_SIZE {
                assert_eq!(arr.len(), DEFAULT_CHUNK_BUFFER_SIZE);
            } else {
                assert_eq!(arr.len(), remained_values);
            }
            remained_values -= arr.len();
        }
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_generate_time_series() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = IntervalUnit::from_minutes(1);
        let one_hour_step = IntervalUnit::from_minutes(60);
        let one_day_step = IntervalUnit::from_days(1);
        generate_time_series_test_case(start_time, stop_time, one_minute_step, 60 * 24 * 8 + 1)
            .await;
        generate_time_series_test_case(start_time, stop_time, one_hour_step, 24 * 8 + 1).await;
        generate_time_series_test_case(start_time, stop_time, one_day_step, 8 + 1).await;
    }

    async fn generate_time_series_test_case(
        start: NaiveDateTimeWrapper,
        stop: NaiveDateTimeWrapper,
        step: IntervalUnit,
        expected_rows_count: usize,
    ) {
        let executor = Box::new(
            GenerateSeriesExecutor::<NaiveDateTimeArray, IntervalArray>::new(
                start,
                stop,
                step,
                Schema::new(vec![Field::unnamed(DataType::Int32)]),
                "GenerateSeriesTimestampExecutor2".to_string(),
            ),
        );

        let mut result_count = 0;
        let mut stream = executor.execute();
        while let Some(data_chunk) = stream.next().await {
            let data_chunk = data_chunk.unwrap();
            result_count += data_chunk.cardinality();
        }
        assert_eq!(result_count, expected_rows_count)
    }
}
