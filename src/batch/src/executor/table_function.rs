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

use anyhow::anyhow;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayRef, DataChunk, I32Array, IntervalArray, NaiveDateTimeArray,
};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{CheckedAdd, DataType, Scalar};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::table_function_node::Type::*;
use risingwave_pb::expr::ExprNode;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::error::BatchError;
use crate::executor::{BoxedDataChunkStream, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub enum TableFunction {
    GenerateSeriesTime(GenerateSeries<NaiveDateTimeArray, IntervalArray>),
    GenerateSeriesInt(GenerateSeries<I32Array, I32Array>),
    Unnest(Unnest),
}

pub struct GenerateSeries<T: Array, S: Array> {
    start: T::OwnedItem,
    stop: T::OwnedItem,
    step: S::OwnedItem,
}

impl<T: Array, S: Array> GenerateSeries<T, S>
where
    T: Array,
    S: Array,
    T::OwnedItem: PartialOrd<T::OwnedItem>,
    T::OwnedItem: for<'a> CheckedAdd<S::RefItem<'a>, Output = T::OwnedItem>,
{
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn eval(self) {
        let Self {
            start, stop, step, ..
        } = self;

        // let mut rest_rows = ((stop - start) / step + 1) as usize;
        let mut cur = start;

        // Simulate a do-while loop.
        while cur <= stop {
            let chunk_size = DEFAULT_CHUNK_BUFFER_SIZE;
            let mut builder = T::Builder::new(chunk_size);

            for _ in 0..chunk_size {
                if cur > stop {
                    break;
                }
                builder.append(Some(cur.as_scalar_ref())).unwrap();
                cur = cur
                    .checked_add(step.as_scalar_ref())
                    .ok_or(BatchError::NumericOutOfRange)?;
            }

            let arr = builder.finish()?;
            let len = arr.len();
            let columns = vec![Column::new(Arc::new(arr.into()))];
            let chunk: DataChunk = DataChunk::new(columns, len);

            yield chunk;
        }
    }
}

pub struct Unnest {
    array_refs: Vec<ArrayRef>,
    return_type: DataType,
}

impl Unnest {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn eval(self) {
        let sum = self.array_refs.iter().map(|a| a.len()).sum();
        let mut builder = self.return_type.create_array_builder(sum);
        self.array_refs
            .iter()
            .try_for_each(|a| builder.append_array(a))?;
        let arr = builder.finish()?;
        let len = arr.len();
        let columns = vec![Column::new(Arc::new(arr))];
        let chunk: DataChunk = DataChunk::new(columns, len);

        yield chunk;
    }
}

pub struct TableFunctionExecutor {
    schema: Schema,
    identity: String,
    table_function: TableFunction,
}

impl Executor for TableFunctionExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        match self.table_function {
            TableFunction::GenerateSeriesTime(gs) => gs.eval(),
            TableFunction::GenerateSeriesInt(gs) => gs.eval(),
            TableFunction::Unnest(us) => us.eval(),
        }
    }
}

pub struct TableFunctionExecutorBuilder {}

impl TableFunctionExecutorBuilder {
    fn new_time_generate_series(
        array_refs: Vec<ArrayRef>,
    ) -> Result<GenerateSeries<NaiveDateTimeArray, IntervalArray>> {
        let start = array_refs[0].clone().as_naivedatetime().value_at(0);
        let stop = array_refs[1].clone().as_naivedatetime().value_at(0);
        let step = array_refs[2].clone().as_interval().value_at(0);

        if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
            Ok(GenerateSeries::<NaiveDateTimeArray, IntervalArray> { start, stop, step })
        } else {
            Err(BatchError::Internal(anyhow!(
                "the parameters of Generate Series Function are incorrect".to_string(),
            ))
            .into())
        }
    }

    fn new_int_generate_series(
        array_refs: Vec<ArrayRef>,
    ) -> Result<GenerateSeries<I32Array, I32Array>> {
        let start = array_refs[0].clone().as_int32().value_at(0);
        let stop = array_refs[1].clone().as_int32().value_at(0);
        let step = array_refs[2].clone().as_int32().value_at(0);

        if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
            Ok(GenerateSeries::<I32Array, I32Array> { start, stop, step })
        } else {
            Err(BatchError::Internal(anyhow!(
                "the parameters of Generate Series Function are incorrect".to_string(),
            ))
            .into())
        }
    }

    pub fn new_generate_series(
        array_refs: Vec<ArrayRef>,
        return_type: DataType,
    ) -> Result<(Schema, TableFunction)> {
        match return_type {
            DataType::Timestamp => {
                let schema = Schema::new(vec![Field::unnamed(DataType::Timestamp)]);
                let func = TableFunctionExecutorBuilder::new_time_generate_series(array_refs)?;
                Ok((schema, TableFunction::GenerateSeriesTime(func)))
            }
            DataType::Int32 => {
                let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);
                let func = TableFunctionExecutorBuilder::new_int_generate_series(array_refs)?;
                Ok((schema, TableFunction::GenerateSeriesInt(func)))
            }
            _ => Err(BatchError::Internal(anyhow!(
                "the parameters of Generate Series Function are incorrect".to_string(),
            ))
            .into()),
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for TableFunctionExecutorBuilder {
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
            NodeBody::TableFunction
        )?;

        let identity = source.plan_node().get_identity().clone();

        let dummy_chunk = DataChunk::new_dummy(1);
        let args: &Vec<ExprNode> = node.get_args();

        let array_refs = args
            .iter()
            .map(|a| {
                let expr = build_from_prost(a)?;
                expr.eval(&dummy_chunk)
            })
            .try_collect()?;
        let return_type = DataType::from(node.return_type.as_ref().unwrap());
        let (schema, table_function) = match node.get_function_type()? {
            Generate => TableFunctionExecutorBuilder::new_generate_series(array_refs, return_type)?,
            Unnest => (
                Schema::new(vec![Field::unnamed(return_type.clone())]),
                TableFunction::Unnest(Unnest {
                    array_refs,
                    return_type,
                }),
            ),
        };
        Ok(Box::new(TableFunctionExecutor {
            schema,
            identity,
            table_function,
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{Array, ArrayImpl};
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
        let executor = Box::new(TableFunctionExecutor {
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: "GenerateSeriesI32Executor2".to_string(),
            table_function: TableFunction::GenerateSeriesInt(GenerateSeries { start, stop, step }),
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
        let executor = Box::new(TableFunctionExecutor {
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: "GenerateSeriesTimestampExecutor2".to_string(),
            table_function: TableFunction::GenerateSeriesTime(GenerateSeries { start, stop, step }),
        });

        let mut result_count = 0;
        let mut stream = executor.execute();
        while let Some(data_chunk) = stream.next().await {
            let data_chunk = data_chunk.unwrap();
            result_count += data_chunk.cardinality();
        }
        assert_eq!(result_count, expected_rows_count)
    }
}
