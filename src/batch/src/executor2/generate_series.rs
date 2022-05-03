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
use risingwave_common::array::{ArrayBuilder, DataChunk, I32ArrayBuilder};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::ExecutorBuilder;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2};

pub struct GenerateSeriesI32Executor2 {
    start: i32,
    stop: i32,
    step: i32,
    cur: i32, // Current value in the series.

    schema: Schema,
    identity: String,
}

impl BoxedExecutor2Builder for GenerateSeriesI32Executor2 {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GenerateInt32Series
        )?;

        Ok(Box::new(Self {
            start: node.start,
            stop: node.stop,
            step: node.step,
            cur: node.start,
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

impl Executor2 for GenerateSeriesI32Executor2 {
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

impl GenerateSeriesI32Executor2 {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut chunk_size = self.next_chunk_size();

        while chunk_size != 0 {
            let mut builder = I32ArrayBuilder::new(chunk_size).unwrap();
            let mut current_value = self.cur;
            for _ in 0..chunk_size {
                builder.append(Some(current_value)).unwrap();
                current_value += self.step;
            }
            self.cur = current_value;

            let arr = builder.finish()?;
            let columns = vec![Column::new(Arc::new(arr.into()))];
            let chunk: DataChunk = DataChunk::builder().columns(columns).build();

            yield chunk;

            chunk_size = self.next_chunk_size();
        }
    }
}

impl GenerateSeriesI32Executor2 {
    fn next_chunk_size(&self) -> usize {
        if self.cur > self.stop {
            return 0;
        }
        let mut num: usize = ((self.stop - self.cur) / self.step + 1) as usize;
        if num > DEFAULT_CHUNK_BUFFER_SIZE {
            num = DEFAULT_CHUNK_BUFFER_SIZE;
        }
        num
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{Array, ArrayImpl};
    use risingwave_common::try_match_expand;

    use super::*;

    #[tokio::test]
    async fn test_generate_series() {
        generate_series_test_case(2, 4, 1).await;
        generate_series_test_case(0, 9, 2).await;
        generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        let executor = Box::new(GenerateSeriesI32Executor2 {
            start,
            stop,
            step,
            cur: start,
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
}
