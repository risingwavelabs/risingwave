use std::sync::Arc;

use prost::Message;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, I32ArrayBuilder};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::Result;
use risingwave_common::types::Int32Type;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::GenerateInt32SeriesNode;

use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

pub(super) struct GenerateSeriesI32Executor {
    start: i32,
    stop: i32,
    step: i32,
    cur: i32, // Current value in the series.

    schema: Schema,
    identity: String,
}

impl BoxedExecutorBuilder for GenerateSeriesI32Executor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::GenerateSeries);

        let node = GenerateInt32SeriesNode::decode(&(source.plan_node()).get_body().value[..])
            .map_err(ProstError)?;

        Ok(Box::new(Self {
            start: node.start,
            stop: node.stop,
            step: node.step,
            cur: node.start,
            schema: Schema::new(vec![Field::new_without_name(Int32Type::create(false))]),
            identity: format!("GenerateSeriesI32Executor{:?}", source.task_id),
        }))
    }
}

#[async_trait::async_trait]
impl Executor for GenerateSeriesI32Executor {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let chunk_size = self.next_chunk_size();
        if chunk_size == 0 {
            return Ok(None);
        }

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
        Ok(Some(chunk))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl GenerateSeriesI32Executor {
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
    use risingwave_common::array::{Array, ArrayImpl};
    use risingwave_common::try_match_expand;

    use super::*;
    use crate::task::TaskId;

    #[tokio::test]
    async fn test_generate_series() {
        generate_series_test_case(2, 4, 1).await;
        generate_series_test_case(0, 9, 2).await;
        generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        let mut executor = GenerateSeriesI32Executor {
            start,
            stop,
            step,
            cur: start,
            schema: Schema::new(vec![Field::new_without_name(Int32Type::create(false))]),
            identity: format!("GenerateSeriesI32Executor{:?}", TaskId::default()),
        };
        let mut remained_values = ((stop - start) / step + 1) as usize;
        while remained_values > 0 {
            let chunk = executor.next().await.unwrap().unwrap();
            let col = chunk.column_at(0).unwrap();
            let arr = try_match_expand!(col.array_ref(), ArrayImpl::Int32).unwrap();

            if remained_values > DEFAULT_CHUNK_BUFFER_SIZE {
                assert_eq!(arr.len(), DEFAULT_CHUNK_BUFFER_SIZE);
            } else {
                assert_eq!(arr.len(), remained_values);
            }
            remained_values -= arr.len();
        }
        assert!(executor.next().await.unwrap().is_none());
    }
}
