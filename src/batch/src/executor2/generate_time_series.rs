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

use chrono::Duration;
use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, NaiveDateTimeArrayBuilder};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_common::types::{IntervalUnit, NaiveDateTimeWrapper};
use crate::executor::ExecutorBuilder;
use crate::executor2::{BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2};

pub struct GenerateSeriesTimeStampExecutor2 {
    start: NaiveDateTimeWrapper,
    stop: NaiveDateTimeWrapper,
    step: IntervalUnit,
    cur: NaiveDateTimeWrapper, // Current value in the series.

    schema: Schema,
    identity: String,
}

impl BoxedExecutor2Builder for GenerateSeriesTimeStampExecutor2 {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GenerateTimeSeries
        )?;

        Ok(Box::new(Self {
            start: NaiveDateTimeWrapper::parse_from_str(node.get_start())?,
            stop: NaiveDateTimeWrapper::parse_from_str(node.get_stop())?,
            step: node.get_step()?.into(),
            cur: NaiveDateTimeWrapper::parse_from_str(node.get_start())?,
            schema: Schema::new(vec![Field::unnamed(DataType::Timestamp)]),
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

impl Executor2 for GenerateSeriesTimeStampExecutor2 {
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

impl GenerateSeriesTimeStampExecutor2 {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        let mut chunk_size = self.next_chunk_size();

        while chunk_size != 0 {
            let mut builder = NaiveDateTimeArrayBuilder::new(chunk_size).unwrap();
            let mut current_value = self.cur;
            for _ in 0..chunk_size {
                builder.append(Some(current_value)).unwrap();
                current_value = current_value.add(self.step.into()) ;
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

impl GenerateSeriesTimeStampExecutor2 {
    fn next_chunk_size(&self) -> usize {
        if self.cur > self.stop {
            return 0;
        }
        let interval_ms = self.step.get_total_ms();
        let spread_ms = self.stop.sub(self.cur).num_milliseconds();
        let mut num = (spread_ms / interval_ms + 1) as usize;
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
    async fn test_generate_time_series() {
        let start_time = NaiveDateTimeWrapper::parse_from_str("2008-03-01 00:00:00").unwrap();
        let stop_time = NaiveDateTimeWrapper::parse_from_str("2008-03-09 00:00:00").unwrap();
        let one_minute_step = IntervalUnit::from_minutes(1);
        let one_hour_step = IntervalUnit::from_minutes(60);
        let one_day_step = IntervalUnit::from_days(1);
        generate_time_series_test_case(start_time, stop_time, one_minute_step).await;
        generate_time_series_test_case(start_time, stop_time, one_hour_step).await;
        generate_time_series_test_case(start_time, stop_time, one_day_step).await;
        // generate_series_test_case(0, 9, 2).await;
        // generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_time_series_test_case(start: NaiveDateTimeWrapper, stop: NaiveDateTimeWrapper, step: IntervalUnit) {
        let executor = Box::new(GenerateSeriesTimeStampExecutor2 {
            start,
            stop,
            step,
            cur: start,
            schema: Schema::new(vec![Field::unnamed(DataType::Int32)]),
            identity: "GenerateSeriesTimeStampExecutor2".to_string(),
        });
        let interval_ms = step.get_total_ms();
        let start_stop_spread_ms = stop.sub(start).num_milliseconds();
        let mut remained_values = (start_stop_spread_ms / interval_ms + 1) as usize;
        let mut stream = executor.execute();
        while remained_values > 0 {
            let chunk = stream.next().await.unwrap().unwrap();
            let col = chunk.column_at(0);
            let arr = try_match_expand!(col.array_ref(), ArrayImpl::NaiveDateTime).unwrap();

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
