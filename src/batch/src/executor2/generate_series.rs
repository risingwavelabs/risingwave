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

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::ExecutorBuilder;
use crate::executor2::{
    BoxedDataChunkStream, BoxedExecutor2, BoxedExecutor2Builder, Executor2,
    GenerateSeriesI32Executor2, GenerateSeriesTimestampExecutor2,
};

pub struct GenerateSeriesExecutor2Wraper {
    input: BoxedExecutor2,
    schema: Schema,
    identity: String,
}

impl BoxedExecutor2Builder for GenerateSeriesExecutor2Wraper {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2> {
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

        match (start.datum_at(0), stop.datum_at(0), step.datum_at(0)) {
            (
                Some(ScalarImpl::NaiveDateTime(start)),
                Some(ScalarImpl::NaiveDateTime(stop)),
                Some(ScalarImpl::Interval(step)),
            ) => {
                let schema = Schema::new(vec![Field::unnamed(DataType::Timestamp)]);

                let input = Box::new(GenerateSeriesTimestampExecutor2::new(
                    start,
                    stop,
                    step,
                    schema.clone(),
                    identity.clone(),
                ));

                Ok(Box::new(Self {
                    input,
                    schema,
                    identity,
                }))
            }
            (
                Some(ScalarImpl::Int32(start)),
                Some(ScalarImpl::Int32(stop)),
                Some(ScalarImpl::Int32(step)),
            ) => {
                let schema = Schema::new(vec![Field::unnamed(DataType::Int32)]);

                let input = Box::new(GenerateSeriesI32Executor2::new(
                    start,
                    stop,
                    step,
                    schema.clone(),
                    identity.clone(),
                ));

                Ok(Box::new(Self {
                    input,
                    schema,
                    identity,
                }))
            }
            _ => todo!(),
        }
    }
}

impl Executor2 for GenerateSeriesExecutor2Wraper {
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

impl GenerateSeriesExecutor2Wraper {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let input = self.input.execute();
        #[for_await]
        for data_chunk in input {
            let data_chunk = data_chunk?;
            yield data_chunk
        }
    }
}
