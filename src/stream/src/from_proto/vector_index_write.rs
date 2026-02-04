// Copyright 2025 RisingWave Labs
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

use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::VectorIndexWriteNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{Executor, VectorIndexWriteExecutor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct VectorIndexWriteExecutorBuilder;

impl ExecutorBuilder for VectorIndexWriteExecutorBuilder {
    type Node = VectorIndexWriteNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let table = node.table.as_ref().unwrap();
        assert_eq!(table.columns.len(), input.schema().len());
        let index_col_type = DataType::from(
            table.columns[0]
                .column_desc
                .as_ref()
                .unwrap()
                .column_type
                .as_ref()
                .unwrap(),
        );
        let DataType::Vector(dimension) = &index_col_type else {
            bail!("expect vector column but got: {:?}", index_col_type)
        };
        let DataType::Vector(input_dimension) = &input.schema().fields[0].data_type else {
            bail!(
                "expect first input vector column but got: {:?}",
                index_col_type
            )
        };
        assert_eq!(dimension, input_dimension);

        let executor = VectorIndexWriteExecutor::new(input, store, table.id).await?;
        Ok(Executor::new(params.info, Box::new(executor)))
    }
}
