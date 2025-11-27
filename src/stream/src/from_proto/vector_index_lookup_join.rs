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

use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::VectorIndexLookupJoinNode;
use risingwave_storage::StateStore;
use risingwave_storage::table::batch_table::VectorIndexReader;

use crate::error::StreamResult;
use crate::executor::{Executor, VectorIndexLookupJoinExecutor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct VectorIndexLookupJoinBuilder;

impl ExecutorBuilder for VectorIndexLookupJoinBuilder {
    type Node = VectorIndexLookupJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();

        let reader = VectorIndexReader::new(node.reader_desc.as_ref().unwrap(), store);

        assert!(
            params
                .info
                .schema
                .fields
                .last()
                .expect("non-empty")
                .data_type
                .equals_datatype(&DataType::Struct(reader.info_struct_type().clone()).list())
        );

        let executor =
            VectorIndexLookupJoinExecutor::new(input, reader, node.vector_column_idx as _);
        Ok(Executor::new(params.info, Box::new(executor)))
    }
}
