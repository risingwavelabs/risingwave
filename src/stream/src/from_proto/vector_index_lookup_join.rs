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

use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::row::RowDeserializer;
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::stream_plan::{VectorIndexLookupJoinNode, VectorIndexWriteNode};
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::{Execute, Executor, VectorIndexLookupJoinExecutor, VectorIndexWriteExecutor};
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

        let deserializer = RowDeserializer::new(
            node.info_column_desc
                .iter()
                .map(|col| DataType::from(col.column_type.clone().unwrap()))
                .collect_vec(),
        );

        let info_output_indices: Vec<usize> = node
            .info_output_indices
            .iter()
            .map(|&idx| idx as _)
            .collect();

        let vector_info_struct_type = StructType::new(
            info_output_indices
                .iter()
                .map(|idx| {
                    (
                        node.info_column_desc[*idx].name.clone(),
                        DataType::from(node.info_column_desc[*idx].column_type.clone().unwrap()),
                    )
                })
                .chain(
                    node.include_distance
                        .then(|| [("__distance".to_owned(), DataType::Float64)].into_iter())
                        .into_iter()
                        .flatten(),
                ),
        );

        assert!(
            params
                .info
                .schema
                .fields
                .last()
                .expect("non-empty")
                .data_type
                .equals_datatype(&DataType::Struct(vector_info_struct_type.clone()).list())
        );

        let executor = VectorIndexLookupJoinExecutor::new(
            input,
            store,
            vector_info_struct_type,
            info_output_indices,
            node.include_distance,
            node.table_id.into(),
            node.vector_column_idx as _,
            node.top_n as _,
            PbDistanceType::try_from(node.distance_type).unwrap().into(),
            deserializer,
            node.hnsw_ef_search as usize,
        );
        Ok(Executor::new(params.info, Box::new(executor)))
    }
}
