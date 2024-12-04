// Copyright 2024 RisingWave Labs
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
use risingwave_common::catalog::TableId;
use risingwave_pb::stream_plan::DmlNode;
use risingwave_storage::StateStore;

use super::ExecutorBuilder;
use crate::error::StreamResult;
use crate::executor::dml::DmlExecutor;
use crate::executor::Executor;
use crate::task::ExecutorParams;

pub struct DmlExecutorBuilder;

impl ExecutorBuilder for DmlExecutorBuilder {
    type Node = DmlNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();
        let table_id = TableId::new(node.table_id);
        let column_descs = node.column_descs.iter().map(Into::into).collect_vec();

        let exec = DmlExecutor::new(
            params.actor_context.clone(),
            upstream,
            params.env.dml_manager_ref(),
            table_id,
            node.table_version_id,
            column_descs,
            params.env.config().developer.chunk_size,
            node.rate_limit,
        );
        Ok((params.info, exec).into())
    }
}
