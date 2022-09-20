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

use risingwave_common::catalog::{ColumnId, TableId};

use super::*;
use crate::executor::SinkExecutor;

pub struct SinkExecutorBuilder;

impl ExecutorBuilder for SinkExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Sink)?;
        let [materialize_executor]: [_; 1] = params.input.try_into().unwrap();

        let _sink_id = TableId::from(node.table_id);
        let _column_ids = node
            .get_column_ids()
            .iter()
            .map(|i| ColumnId::from(*i))
            .collect::<Vec<ColumnId>>();

        Ok(Box::new(SinkExecutor::new(
            materialize_executor,
            store,
            stream.streaming_metrics.clone(),
            node.properties.clone(),
            params.executor_id,
        )))
    }
}
