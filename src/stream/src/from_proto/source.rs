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

use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use tokio::sync::mpsc::unbounded_channel;

use super::*;
use crate::executor::SourceExecutor;

pub struct SourceExecutorBuilder;

impl ExecutorBuilder for SourceExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> Result<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Source)?;
        let (sender, barrier_receiver) = unbounded_channel();
        stream
            .context
            .lock_barrier_manager()
            .register_sender(params.actor_context.id, sender);

        let source_id = TableId::new(node.source_id);
        let source_desc = params.env.source_manager().get_source(&source_id)?;

        let column_ids: Vec<_> = node
            .get_column_ids()
            .iter()
            .map(|i| ColumnId::from(*i))
            .collect();
        let mut fields = Vec::with_capacity(column_ids.len());
        fields.extend(column_ids.iter().map(|column_id| {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| &c.column_id == column_id)
                .unwrap();
            Field::with_name(column_desc.data_type.clone(), column_desc.name.clone())
        }));
        let schema = Schema::new(fields);
        let keyspace = Keyspace::table_root(store, &TableId::new(node.state_table_id));
        let vnodes = params
            .vnode_bitmap
            .expect("vnodes not set for source executor");

        Ok(Box::new(SourceExecutor::new(
            params.actor_context,
            source_id,
            source_desc,
            vnodes,
            keyspace,
            column_ids,
            schema,
            params.pk_indices,
            barrier_receiver,
            params.executor_id,
            params.operator_id,
            params.op_info,
            params.executor_stats,
            stream.config.checkpoint_interval_ms as u64,
        )?))
    }
}
