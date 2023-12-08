// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use risingwave_common::catalog::{ColumnId, Schema};
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::stream_plan::StreamArrangementBackfillNode;

use super::*;
use crate::common::table::state_table::{ReplicatedStateTable, StateTable};
use crate::executor::{ArrangementBackfillExecutor, FlowControlExecutor};

pub struct ArrangementBackfillExecutorBuilder;

impl ExecutorBuilder for ArrangementBackfillExecutorBuilder {
    type Node = StreamArrangementBackfillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [upstream, snapshot]: [_; 2] = params.input.try_into().unwrap();
        // For reporting the progress.
        let progress = stream
            .context
            .register_create_mview_progress(params.actor_context.id);

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let schema = Schema::new(
            output_indices
                .iter()
                .map(|i| snapshot.schema().fields()[*i].clone())
                .collect_vec(),
        );

        let column_ids = node
            .upstream_column_ids
            .iter()
            .map(ColumnId::from)
            .collect_vec();

        let vnodes = params.vnode_bitmap.map(Arc::new);

        let state_table = if let Ok(table) = node.get_state_table() {
            Some(StateTable::from_table_catalog(table, state_store.clone(), vnodes.clone()).await)
        } else {
            None
        };
        let upstream_table = node.get_arrangement_table().unwrap();
        let versioned = upstream_table.get_version().is_ok();

        macro_rules! new_executor {
            ($SD:ident) => {{
                let upstream_table =
                    ReplicatedStateTable::<_, $SD>::from_table_catalog_with_output_column_ids(
                        upstream_table,
                        state_store.clone(),
                        vnodes,
                        column_ids,
                    )
                    .await;
                ArrangementBackfillExecutor::<_, $SD>::new(
                    params.info,
                    upstream_table,
                    upstream,
                    state_table.unwrap(),
                    output_indices,
                    progress,
                    schema,
                    stream.streaming_metrics.clone(),
                    params.env.config().developer.chunk_size,
                )
                .boxed()
            }};
        }
        let executor = if versioned {
            new_executor!(ColumnAwareSerde)
        } else {
            new_executor!(BasicSerde)
        };
        Ok(FlowControlExecutor::new(
            executor,
            params.actor_context,
            node.rate_limit.map(|x| x as _),
        )
        .boxed())
    }
}
