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

use std::sync::Arc;

use anyhow::Context;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::fragment_vnode::{FragmentVNodeReader, fragment_vnode_columns};
use risingwave_common::types::{ListValue, ScalarImpl};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`GetFragmentVnodesExecutor`] implements fetching fragment vnode assignments
/// from the meta node. This executor has no inputs and returns vnode info per actor.
pub struct GetFragmentVnodesExecutor {
    schema: Schema,
    identity: String,
    fragment_id: u32,
    fragment_vnode_reader: Arc<dyn FragmentVNodeReader>,
}

impl GetFragmentVnodesExecutor {
    pub fn new(
        schema: Schema,
        identity: String,
        fragment_id: u32,
        fragment_vnode_reader: Arc<dyn FragmentVNodeReader>,
    ) -> Self {
        Self {
            schema,
            identity,
            fragment_id,
            fragment_vnode_reader,
        }
    }

    async fn fetch_fragment_vnodes(&self) -> Result<Vec<Vec<Option<ScalarImpl>>>> {
        let info = self
            .fragment_vnode_reader
            .get_fragment_vnodes(self.fragment_id)
            .await?;

        let mut rows = Vec::with_capacity(info.actors.len());
        for actor in info.actors {
            let vnode_list_scalar = if actor.vnodes.is_empty() {
                None
            } else {
                let array = risingwave_common::array::I16Array::from_iter(
                    actor.vnodes.iter().copied().map(Some),
                );
                let list = ListValue::new(array.into());
                Some(list.into())
            };

            let row = vec![
                Some(ScalarImpl::Int32(info.fragment_id as i32)),
                Some(ScalarImpl::Int32(actor.actor_id as i32)),
                Some(ScalarImpl::Int32(actor.worker_id as i32)),
                Some(ScalarImpl::Int32(info.vnode_count as i32)),
                vnode_list_scalar,
            ];
            rows.push(row);
        }

        Ok(rows)
    }
}

impl Executor for GetFragmentVnodesExecutor {
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

impl GetFragmentVnodesExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let rows = self.fetch_fragment_vnodes().await?;

        if !rows.is_empty() {
            let mut array_builders = self.schema.create_array_builders(rows.len());

            for (col_idx, builder) in array_builders.iter_mut().enumerate() {
                for row in &rows {
                    builder.append(&row[col_idx]);
                }
            }

            let columns: Vec<_> = array_builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();

            let chunk = DataChunk::new(columns, rows.len());
            yield chunk;
        }
    }
}

impl BoxedExecutorBuilder for GetFragmentVnodesExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "GetFragmentVnodesExecutor should have no child!"
        );

        let get_fragment_vnodes_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GetFragmentVnodes
        )?;

        let fields = fragment_vnode_columns()
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type))
            .collect();
        let schema = Schema { fields };

        let fragment_vnode_reader = source
            .context()
            .fragment_vnode_reader()
            .context("fragment vnode reader is unavailable in this execution context")?;

        Ok(Box::new(Self::new(
            schema,
            source.plan_node().get_identity().clone(),
            get_fragment_vnodes_node.fragment_id,
            fragment_vnode_reader,
        )))
    }
}
