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

use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::stream_plan::GroupTopNNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::{ActorContextRef, AppendOnlyGroupTopNExecutor, GroupTopNExecutor};
use crate::task::AtomicU64Ref;

pub struct GroupTopNExecutorBuilder<const APPEND_ONLY: bool>;

impl<const APPEND_ONLY: bool> ExecutorBuilder for GroupTopNExecutorBuilder<APPEND_ONLY> {
    type Node = GroupTopNNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let group_by: Vec<usize> = node
            .get_group_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect();
        let table = node.get_table()?;
        let vnodes = params.vnode_bitmap.map(Arc::new);
        let state_table = StateTable::from_table_catalog(table, store, vnodes).await;
        let storage_key = table
            .get_pk()
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let group_key_types = group_by
            .iter()
            .map(|i| input.schema()[*i].data_type())
            .collect();
        let order_by = node
            .order_by
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let args = GroupTopNExecutorDispatcherArgs {
            input,
            ctx: params.actor_context,
            schema: params.info.schema.clone(),
            storage_key,
            offset_and_limit: (node.offset as usize, node.limit as usize),
            order_by,
            group_by,
            state_table,
            watermark_epoch: params.watermark_epoch,
            group_key_types,

            with_ties: node.with_ties,
            append_only: APPEND_ONLY,
        };
        Ok((params.info, args.dispatch()?).into())
    }
}

struct GroupTopNExecutorDispatcherArgs<S: StateStore> {
    input: Executor,
    ctx: ActorContextRef,
    schema: Schema,
    storage_key: Vec<ColumnOrder>,
    offset_and_limit: (usize, usize),
    order_by: Vec<ColumnOrder>,
    group_by: Vec<usize>,
    state_table: StateTable<S>,
    watermark_epoch: AtomicU64Ref,
    group_key_types: Vec<DataType>,

    with_ties: bool,
    append_only: bool,
}

impl<S: StateStore> HashKeyDispatcher for GroupTopNExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        macro_rules! build {
            ($excutor:ident, $with_ties:literal) => {
                Ok($excutor::<K, S, $with_ties>::new(
                    self.input,
                    self.ctx,
                    self.schema,
                    self.storage_key,
                    self.offset_and_limit,
                    self.order_by,
                    self.group_by,
                    self.state_table,
                    self.watermark_epoch,
                )?
                .boxed())
            };
        }
        match (self.append_only, self.with_ties) {
            (true, true) => build!(AppendOnlyGroupTopNExecutor, true),
            (true, false) => build!(AppendOnlyGroupTopNExecutor, false),
            (false, true) => build!(GroupTopNExecutor, true),
            (false, false) => build!(GroupTopNExecutor, false),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}
