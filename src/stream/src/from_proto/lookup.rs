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

use std::sync::Arc;

use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::util::sort_util::OrderPair;
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::executor::{LookupExecutor, LookupExecutorParams};

pub struct LookupExecutorBuilder;

impl ExecutorBuilder for LookupExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream_manager: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let lookup = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Lookup)?;

        let [stream, arrangement]: [_; 2] = params.input.try_into().unwrap();

        let arrangement_order_rules = lookup
            .get_arrangement_table_info()?
            .arrange_key_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();

        let arrangement_col_descs = lookup
            .get_arrangement_table_info()?
            .column_descs
            .iter()
            .map(ColumnDesc::from)
            .collect();
        let state_table = StateTable::from_table_catalog(
            lookup.arrangement_table.as_ref().unwrap(),
            store,
            params.vnode_bitmap.map(Arc::new),
        );

        Ok(Box::new(LookupExecutor::new(LookupExecutorParams {
            schema: Schema::new(node.fields.iter().map(Field::from).collect()),
            arrangement,
            stream,
            arrangement_col_descs,
            arrangement_order_rules,
            pk_indices: params.pk_indices,
            use_current_epoch: lookup.use_current_epoch,
            stream_join_key_indices: lookup.stream_key.iter().map(|x| *x as usize).collect(),
            arrange_join_key_indices: lookup.arrange_key.iter().map(|x| *x as usize).collect(),
            column_mapping: lookup.column_mapping.iter().map(|x| *x as usize).collect(),
            state_table,
            lru_manager: stream_manager.context.lru_manager.clone(),
            cache_size: stream_manager
                .config
                .developer
                .unsafe_stream_join_cache_size,
        })))
    }
}
