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

use risingwave_expr::expr::build_from_prost;
use risingwave_pb::stream_plan::WatermarkFilterNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::WatermarkFilterExecutor;

pub struct WatermarkFilterBuilder;

#[async_trait::async_trait]
impl ExecutorBuilder for WatermarkFilterBuilder {
    type Node = WatermarkFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let watermark_desc = node.get_watermark_desc()?.clone();
        let watermark_expr = build_from_prost(&watermark_desc.expr.unwrap())?;
        let event_time_col_idx = watermark_desc.watermark_idx.unwrap().index as usize;
        let vnodes = Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for watermark filter"),
        );

        // TODO: may enable sanity check for watermark filter after we have upsert.
        let table =
            StateTable::from_table_catalog_no_sanity_check(node.get_table()?, store, Some(vnodes))
                .await;

        Ok(WatermarkFilterExecutor::new(
            input,
            watermark_expr,
            event_time_col_idx,
            params.actor_context,
            table,
        )
        .boxed())
    }
}
