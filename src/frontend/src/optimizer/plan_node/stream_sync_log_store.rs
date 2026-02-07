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

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::SyncLogStoreNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::StreamPlanNodeType;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::{
    Distill, childless_record, infer_synced_kv_log_store_table_catalog_inner,
};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, StreamPlanRef as PlanRef,
};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncLogStoreTarget {
    UnalignedHashJoin,
    SinkIntoTable,
}

impl SyncLogStoreTarget {
    pub fn metrics_target(self) -> &'static str {
        match self {
            Self::UnalignedHashJoin => "unaligned_hash_join",
            Self::SinkIntoTable => "sink-into-table",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSyncLogStore {
    pub base: PlanBase<Stream>,
    pub input: PlanRef,
    pub plan_node_discriminant: StreamPlanNodeType,
    pub metrics_target: SyncLogStoreTarget,
}

impl StreamSyncLogStore {
    pub fn new_with_target(
        input: PlanRef,
        plan_node_discriminant: StreamPlanNodeType,
        metrics_target: SyncLogStoreTarget,
    ) -> Self {
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.stream_key().map(|keys| keys.to_vec()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.stream_kind(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );

        Self {
            base,
            input,
            plan_node_discriminant,
            metrics_target,
        }
    }
}

impl Distill for StreamSyncLogStore {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamSyncLogStore", vec![])
    }
}

impl PlanTreeNodeUnary<Stream> for StreamSyncLogStore {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new_with_target(input, self.plan_node_discriminant, self.metrics_target)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamSyncLogStore }

impl StreamNode for StreamSyncLogStore {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let columns = self.input.schema().fields();
        let log_store_table = infer_synced_kv_log_store_table_catalog_inner(&self.input, columns)
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost()
            .into();
        NodeBody::SyncLogStore(Box::new(SyncLogStoreNode {
            log_store_table,
            aligned: false,
            metrics_target: self.metrics_target.metrics_target().to_owned(),

            // The following fields should now be read from per-job config override.
            #[allow(deprecated)]
            pause_duration_ms: None,
            #[allow(deprecated)]
            buffer_size: None,
        }))
    }
}

impl ExprRewritable<Stream> for StreamSyncLogStore {}

impl ExprVisitable for StreamSyncLogStore {}
