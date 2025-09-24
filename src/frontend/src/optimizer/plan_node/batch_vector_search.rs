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
use risingwave_pb::batch_plan::PbVectorIndexNearestNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::VectorIndexLookupJoin;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    Batch, BatchPlanRef as PlanRef, BatchPlanRef, ExprRewritable, PlanBase, PlanTreeNodeUnary,
    ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchVectorSearch {
    pub base: PlanBase<Batch>,
    pub core: VectorIndexLookupJoin<BatchPlanRef>,
}

impl BatchVectorSearch {
    pub(super) fn with_core(core: VectorIndexLookupJoin<BatchPlanRef>) -> Self {
        Self::with_core_inner(core, Distribution::Single)
    }

    fn with_core_someshard(core: VectorIndexLookupJoin<BatchPlanRef>) -> Self {
        Self::with_core_inner(core, Distribution::SomeShard)
    }

    fn with_core_inner(
        core: VectorIndexLookupJoin<BatchPlanRef>,
        distribution: Distribution,
    ) -> Self {
        // TODO: support specifying order in nested struct to avoid unnecessary sort
        let order = Order::any();
        let base = PlanBase::new_batch_with_core(&core, distribution, order);
        Self { base, core }
    }
}

impl Distill for BatchVectorSearch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = self.core.distill();
        childless_record("BatchVectorSearch", fields)
    }
}

impl PlanTreeNodeUnary<Batch> for BatchVectorSearch {
    fn input(&self) -> crate::PlanRef<Batch> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: crate::PlanRef<Batch>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::with_core(core)
    }
}

impl_plan_tree_node_for_unary!(Batch, BatchVectorSearch);

impl ToBatchPb for BatchVectorSearch {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::VectorIndexNearest(PbVectorIndexNearestNode {
            table_id: self.core.index_table_id.table_id,
            info_column_desc: self
                .core
                .info_column_desc
                .iter()
                .map(|col| col.to_protobuf())
                .collect(),
            vector_column_idx: self.core.vector_column_idx as _,
            top_n: self.core.top_n as _,
            distance_type: self.core.distance_type as _,
            hnsw_ef_search: self.core.hnsw_ef_search.unwrap_or(0) as _,
            info_output_indices: self
                .core
                .info_output_indices
                .iter()
                .map(|&idx| idx as _)
                .collect(),
            include_distance: self.core.include_distance,
        })
    }
}

impl ToLocalBatch for BatchVectorSearch {
    fn to_local(&self) -> crate::error::Result<PlanRef> {
        Ok(Self::with_core_someshard(self.core.clone()).into())
    }
}

impl ToDistributedBatch for BatchVectorSearch {
    fn to_distributed(&self) -> crate::error::Result<PlanRef> {
        Ok(Self::with_core_someshard(self.core.clone()).into())
    }
}

impl ExprVisitable for BatchVectorSearch {}

impl ExprRewritable<Batch> for BatchVectorSearch {}
