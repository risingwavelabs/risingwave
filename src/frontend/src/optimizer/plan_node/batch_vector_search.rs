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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::batch_plan::PbVectorIndexNearestNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::PbDistanceType;

use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::expr::{ExprDisplay, ExprImpl, InputRef};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    Batch, BatchPlanRef as PlanRef, BatchPlanRef, ExprRewritable, PlanBase, PlanTreeNodeUnary,
    ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order};

#[derive(Debug, Clone, educe::Educe)]
#[educe(Hash, PartialEq, Eq)]
pub struct BatchVectorSearchCore {
    pub input: BatchPlanRef,
    pub top_n: u64,
    pub distance_type: PbDistanceType,
    pub index_name: String,
    pub index_table_id: TableId,
    pub info_column_desc: Vec<ColumnDesc>,
    pub vector_column_idx: usize,
    pub hnsw_ef_search: Option<usize>,
    #[educe(Hash(ignore), Eq(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for BatchVectorSearchCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        // FunctionalDependencySet::new(self.info_column_desc.len())
        // TODO: include dependency derived from info columns
        self.input.functional_dependency().clone()
    }

    fn schema(&self) -> Schema {
        let mut schema = self.input.schema().clone();
        schema.fields.push(Field::new(
            "vector_info",
            DataType::list(
                StructType::new(
                    self.info_column_desc
                        .iter()
                        .map(|col| (col.name.clone(), col.data_type.clone()))
                        .chain([("__distance".to_owned(), DataType::Float64)]),
                )
                .into(),
            ),
        ));
        schema
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchVectorSearch {
    pub base: PlanBase<Batch>,
    pub core: BatchVectorSearchCore,
}

impl BatchVectorSearch {
    pub(super) fn with_core(core: BatchVectorSearchCore) -> Self {
        Self::with_core_inner(core, Distribution::Single)
    }

    fn with_core_someshard(core: BatchVectorSearchCore) -> Self {
        Self::with_core_inner(core, Distribution::SomeShard)
    }

    fn with_core_inner(core: BatchVectorSearchCore, distribution: Distribution) -> Self {
        // TODO: support specifying order in nested struct to avoid unnecessary sort
        let order = Order::any();
        let base = PlanBase::new_batch_with_core(&core, distribution, order);
        Self { base, core }
    }
}

impl Distill for BatchVectorSearch {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut fields = vec![
            (
                "schema",
                Pretty::Array(self.schema().fields.iter().map(Pretty::debug).collect()),
            ),
            ("top_n", Pretty::debug(&self.core.top_n)),
            ("distance_type", Pretty::debug(&self.core.distance_type)),
            ("index_name", Pretty::debug(&self.core.index_name)),
            (
                "vector",
                Pretty::debug(&ExprDisplay {
                    expr: &ExprImpl::InputRef(
                        InputRef::new(
                            self.core.vector_column_idx,
                            self.core.input.schema()[self.core.vector_column_idx].data_type(),
                        )
                        .into(),
                    ),
                    input_schema: self.core.input.schema(),
                }),
            ),
        ];
        if let Some(hnsw_ef_search) = self.core.hnsw_ef_search {
            fields.push(("hnsw_ef_search", Pretty::debug(&hnsw_ef_search)));
        }
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
