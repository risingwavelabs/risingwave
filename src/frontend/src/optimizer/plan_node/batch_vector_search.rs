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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::array::VectorVal;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::types::{DataType, Scalar};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::batch_plan::PbVectorIndexNearestNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::PbDistanceType;

use crate::catalog::TableId;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    Batch, ExprRewritable, PlanBase, ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order};
use crate::{OptimizerContextRef, PlanRef};

#[derive(Debug, Clone, educe::Educe)]
#[educe(Hash, PartialEq, Eq)]
pub struct BatchVectorSearchCore {
    pub top_n: u64,
    pub distance_type: PbDistanceType,
    pub index_name: String,
    pub index_table_id: TableId,
    #[educe(Hash(ignore))]
    pub vector_literal: VectorVal,
    pub info_column_desc: Vec<ColumnDesc>,
    pub include_vector_col: bool,
    pub include_distance_col: bool,
    #[educe(Hash(ignore), Eq(ignore))]
    pub ctx: OptimizerContextRef,
}

impl GenericPlanNode for BatchVectorSearchCore {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(
            self.info_column_desc.len()
                + if self.include_vector_col { 1 } else { 0 }
                + if self.include_distance_col { 1 } else { 0 },
        )
    }

    fn schema(&self) -> Schema {
        let mut fields = self
            .info_column_desc
            .iter()
            .map(|col| Field::new(col.name.clone(), col.data_type.clone()))
            .collect_vec();
        if self.include_vector_col {
            fields.push(Field::new(
                "__vector",
                DataType::Vector(self.vector_literal.as_scalar_ref().into_slice().len()),
            ));
        }
        if self.include_distance_col {
            fields.push(Field::new("__distance", DataType::Float64));
        }
        Schema { fields }
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
        let order = if core.include_distance_col {
            let column_index =
                core.info_column_desc.len() + if core.include_vector_col { 1 } else { 0 };
            Order::new(vec![ColumnOrder {
                column_index,
                order_type: OrderType::ascending(),
            }])
        } else {
            Order::any()
        };
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
        ];
        if self.core.ctx.is_explain_verbose() {
            fields.push(("vector", Pretty::debug(&self.core.vector_literal)));
        }
        childless_record("BatchVectorSearch", fields)
    }
}

impl_plan_tree_node_for_leaf!(BatchVectorSearch);

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
            query_vector: self
                .core
                .vector_literal
                .as_scalar_ref()
                .into_slice()
                .to_vec(),
            top_n: self.core.top_n as _,
            distance_type: self.core.distance_type as _,
            include_vector_col: self.core.include_vector_col,
            include_distance_col: self.core.include_distance_col,
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

impl ExprRewritable for BatchVectorSearch {}
