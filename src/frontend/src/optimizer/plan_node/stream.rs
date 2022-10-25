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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan as pb;
use pb::stream_node as pb_node;

use super::generic::GenericBase;
use super::utils::TableCatalogBuilder;
use super::{generic, EqJoinPredicate, PlanNodeId};
use crate::optimizer::property::{Distribution, FunctionalDependencySet};
use crate::session::OptimizerContextRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::Condition;
use crate::{TableCatalog, WithOptions};

macro_rules! impl_node {
($base:ident, $($t:ident),*) => {
    #[derive(Debug, Clone)]
    pub enum Node {
        $($t(Box<$t>),)*
    }
    $(
    impl From<$t> for Node {
        fn from(o: $t) -> Node {
            Node::$t(Box::new(o))
        }
    }
    )*
    pub type PlanOwned = ($base, Node);
    pub type PlanRef = std::rc::Rc<PlanOwned>;
};
}

impl generic::GenericPlanRef for PlanRef {
    fn schema(&self) -> &Schema {
        &self.0.schema
    }

    fn distribution(&self) -> &Distribution {
        &self.0.dist
    }

    fn append_only(&self) -> bool {
        self.0.append_only
    }

    fn logical_pk(&self) -> &[usize] {
        &self.0.logical_pk
    }
}

impl generic::GenericBase for PlanBase {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn logical_pk(&self) -> &[usize] {
        &self.logical_pk
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn distribution(&self) -> &Distribution {
        &self.dist
    }
}

/// Implements [`generic::Join`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone)]
pub struct DeltaJoin {
    pub logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,
}

#[derive(Clone, Debug)]
pub struct DynamicFilter {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub predicate: Condition,
    // dist_key_l: Distribution,
    pub left_index: usize,
    pub left: PlanRef,
    pub right: PlanRef,
}

#[derive(Debug, Clone)]
pub struct Exchange(pub PlanRef);

#[derive(Debug, Clone)]
pub struct Expand(pub generic::Expand<PlanRef>);

#[derive(Debug, Clone)]
pub struct Filter(pub generic::Filter<PlanRef>);

#[derive(Debug, Clone)]
pub struct GlobalSimpleAgg(pub generic::Agg<PlanRef>);

#[derive(Debug, Clone)]
pub struct GroupTopN {
    pub logical: generic::TopN<PlanRef>,
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct HashAgg {
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
    pub logical: generic::Agg<PlanRef>,
}

/// Implements [`generic::Join`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct HashJoin {
    pub logical: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    pub is_append_only: bool,
}

impl HashJoin {
    /// Return hash join internal table catalog and degree table catalog.
    pub fn infer_internal_and_degree_table_catalog(
        base: &impl GenericBase,
        join_key_indices: Vec<usize>,
    ) -> (TableCatalog, TableCatalog) {
        let schema = base.schema();

        let internal_table_dist_keys = base.distribution().dist_column_indices().to_vec();

        // Find the dist key position in join key.
        // FIXME(yuhao): currently the dist key position is not the exact position mapped to the
        // join key when there are duplicate value in join key indices.
        let degree_table_dist_keys = internal_table_dist_keys
            .iter()
            .map(|idx| {
                join_key_indices
                    .iter()
                    .position(|v| v == idx)
                    .expect("join key should contain dist key.")
            })
            .collect();

        // The pk of hash join internal and degree table should be join_key + input_pk.
        let mut pk_indices = join_key_indices;
        // TODO(yuhao): dedup the dist key and pk.
        pk_indices.extend(base.logical_pk());

        // Build internal table
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(base.ctx().inner().with_options.internal_table_subset());
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending)
        });

        // Build degree table.
        let mut degree_table_catalog_builder =
            TableCatalogBuilder::new(base.ctx().inner().with_options.internal_table_subset());

        let degree_column_field = Field::with_name(DataType::Int64, "_degree");

        pk_indices.iter().enumerate().for_each(|(order_idx, idx)| {
            degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
            degree_table_catalog_builder.add_order_column(order_idx, OrderType::Ascending)
        });
        degree_table_catalog_builder.add_column(&degree_column_field);
        degree_table_catalog_builder
            .set_value_indices(vec![degree_table_catalog_builder.columns().len() - 1]);

        (
            internal_table_catalog_builder.build(internal_table_dist_keys),
            degree_table_catalog_builder.build(degree_table_dist_keys),
        )
    }
}

#[derive(Debug, Clone)]
pub struct HopWindow(pub generic::HopWindow<PlanRef>);

/// [`IndexScan`] is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request. Compared with [`TableScan`], it will reorder columns, and the chain node
/// doesn't allow rearrange.
#[derive(Debug, Clone)]
pub struct IndexScan {
    pub logical: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}

/// Local simple agg.
///
/// Should only be used for stateless agg, including `sum`, `count` and *append-only* `min`/`max`.
///
/// The output of `LocalSimpleAgg` doesn't have pk columns, so the result can only
/// be used by `GlobalSimpleAgg` with `ManagedValueState`s.
#[derive(Debug, Clone)]
pub struct LocalSimpleAgg(pub generic::Agg<PlanRef>);

#[derive(Debug, Clone)]
pub struct Materialize {
    /// Child of Materialize plan
    pub input: PlanRef,
    pub table: TableCatalog,
}

#[derive(Debug, Clone)]
pub struct ProjectSet(pub generic::ProjectSet<PlanRef>);

/// `Project` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone)]
pub struct Project(pub generic::Project<PlanRef>);

/// [`Sink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct Sink {
    pub input: PlanRef,
    pub properties: WithOptions,
}

/// [`Source`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct Source(pub generic::Source);

/// `TableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone)]
pub struct TableScan {
    pub logical: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}

/// `TopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct TopN(pub generic::TopN<PlanRef>);

#[derive(Clone, Debug)]
pub struct PlanBase {
    pub id: PlanNodeId,
    pub ctx: OptimizerContextRef,
    pub schema: Schema,
    pub logical_pk: Vec<usize>,
    pub dist: Distribution,
    pub append_only: bool,
    pub functional_dependency: FunctionalDependencySet,
}

impl_node!(
    PlanBase,
    Exchange,
    DynamicFilter,
    DeltaJoin,
    Expand,
    Filter,
    GlobalSimpleAgg,
    GroupTopN,
    HashAgg,
    HashJoin,
    HopWindow,
    IndexScan,
    LocalSimpleAgg,
    Materialize,
    ProjectSet,
    Project,
    Sink,
    Source,
    TableScan,
    TopN
);

pub fn to_stream_prost_body(
    (base, core): &PlanOwned,
    state: &mut BuildFragmentGraphState,
) -> pb_node::NodeBody {
    match core {
        Node::Exchange(_) => todo!(),
        Node::DynamicFilter(_) => todo!(),
        Node::DeltaJoin(_) => todo!(),
        Node::Expand(_) => todo!(),
        Node::Filter(_) => todo!(),
        Node::GlobalSimpleAgg(_) => todo!(),
        Node::GroupTopN(_) => todo!(),
        Node::HashAgg(_) => todo!(),
        Node::HashJoin(_) => todo!(),
        Node::HopWindow(_) => todo!(),
        Node::IndexScan(_) => todo!(),
        Node::LocalSimpleAgg(_) => todo!(),
        Node::Materialize(_) => todo!(),
        Node::ProjectSet(_) => todo!(),
        Node::Project(_) => todo!(),
        Node::Sink(_) => todo!(),
        Node::Source(_) => todo!(),
        Node::TableScan(_) => todo!(),
        Node::TopN(me) => {
            let TopN(me) = &**me;
            use pb::*;
            let topn_node = TopNNode {
                limit: me.limit as u64,
                offset: me.offset as u64,
                with_ties: me.with_ties,
                table: Some(
                    me.infer_internal_table_catalog(base, None)
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                order_by_len: me.order.len() as u32,
            };
            // TODO: support with ties for append only TopN
            // <https://github.com/risingwavelabs/risingwave/issues/5642>
            if me.input.0.append_only && !me.with_ties {
                pb_node::NodeBody::AppendOnlyTopN(topn_node)
            } else {
                pb_node::NodeBody::TopN(topn_node)
            }
        }
    }
}
