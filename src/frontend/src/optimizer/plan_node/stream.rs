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

use derivative::Derivative;
use generic::PlanAggCall;
use pb::stream_node as pb_node;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_pb::stream_plan as pb;
use smallvec::SmallVec;

use super::generic::{GenericPlanNode, GenericPlanRef};
use super::utils::TableCatalogBuilder;
use super::{generic, EqJoinPredicate, PlanNodeId};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::plan_tree_node_v2::PlanTreeNodeV2;
use crate::optimizer::property::{Distribution, FieldOrder};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

macro_rules! impl_node {
($base:ident, $($t:ident),*) => {
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub enum Node {
        $($t(Box<$t>),)*
    }
    pub type PlanOwned = ($base, Node);
    pub type PlanRef = std::rc::Rc<PlanOwned>;
    $(
    impl From<$t> for PlanRef {
        fn from(o: $t) -> PlanRef {
            std::rc::Rc::new((o.to_stream_base(),  Node::$t(Box::new(o))))
        }
    }
    )*
    impl PlanTreeNodeV2 for PlanRef {
        type PlanRef = PlanRef;

        fn inputs(&self) -> SmallVec<[Self::PlanRef; 2]> {
            match &self.1 {
                $(Node::$t(inner) => inner.inputs(),)*
            }
        }
        fn clone_with_inputs(&self, inputs: impl Iterator<Item = Self::PlanRef>) -> Self {
            match &self.1 {
                $(Node::$t(inner) => inner.clone_with_inputs(inputs).into(),)*
            }
        }

    }
};
}

pub trait StreamPlanNode: GenericPlanNode {
    fn distribution(&self) -> Distribution;
    fn append_only(&self) -> bool;
    fn to_stream_base(&self) -> PlanBase {
        let ctx = self.ctx();
        PlanBase {
            id: ctx.next_plan_node_id(),
            ctx,
            schema: self.schema(),
            logical_pk: self.logical_pk().unwrap_or_default(),
            dist: self.distribution(),
            append_only: self.append_only(),
        }
    }
}

pub trait StreamPlanRef: GenericPlanRef {
    fn distribution(&self) -> &Distribution;
    fn append_only(&self) -> bool;
}

impl generic::GenericPlanRef for PlanRef {
    fn schema(&self) -> &Schema {
        &self.0.schema
    }

    fn logical_pk(&self) -> &[usize] {
        &self.0.logical_pk
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.0.ctx.clone()
    }
}

impl generic::GenericPlanRef for PlanBase {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn logical_pk(&self) -> &[usize] {
        &self.logical_pk
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}

impl StreamPlanRef for PlanBase {
    fn distribution(&self) -> &Distribution {
        &self.dist
    }

    fn append_only(&self) -> bool {
        self.append_only
    }
}

impl StreamPlanRef for PlanRef {
    fn distribution(&self) -> &Distribution {
        &self.0.dist
    }

    fn append_only(&self) -> bool {
        self.0.append_only
    }
}

/// Implements [`generic::Join`] with delta join. It requires its two
/// inputs to be indexes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeltaJoin {
    pub core: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,
}
impl_plan_tree_node_v2_for_stream_binary_node_with_core_delegating!(DeltaJoin, core, left, right);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DynamicFilter {
    pub core: generic::DynamicFilter<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_binary_node_with_core_delegating!(
    DynamicFilter,
    core,
    left,
    right
);
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Exchange {
    pub dist: Distribution,
    pub input: PlanRef,
}
impl_plan_tree_node_v2_for_stream_unary_node!(Exchange, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expand {
    pub core: generic::Expand<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(Expand, core, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Filter {
    pub core: generic::Filter<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(Filter, core, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GlobalSimpleAgg {
    pub core: generic::Agg<PlanRef>,
    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(GlobalSimpleAgg, core, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupTopN {
    pub core: generic::TopN<PlanRef>,
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    pub vnode_col_idx: Option<usize>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(GroupTopN, core, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashAgg {
    pub core: generic::Agg<PlanRef>,
    /// An optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution.
    vnode_col_idx: Option<usize>,
    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(HashAgg, core, input);

/// Implements [`generic::Join`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashJoin {
    pub core: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    pub eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    pub is_append_only: bool,
}
impl_plan_tree_node_v2_for_stream_binary_node_with_core_delegating!(HashJoin, core, left, right);

impl HashJoin {
    /// Return hash join internal table catalog and degree table catalog.
    pub fn infer_internal_and_degree_table_catalog(
        input: &impl StreamPlanRef,
        join_key_indices: Vec<usize>,
    ) -> (TableCatalog, TableCatalog, Vec<usize>) {
        let schema = input.schema();

        let internal_table_dist_keys = input.distribution().dist_column_indices().to_vec();

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
        let join_key_len = join_key_indices.len();
        let mut pk_indices = join_key_indices;

        // dedup the pk in dist key..
        let mut deduped_input_pk_indices = vec![];
        for input_pk_idx in input.logical_pk() {
            if !pk_indices.contains(input_pk_idx)
                && !deduped_input_pk_indices.contains(input_pk_idx)
            {
                deduped_input_pk_indices.push(*input_pk_idx);
            }
        }

        pk_indices.extend(deduped_input_pk_indices.clone());

        // Build internal table
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(input.ctx().with_options().internal_table_subset());
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending)
        });

        // Build degree table.
        let mut degree_table_catalog_builder =
            TableCatalogBuilder::new(input.ctx().with_options().internal_table_subset());

        let degree_column_field = Field::with_name(DataType::Int64, "_degree");

        pk_indices.iter().enumerate().for_each(|(order_idx, idx)| {
            degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
            degree_table_catalog_builder.add_order_column(order_idx, OrderType::Ascending);
        });
        degree_table_catalog_builder.add_column(&degree_column_field);
        degree_table_catalog_builder
            .set_value_indices(vec![degree_table_catalog_builder.columns().len() - 1]);

        internal_table_catalog_builder.set_read_prefix_len_hint(join_key_len);
        degree_table_catalog_builder.set_read_prefix_len_hint(join_key_len);
        (
            internal_table_catalog_builder.build(internal_table_dist_keys),
            degree_table_catalog_builder.build(degree_table_dist_keys),
            deduped_input_pk_indices,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HopWindow {
    pub core: generic::HopWindow<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(HopWindow, core, input);

/// [`IndexScan`] is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request. Compared with [`TableScan`], it will reorder columns, and the chain node
/// doesn't allow rearrange.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexScan {
    pub core: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}
impl_plan_tree_node_v2_for_stream_leaf_node!(IndexScan);
/// Local simple agg.
///
/// Should only be used for stateless agg, including `sum`, `count` and *append-only* `min`/`max`.
///
/// The output of `LocalSimpleAgg` doesn't have pk columns, so the result can only
/// be used by `GlobalSimpleAgg` with `ManagedValueState`s.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalSimpleAgg {
    pub core: generic::Agg<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(LocalSimpleAgg, core, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Materialize {
    /// Child of Materialize plan
    pub input: PlanRef,
    pub table: TableCatalog,
}
impl_plan_tree_node_v2_for_stream_unary_node!(Materialize, input);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet {
    pub core: generic::ProjectSet<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(ProjectSet, core, input);

/// `Project` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Project {
    pub core: generic::Project<PlanRef>,
    watermark_derivations: Vec<(usize, usize)>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(Project, core, input);

/// [`Sink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sink {
    pub input: PlanRef,
    pub sink_desc: SinkDesc,
}
impl_plan_tree_node_v2_for_stream_unary_node!(Sink, input);
/// [`Source`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Source {
    pub core: generic::Source,
}
impl_plan_tree_node_v2_for_stream_leaf_node!(Source);

/// `TableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableScan {
    pub core: generic::Scan,
    pub batch_plan_id: PlanNodeId,
}
impl_plan_tree_node_v2_for_stream_leaf_node!(TableScan);

/// `TopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopN {
    pub core: generic::TopN<PlanRef>,
}
impl_plan_tree_node_v2_for_stream_unary_node_with_core_delegating!(TopN, core, input);

#[derive(Clone, Debug, Derivative)]
#[derivative(PartialEq, Eq, Hash)]
pub struct PlanBase {
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub id: PlanNodeId,
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub ctx: OptimizerContextRef,
    pub schema: Schema,
    pub logical_pk: Vec<usize>,
    #[derivative(PartialEq = "ignore")]
    #[derivative(Hash = "ignore")]
    pub dist: Distribution,
    pub append_only: bool,
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

use pb_node::NodeBody as ProstNode;
#[allow(dead_code)]
pub fn to_stream_prost_body(
    (base, core): &PlanOwned,
    state: &mut BuildFragmentGraphState,
) -> ProstNode {
    use pb::*;
    match core {
        Node::TableScan(_) => todo!(),
        Node::IndexScan(_) => todo!(),
        // ^ need standalone implementations
        Node::Exchange(_) => ProstNode::Exchange(ExchangeNode {
            strategy: Some(DispatchStrategy {
                r#type: match &base.dist {
                    Distribution::HashShard(_) => DispatcherType::Hash,
                    Distribution::Single => DispatcherType::Simple,
                    Distribution::Broadcast => DispatcherType::Broadcast,
                    _ => panic!("Do not allow Any or AnyShard in serialization process"),
                } as i32,
                dist_key_indices: match &base.dist {
                    Distribution::HashShard(keys) => keys.iter().map(|&num| num as u32).collect(),
                    _ => vec![],
                },
                output_indices: (0..base.schema().len() as u32).collect(),
            }),
        }),
        Node::DynamicFilter(me) => {
            use generic::dynamic_filter::*;
            let me = &me.core;
            let condition = me
                .predicate()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto());
            let left_table = infer_left_internal_table_catalog(base, me.left_index)
                .with_id(state.gen_table_id_wrapped());
            let right_table = infer_right_internal_table_catalog(&me.right.0)
                .with_id(state.gen_table_id_wrapped());
            ProstNode::DynamicFilter(DynamicFilterNode {
                left_key: me.left_index as u32,
                condition,
                left_table: Some(left_table.to_internal_table_prost()),
                right_table: Some(right_table.to_internal_table_prost()),
            })
        }
        Node::DeltaJoin(me) => {
            let (_, left_node) = &*me.core.left;
            let (_, right_node) = &*me.core.right;
            fn cast(node: &Node) -> &IndexScan {
                match node {
                    Node::IndexScan(scan) => scan,
                    _ => unreachable!(),
                }
            }
            let left_table = cast(left_node);
            let right_table = cast(right_node);
            let left_table_desc = &*left_table.core.table_desc;
            let right_table_desc = &*right_table.core.table_desc;

            // TODO: add a separate delta join node in proto, or move fragmenter to frontend so that
            // we don't need an intermediate representation.
            ProstNode::DeltaIndexJoin(DeltaIndexJoinNode {
                join_type: me.core.join_type as i32,
                left_key: me
                    .eq_join_predicate
                    .left_eq_indexes()
                    .iter()
                    .map(|v| *v as i32)
                    .collect(),
                right_key: me
                    .eq_join_predicate
                    .right_eq_indexes()
                    .iter()
                    .map(|v| *v as i32)
                    .collect(),
                condition: me
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                left_table_id: left_table_desc.table_id.table_id(),
                right_table_id: right_table_desc.table_id.table_id(),
                left_info: Some(ArrangementInfo {
                    arrange_key_orders: left_table_desc.arrange_key_orders_prost(),
                    column_descs: left_table
                        .core
                        .column_descs()
                        .iter()
                        .map(ColumnDesc::to_protobuf)
                        .collect(),
                    table_desc: Some(left_table_desc.to_protobuf()),
                }),
                right_info: Some(ArrangementInfo {
                    arrange_key_orders: right_table_desc.arrange_key_orders_prost(),
                    column_descs: right_table
                        .core
                        .column_descs()
                        .iter()
                        .map(ColumnDesc::to_protobuf)
                        .collect(),
                    table_desc: Some(right_table_desc.to_protobuf()),
                }),
                output_indices: me.core.output_indices.iter().map(|&x| x as u32).collect(),
            })
        }
        Node::Expand(me) => {
            use pb::expand_node::Subset;

            let me = &me.core;
            ProstNode::Expand(ExpandNode {
                column_subsets: me
                    .column_subsets
                    .iter()
                    .map(|subset| {
                        let column_indices = subset.iter().map(|&key| key as u32).collect();
                        Subset { column_indices }
                    })
                    .collect(),
            })
        }
        Node::Filter(me) => {
            let me = &me.core;
            ProstNode::Filter(FilterNode {
                search_condition: Some(ExprImpl::from(me.predicate.clone()).to_expr_proto()),
            })
        }
        Node::GlobalSimpleAgg(me) => {
            let result_table = me.core.infer_result_table(base, None);
            let agg_states = me.core.infer_stream_agg_state(base, None);
            let distinct_dedup_tables = me.core.infer_distinct_dedup_tables(base, None);

            ProstNode::GlobalSimpleAgg(SimpleAggNode {
                agg_calls: me
                    .core
                    .agg_calls
                    .iter()
                    .map(PlanAggCall::to_protobuf)
                    .collect(),
                row_count_index: me.row_count_idx as u32,
                distribution_key: base
                    .dist
                    .dist_column_indices()
                    .iter()
                    .map(|&idx| idx as u32)
                    .collect(),
                is_append_only: me.core.input.0.append_only,
                agg_call_states: agg_states
                    .into_iter()
                    .map(|s| s.into_prost(state))
                    .collect(),
                result_table: Some(
                    result_table
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                distinct_dedup_tables: distinct_dedup_tables
                    .into_iter()
                    .map(|(key_idx, table)| (key_idx as u32, table.to_internal_table_prost()))
                    .collect(),
            })
        }
        Node::GroupTopN(me) => {
            let table = me
                .core
                .infer_internal_table_catalog(base, me.vnode_col_idx)
                .with_id(state.gen_table_id_wrapped());
            let group_topn_node = GroupTopNNode {
                limit: me.core.limit,
                offset: me.core.offset,
                with_ties: me.core.with_ties,
                group_key: me.core.group_key.iter().map(|idx| *idx as u32).collect(),
                table: Some(table.to_internal_table_prost()),
                order_by: me.core.order.to_protobuf(),
            };

            ProstNode::GroupTopN(group_topn_node)
        }
        Node::HashAgg(me) => {
            let result_table = me.core.infer_result_table(base, me.vnode_col_idx);
            let agg_states = me.core.infer_stream_agg_state(base, me.vnode_col_idx);
            let distinct_dedup_tables = me.core.infer_distinct_dedup_tables(base, me.vnode_col_idx);

            ProstNode::HashAgg(HashAggNode {
                group_key: me.core.group_key.iter().map(|&idx| idx as u32).collect(),
                agg_calls: me
                    .core
                    .agg_calls
                    .iter()
                    .map(PlanAggCall::to_protobuf)
                    .collect(),
                row_count_index: me.row_count_idx as u32,
                is_append_only: me.core.input.0.append_only,
                agg_call_states: agg_states
                    .into_iter()
                    .map(|s| s.into_prost(state))
                    .collect(),
                result_table: Some(
                    result_table
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                distinct_dedup_tables: distinct_dedup_tables
                    .into_iter()
                    .map(|(key_idx, table)| (key_idx as u32, table.to_internal_table_prost()))
                    .collect(),
            })
        }
        Node::HashJoin(me) => {
            let left_key_indices = me.eq_join_predicate.left_eq_indexes();
            let right_key_indices = me.eq_join_predicate.right_eq_indexes();
            let left_key_indices_prost = left_key_indices.iter().map(|&idx| idx as i32).collect();
            let right_key_indices_prost = right_key_indices.iter().map(|&idx| idx as i32).collect();

            let (left_table, left_degree_table, left_deduped_input_pk_indices) =
                HashJoin::infer_internal_and_degree_table_catalog(
                    &me.core.left.0,
                    left_key_indices,
                );
            let (right_table, right_degree_table, right_deduped_input_pk_indices) =
                HashJoin::infer_internal_and_degree_table_catalog(
                    &me.core.right.0,
                    right_key_indices,
                );

            let left_deduped_input_pk_indices = left_deduped_input_pk_indices
                .iter()
                .map(|idx| *idx as u32)
                .collect();

            let right_deduped_input_pk_indices = right_deduped_input_pk_indices
                .iter()
                .map(|idx| *idx as u32)
                .collect();

            let (left_table, left_degree_table) = (
                left_table.with_id(state.gen_table_id_wrapped()),
                left_degree_table.with_id(state.gen_table_id_wrapped()),
            );
            let (right_table, right_degree_table) = (
                right_table.with_id(state.gen_table_id_wrapped()),
                right_degree_table.with_id(state.gen_table_id_wrapped()),
            );

            let null_safe_prost = me.eq_join_predicate.null_safes().into_iter().collect();

            ProstNode::HashJoin(HashJoinNode {
                join_type: me.core.join_type as i32,
                left_key: left_key_indices_prost,
                right_key: right_key_indices_prost,
                null_safe: null_safe_prost,
                condition: me
                    .eq_join_predicate
                    .other_cond()
                    .as_expr_unless_true()
                    .map(|x| x.to_expr_proto()),
                left_table: Some(left_table.to_internal_table_prost()),
                right_table: Some(right_table.to_internal_table_prost()),
                left_degree_table: Some(left_degree_table.to_internal_table_prost()),
                right_degree_table: Some(right_degree_table.to_internal_table_prost()),
                left_deduped_input_pk_indices,
                right_deduped_input_pk_indices,
                output_indices: me.core.output_indices.iter().map(|&x| x as u32).collect(),
                is_append_only: me.is_append_only,
            })
        }
        Node::HopWindow(me) => {
            let me = &me.core;
            ProstNode::HopWindow(HopWindowNode {
                time_col: me.time_col.to_proto(),
                window_slide: Some(me.window_slide.into()),
                window_size: Some(me.window_size.into()),
                output_indices: me.output_indices.iter().map(|&x| x as u32).collect(),
            })
        }
        Node::LocalSimpleAgg(me) => {
            let me = &me.core;
            ProstNode::LocalSimpleAgg(SimpleAggNode {
                agg_calls: me.agg_calls.iter().map(PlanAggCall::to_protobuf).collect(),
                row_count_index: u32::MAX, // this is not used
                distribution_key: base
                    .dist
                    .dist_column_indices()
                    .iter()
                    .map(|&idx| idx as u32)
                    .collect(),
                agg_call_states: vec![],
                result_table: None,
                is_append_only: me.input.0.append_only,
                distinct_dedup_tables: Default::default(),
            })
        }
        Node::Materialize(me) => {
            ProstNode::Materialize(MaterializeNode {
                // We don't need table id for materialize node in frontend. The id will be generated
                // on meta catalog service.
                table_id: 0,
                column_orders: me.table.pk().iter().map(FieldOrder::to_protobuf).collect(),
                table: Some(me.table.to_internal_table_prost()),
                handle_pk_conflict_behavior: 0,
            })
        }
        Node::ProjectSet(me) => {
            let me = &me.core;
            let select_list = me
                .select_list
                .iter()
                .map(ExprImpl::to_project_set_select_item_proto)
                .collect();
            ProstNode::ProjectSet(ProjectSetNode { select_list })
        }
        Node::Project(me) => ProstNode::Project(ProjectNode {
            select_list: me.core.exprs.iter().map(|x| x.to_expr_proto()).collect(),
            watermark_input_key: me
                .watermark_derivations
                .iter()
                .map(|(x, _)| *x as u32)
                .collect(),
            watermark_output_key: me
                .watermark_derivations
                .iter()
                .map(|(_, y)| *y as u32)
                .collect(),
        }),
        Node::Sink(me) => ProstNode::Sink(SinkNode {
            sink_desc: Some(me.sink_desc.to_proto()),
        }),
        Node::Source(me) => {
            let me = &me.core.catalog;
            let source_inner = me.as_ref().map(|me| StreamSource {
                source_id: me.id,
                source_name: me.name.clone(),
                state_table: Some(
                    generic::Source::infer_internal_table_catalog()
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                info: Some(me.info.clone()),
                row_id_index: me.row_id_index.map(|index| index as _),
                columns: me.columns.iter().map(|c| c.to_protobuf()).collect(),
                pk_column_ids: me.pk_col_ids.iter().map(Into::into).collect(),
                properties: me.properties.clone().into_iter().collect(),
            });
            ProstNode::Source(SourceNode { source_inner })
        }
        Node::TopN(me) => {
            let me = &me.core;
            let topn_node = TopNNode {
                limit: me.limit,
                offset: me.offset,
                with_ties: me.with_ties,
                table: Some(
                    me.infer_internal_table_catalog(base, None)
                        .with_id(state.gen_table_id_wrapped())
                        .to_internal_table_prost(),
                ),
                order_by: me.order.to_protobuf(),
            };
            // TODO: support with ties for append only TopN
            // <https://github.com/risingwavelabs/risingwave/issues/5642>
            if me.input.0.append_only && !me.with_ties {
                ProstNode::AppendOnlyTopN(topn_node)
            } else {
                ProstNode::TopN(topn_node)
            }
        }
    }
}
