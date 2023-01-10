// Copyright 2023 Singularity Data
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

#![allow(rustdoc::private_intra_doc_links)]
//! Defines all kinds of node in the plan tree, each node represent a relational expression.
//!
//! We use a immutable style tree structure, every Node are immutable and cannot be modified after
//! it has been created. If you want to modify the node, such as rewriting the expression in a
//! `ProjectNode` or changing a node's input node, you need to create a new node. We use Rc as the
//! node's reference, and a node just storage its inputs' reference, so change a node just need
//! create one new node but not the entire sub-tree.
//!
//! So when you want to add a new node, make sure:
//! - each field in the node struct are private
//! - recommend to implement the construction of Node in a unified `new()` function, if have multi
//!   methods to construct, make they have a consistent behavior
//! - all field should be valued in construction, so the properties' derivation should be finished
//!   in the `new()` function.

use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use dyn_clone::{self, DynClone};
use fixedbitset::FixedBitSet;
use itertools::Itertools;
pub use logical_source::KAFKA_TIMESTAMP_COLUMN_NAME;
use paste::paste;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::batch_plan::PlanNode as BatchPlanProst;
use risingwave_pb::stream_plan::StreamNode as StreamPlanProst;
use serde::Serialize;
use smallvec::SmallVec;

use self::generic::GenericPlanRef;
use self::stream::StreamPlanRef;
use super::property::{Distribution, FunctionalDependencySet, Order};

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as
/// `dyn PlanNode`
///
/// We split the trait into lots of sub-trait so that we can easily use macro to impl them.
pub trait PlanNode:
    PlanTreeNode
    + DynClone
    + Debug
    + Display
    + Downcast
    + ColPrunable
    + ToBatch
    + ToStream
    + ToDistributedBatch
    + ToProst
    + ToLocalBatch
    + PredicatePushdown
{
    fn node_type(&self) -> PlanNodeType;
    fn plan_base(&self) -> &PlanBase;
    fn convention(&self) -> Convention;
}

impl_downcast!(PlanNode);
pub type PlanRef = Rc<dyn PlanNode>;

#[derive(Clone, Debug, Copy, Serialize, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct PlanNodeId(pub i32);

#[derive(Debug, PartialEq)]
pub enum Convention {
    Logical,
    Batch,
    Stream,
}

impl ColPrunable for PlanRef {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        if let Some(logical_share) = self.as_logical_share() {
            // Check the share cache first. If cache exists, it means this is the second round of
            // column pruning.
            if let Some((new_share, merge_required_cols)) = ctx.get_share_cache(self.id()) {
                // Piggyback share remove if its has only one parent.
                if ctx.get_parent_num(logical_share) == 1 {
                    let input: PlanRef = logical_share.input();
                    return input.prune_col(required_cols, ctx);
                }

                // If it is the first visit, recursively call `prune_col` for its input and
                // replace it.
                if ctx.visit_share_at_second_round(self.id()) {
                    let new_logical_share: &LogicalShare = new_share
                        .as_logical_share()
                        .expect("must be share operator");
                    let new_share_input = new_logical_share.input().prune_col(
                        &(0..new_logical_share.base.schema().len()).collect_vec(),
                        ctx,
                    );
                    new_logical_share.replace_input(new_share_input);
                }

                // Calculate the new required columns based on the new share.
                let new_required_cols: Vec<usize> = required_cols
                    .iter()
                    .map(|col| merge_required_cols.iter().position(|x| x == col).unwrap())
                    .collect_vec();
                let mapping = ColIndexMapping::with_remaining_columns(
                    &new_required_cols,
                    new_share.schema().len(),
                );
                return LogicalProject::with_mapping(new_share.clone(), mapping).into();
            }

            // `LogicalShare` can't clone, so we implement column pruning for `LogicalShare`
            // here.
            // Basically, we need to wait for all parents of `LogicalShare` to prune columns before
            // we merge the required columns and prune.
            let parent_has_pushed = ctx.add_required_cols(self.id(), required_cols.into());
            if parent_has_pushed == ctx.get_parent_num(logical_share) {
                let merge_require_cols = ctx
                    .take_required_cols(self.id())
                    .expect("must have required columns")
                    .into_iter()
                    .flat_map(|x| x.into_iter())
                    .sorted()
                    .dedup()
                    .collect_vec();
                let input: PlanRef = logical_share.input();
                let input = input.prune_col(&merge_require_cols, ctx);

                // Cache the new share operator for the second round.
                let new_logical_share = LogicalShare::create(input.clone());
                ctx.add_share_cache(self.id(), new_logical_share, merge_require_cols.clone());

                let exprs = logical_share
                    .base
                    .schema()
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        if let Some(pos) = merge_require_cols.iter().position(|x| *x == i) {
                            ExprImpl::InputRef(Box::new(InputRef::new(
                                pos,
                                field.data_type.clone(),
                            )))
                        } else {
                            ExprImpl::Literal(Box::new(Literal::new(None, field.data_type.clone())))
                        }
                    })
                    .collect_vec();
                let project = LogicalProject::create(input, exprs);
                logical_share.replace_input(project);
            }
            let mapping =
                ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
            LogicalProject::with_mapping(self.clone(), mapping).into()
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.prune_col(required_cols, ctx)
        }
    }
}

impl PredicatePushdown for PlanRef {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        if let Some(logical_share) = self.as_logical_share() {
            // Piggyback share remove if its has only one parent.
            if ctx.get_parent_num(logical_share) == 1 {
                let input: PlanRef = logical_share.input();
                return input.predicate_pushdown(predicate, ctx);
            }

            // `LogicalShare` can't clone, so we implement predicate pushdown for `LogicalShare`
            // here.
            // Basically, we need to wait for all parents of `LogicalShare` to push down the
            // predicate before we merge the predicates and pushdown.
            let parent_has_pushed = ctx.add_predicate(self.id(), predicate.clone());
            if parent_has_pushed == ctx.get_parent_num(logical_share) {
                let merge_predicate = ctx
                    .take_predicate(self.id())
                    .expect("must have predicate")
                    .into_iter()
                    .reduce(|a, b| a.or(b))
                    .unwrap();
                let input: PlanRef = logical_share.input();
                let input = input.predicate_pushdown(merge_predicate, ctx);
                logical_share.replace_input(input);
            }
            LogicalFilter::create(self.clone(), predicate)
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.predicate_pushdown(predicate, ctx)
        }
    }
}

impl PlanTreeNode for PlanRef {
    fn inputs(&self) -> SmallVec<[PlanRef; 2]> {
        // Dispatch to dyn PlanNode instead of PlanRef.
        let dyn_t = self.deref();
        dyn_t.inputs()
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        if let Some(logical_share) = self.clone().as_logical_share() {
            assert_eq!(inputs.len(), 1);
            // We can't clone `LogicalShare`, but only can replace input instead.
            logical_share.replace_input(inputs[0].clone());
            self.clone()
        } else {
            // Dispatch to dyn PlanNode instead of PlanRef.
            let dyn_t = self.deref();
            dyn_t.clone_with_inputs(inputs)
        }
    }
}

impl StreamPlanRef for PlanRef {
    fn distribution(&self) -> &Distribution {
        &self.plan_base().dist
    }

    fn append_only(&self) -> bool {
        self.plan_base().append_only
    }
}

impl GenericPlanRef for PlanRef {
    fn schema(&self) -> &Schema {
        &self.plan_base().schema
    }

    fn logical_pk(&self) -> &[usize] {
        &self.plan_base().logical_pk
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.plan_base().ctx()
    }
}

impl dyn PlanNode {
    /// Write explain the whole plan tree.
    pub fn explain(
        &self,
        is_last: &mut Vec<bool>,
        level: usize,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        if level > 0 {
            let mut last_iter = is_last.iter().peekable();
            while let Some(last) = last_iter.next() {
                // We are at the current level
                if last_iter.peek().is_none() {
                    if *last {
                        writeln!(f, "└─{}", self)?;
                    } else {
                        writeln!(f, "├─{}", self)?;
                    }
                } else if *last {
                    write!(f, "  ")?;
                } else {
                    write!(f, "| ")?;
                }
            }
        } else {
            writeln!(f, "{}", self)?;
        }
        let inputs = self.inputs();
        let mut inputs_iter = inputs.iter().peekable();
        while let Some(input) = inputs_iter.next() {
            let last = inputs_iter.peek().is_none();
            is_last.push(last);
            input.explain(is_last, level + 1, f)?;
            is_last.pop();
        }
        Ok(())
    }

    /// Explain the plan node and return a string.
    pub fn explain_to_string(&self) -> Result<String> {
        let mut output = String::new();
        self.explain(&mut vec![], 0, &mut output)
            .map_err(|e| ErrorCode::InternalError(format!("failed to explain: {}", e)))?;
        Ok(output)
    }

    pub fn id(&self) -> PlanNodeId {
        self.plan_base().id
    }

    pub fn ctx(&self) -> OptimizerContextRef {
        self.plan_base().ctx.clone()
    }

    pub fn schema(&self) -> &Schema {
        &self.plan_base().schema
    }

    pub fn logical_pk(&self) -> &[usize] {
        &self.plan_base().logical_pk
    }

    pub fn order(&self) -> &Order {
        &self.plan_base().order
    }

    pub fn distribution(&self) -> &Distribution {
        &self.plan_base().dist
    }

    pub fn append_only(&self) -> bool {
        self.plan_base().append_only
    }

    pub fn functional_dependency(&self) -> &FunctionalDependencySet {
        &self.plan_base().functional_dependency
    }

    pub fn watermark_columns(&self) -> &FixedBitSet {
        &self.plan_base().watermark_columns
    }

    /// Serialize the plan node and its children to a stream plan proto.
    ///
    /// Note that [`StreamTableScan`] has its own implementation of `to_stream_prost`. We have a
    /// hook inside to do some ad-hoc thing for [`StreamTableScan`].
    pub fn to_stream_prost(&self, state: &mut BuildFragmentGraphState) -> StreamPlanProst {
        if let Some(stream_table_scan) = self.as_stream_table_scan() {
            return stream_table_scan.adhoc_to_stream_prost();
        }
        if let Some(stream_index_scan) = self.as_stream_index_scan() {
            return stream_index_scan.adhoc_to_stream_prost();
        }
        if let Some(stream_share) = self.as_stream_share() {
            return stream_share.adhoc_to_stream_prost(state);
        }

        let node = Some(self.to_stream_prost_body(state));
        let input = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_stream_prost(state))
            .collect();
        // TODO: support pk_indices and operator_id
        StreamPlanProst {
            input,
            identity: format!("{}", self),
            node_body: node,
            operator_id: self.id().0 as _,
            stream_key: self.logical_pk().iter().map(|x| *x as u32).collect(),
            fields: self.schema().to_prost(),
            append_only: self.append_only(),
        }
    }

    /// Serialize the plan node and its children to a batch plan proto.
    pub fn to_batch_prost(&self) -> BatchPlanProst {
        self.to_batch_prost_identity(true)
    }

    /// Serialize the plan node and its children to a batch plan proto without the identity field
    /// (for testing).
    pub fn to_batch_prost_identity(&self, identity: bool) -> BatchPlanProst {
        let node_body = Some(self.to_batch_prost_body());
        let children = self
            .inputs()
            .into_iter()
            .map(|plan| plan.to_batch_prost_identity(identity))
            .collect();
        BatchPlanProst {
            children,
            identity: if identity {
                format!("{:?}", self)
            } else {
                "".into()
            },
            node_body,
        }
    }
}

mod plan_base;
#[macro_use]
mod plan_tree_node_v2;
pub use plan_base::*;
#[macro_use]
mod plan_tree_node;
pub use plan_tree_node::*;
mod col_pruning;
pub use col_pruning::*;
mod convert;
pub use convert::*;
mod eq_join_predicate;
pub use eq_join_predicate::*;
mod to_prost;
pub use to_prost::*;
mod predicate_pushdown;
pub use predicate_pushdown::*;

pub mod generic;
pub mod stream;
pub mod stream_derive;

pub use generic::{PlanAggCall, PlanAggCallDisplay};

mod batch_delete;
mod batch_exchange;
mod batch_expand;
mod batch_filter;
mod batch_group_topn;
mod batch_hash_agg;
mod batch_hash_join;
mod batch_hop_window;
mod batch_insert;
mod batch_limit;
mod batch_lookup_join;
mod batch_nested_loop_join;
mod batch_project;
mod batch_project_set;
mod batch_seq_scan;
mod batch_simple_agg;
mod batch_sort;
mod batch_sort_agg;
mod batch_source;
mod batch_table_function;
mod batch_topn;
mod batch_union;
mod batch_update;
mod batch_values;
mod logical_agg;
mod logical_apply;
mod logical_delete;
mod logical_expand;
mod logical_filter;
mod logical_hop_window;
mod logical_insert;
mod logical_join;
mod logical_limit;
mod logical_multi_join;
mod logical_over_agg;
mod logical_project;
mod logical_project_set;
mod logical_scan;
mod logical_share;
mod logical_source;
mod logical_table_function;
mod logical_topn;
mod logical_union;
mod logical_update;
mod logical_values;
mod stream_delta_join;
mod stream_dml;
mod stream_dynamic_filter;
mod stream_exchange;
mod stream_expand;
mod stream_filter;
mod stream_global_simple_agg;
mod stream_group_topn;
mod stream_hash_agg;
mod stream_hash_join;
mod stream_hop_window;
mod stream_index_scan;
mod stream_local_simple_agg;
mod stream_materialize;
mod stream_now;
mod stream_project;
mod stream_project_set;
mod stream_row_id_gen;
mod stream_sink;
mod stream_source;
mod stream_table_scan;
mod stream_topn;

mod stream_share;
mod stream_union;
pub mod utils;

pub use batch_delete::BatchDelete;
pub use batch_exchange::BatchExchange;
pub use batch_expand::BatchExpand;
pub use batch_filter::BatchFilter;
pub use batch_group_topn::BatchGroupTopN;
pub use batch_hash_agg::BatchHashAgg;
pub use batch_hash_join::BatchHashJoin;
pub use batch_hop_window::BatchHopWindow;
pub use batch_insert::BatchInsert;
pub use batch_limit::BatchLimit;
pub use batch_lookup_join::BatchLookupJoin;
pub use batch_nested_loop_join::BatchNestedLoopJoin;
pub use batch_project::BatchProject;
pub use batch_project_set::BatchProjectSet;
pub use batch_seq_scan::BatchSeqScan;
pub use batch_simple_agg::BatchSimpleAgg;
pub use batch_sort::BatchSort;
pub use batch_sort_agg::BatchSortAgg;
pub use batch_source::BatchSource;
pub use batch_table_function::BatchTableFunction;
pub use batch_topn::BatchTopN;
pub use batch_union::BatchUnion;
pub use batch_update::BatchUpdate;
pub use batch_values::BatchValues;
pub use logical_agg::LogicalAgg;
pub use logical_apply::LogicalApply;
pub use logical_delete::LogicalDelete;
pub use logical_expand::LogicalExpand;
pub use logical_filter::LogicalFilter;
pub use logical_hop_window::LogicalHopWindow;
pub use logical_insert::LogicalInsert;
pub use logical_join::LogicalJoin;
pub use logical_limit::LogicalLimit;
pub use logical_multi_join::{LogicalMultiJoin, LogicalMultiJoinBuilder};
pub use logical_over_agg::{LogicalOverAgg, PlanWindowFunction};
pub use logical_project::LogicalProject;
pub use logical_project_set::LogicalProjectSet;
pub use logical_scan::LogicalScan;
pub use logical_share::LogicalShare;
pub use logical_source::LogicalSource;
pub use logical_table_function::LogicalTableFunction;
pub use logical_topn::LogicalTopN;
pub use logical_union::LogicalUnion;
pub use logical_update::LogicalUpdate;
pub use logical_values::LogicalValues;
pub use stream_delta_join::StreamDeltaJoin;
pub use stream_dml::StreamDml;
pub use stream_dynamic_filter::StreamDynamicFilter;
pub use stream_exchange::StreamExchange;
pub use stream_expand::StreamExpand;
pub use stream_filter::StreamFilter;
pub use stream_global_simple_agg::StreamGlobalSimpleAgg;
pub use stream_group_topn::StreamGroupTopN;
pub use stream_hash_agg::StreamHashAgg;
pub use stream_hash_join::StreamHashJoin;
pub use stream_hop_window::StreamHopWindow;
pub use stream_index_scan::StreamIndexScan;
pub use stream_local_simple_agg::StreamLocalSimpleAgg;
pub use stream_materialize::StreamMaterialize;
pub use stream_now::StreamNow;
pub use stream_project::StreamProject;
pub use stream_project_set::StreamProjectSet;
pub use stream_row_id_gen::StreamRowIdGen;
pub use stream_share::StreamShare;
pub use stream_sink::StreamSink;
pub use stream_source::StreamSource;
pub use stream_table_scan::StreamTableScan;
pub use stream_topn::StreamTopN;
pub use stream_union::StreamUnion;

use crate::expr::{ExprImpl, InputRef, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition};

/// `for_all_plan_nodes` includes all plan nodes. If you added a new plan node
/// inside the project, be sure to add here and in its conventions like `for_logical_plan_nodes`
///
/// Every tuple has two elements, where `{ convention, name }`
/// You can use it as follows
/// ```rust
/// macro_rules! use_plan {
///     ($({ $convention:ident, $name:ident }),*) => {};
/// }
/// risingwave_frontend::for_all_plan_nodes! { use_plan }
/// ```
/// See the following implementations for example.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:ident) => {
        $macro! {
              { Logical, Agg }
            , { Logical, Apply }
            , { Logical, Filter }
            , { Logical, Project }
            , { Logical, Scan }
            , { Logical, Source }
            , { Logical, Insert }
            , { Logical, Delete }
            , { Logical, Update }
            , { Logical, Join }
            , { Logical, Values }
            , { Logical, Limit }
            , { Logical, TopN }
            , { Logical, HopWindow }
            , { Logical, TableFunction }
            , { Logical, MultiJoin }
            , { Logical, Expand }
            , { Logical, ProjectSet }
            , { Logical, Union }
            , { Logical, OverAgg }
            , { Logical, Share }
            // , { Logical, Sort } we don't need a LogicalSort, just require the Order
            , { Batch, SimpleAgg }
            , { Batch, HashAgg }
            , { Batch, SortAgg }
            , { Batch, Project }
            , { Batch, Filter }
            , { Batch, Insert }
            , { Batch, Delete }
            , { Batch, Update }
            , { Batch, SeqScan }
            , { Batch, HashJoin }
            , { Batch, NestedLoopJoin }
            , { Batch, Values }
            , { Batch, Sort }
            , { Batch, Exchange }
            , { Batch, Limit }
            , { Batch, TopN }
            , { Batch, HopWindow }
            , { Batch, TableFunction }
            , { Batch, Expand }
            , { Batch, LookupJoin }
            , { Batch, ProjectSet }
            , { Batch, Union }
            , { Batch, GroupTopN }
            , { Batch, Source }
            , { Stream, Project }
            , { Stream, Filter }
            , { Stream, TableScan }
            , { Stream, Sink }
            , { Stream, Source }
            , { Stream, HashJoin }
            , { Stream, Exchange }
            , { Stream, HashAgg }
            , { Stream, LocalSimpleAgg }
            , { Stream, GlobalSimpleAgg }
            , { Stream, Materialize }
            , { Stream, TopN }
            , { Stream, HopWindow }
            , { Stream, DeltaJoin }
            , { Stream, IndexScan }
            , { Stream, Expand }
            , { Stream, DynamicFilter }
            , { Stream, ProjectSet }
            , { Stream, GroupTopN }
            , { Stream, Union }
            , { Stream, RowIdGen }
            , { Stream, Dml }
            , { Stream, Now }
            , { Stream, Share }
        }
    };
}

/// `for_logical_plan_nodes` includes all plan nodes with logical convention.
#[macro_export]
macro_rules! for_logical_plan_nodes {
    ($macro:ident) => {
        $macro! {
              { Logical, Agg }
            , { Logical, Apply }
            , { Logical, Filter }
            , { Logical, Project }
            , { Logical, Scan }
            , { Logical, Source }
            , { Logical, Insert }
            , { Logical, Delete }
            , { Logical, Update }
            , { Logical, Join }
            , { Logical, Values }
            , { Logical, Limit }
            , { Logical, TopN }
            , { Logical, HopWindow }
            , { Logical, TableFunction }
            , { Logical, MultiJoin }
            , { Logical, Expand }
            , { Logical, ProjectSet }
            , { Logical, Union }
            , { Logical, OverAgg }
            , { Logical, Share }
            // , { Logical, Sort} not sure if we will support Order by clause in subquery/view/MV
            // if we don't support that, we don't need LogicalSort, just require the Order at the top of query
        }
    };
}

/// `for_batch_plan_nodes` includes all plan nodes with batch convention.
#[macro_export]
macro_rules! for_batch_plan_nodes {
    ($macro:ident) => {
        $macro! {
              { Batch, SimpleAgg }
            , { Batch, HashAgg }
            , { Batch, SortAgg }
            , { Batch, Project }
            , { Batch, Filter }
            , { Batch, SeqScan }
            , { Batch, HashJoin }
            , { Batch, NestedLoopJoin }
            , { Batch, Values }
            , { Batch, Limit }
            , { Batch, Sort }
            , { Batch, TopN }
            , { Batch, Exchange }
            , { Batch, Insert }
            , { Batch, Delete }
            , { Batch, Update }
            , { Batch, HopWindow }
            , { Batch, TableFunction }
            , { Batch, Expand }
            , { Batch, LookupJoin }
            , { Batch, ProjectSet }
            , { Batch, Union }
            , { Batch, GroupTopN }
            , { Batch, Source }
        }
    };
}

/// `for_stream_plan_nodes` includes all plan nodes with stream convention.
#[macro_export]
macro_rules! for_stream_plan_nodes {
    ($macro:ident) => {
        $macro! {
              { Stream, Project }
            , { Stream, Filter }
            , { Stream, HashJoin }
            , { Stream, Exchange }
            , { Stream, TableScan }
            , { Stream, Sink }
            , { Stream, Source }
            , { Stream, HashAgg }
            , { Stream, LocalSimpleAgg }
            , { Stream, GlobalSimpleAgg }
            , { Stream, Materialize }
            , { Stream, TopN }
            , { Stream, HopWindow }
            , { Stream, DeltaJoin }
            , { Stream, IndexScan }
            , { Stream, Expand }
            , { Stream, DynamicFilter }
            , { Stream, ProjectSet }
            , { Stream, GroupTopN }
            , { Stream, Union }
            , { Stream, RowIdGen }
            , { Stream, Dml }
            , { Stream, Now }
            , { Stream, Share }
        }
    };
}

/// impl [`PlanNodeType`] fn for each node.
macro_rules! enum_plan_node_type {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
            #[derive(Copy, Clone, PartialEq, Debug, Hash, Eq, Serialize)]
            pub enum PlanNodeType {
                $( [<$convention $name>] ),*
            }

            $(impl PlanNode for [<$convention $name>] {
                fn node_type(&self) -> PlanNodeType{
                    PlanNodeType::[<$convention $name>]
                }
                fn plan_base(&self) -> &PlanBase {
                    &self.base
                }
                fn convention(&self) -> Convention {
                    Convention::$convention
                }
            })*
        }
    }
}

for_all_plan_nodes! { enum_plan_node_type }

/// impl fn `plan_ref` for each node.
macro_rules! impl_plan_ref {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl From<[<$convention $name>]> for PlanRef {
                fn from(plan: [<$convention $name>]) -> Self {
                    std::rc::Rc::new(plan)
                }
            })*
        }
    }
}

for_all_plan_nodes! { impl_plan_ref }

/// impl plan node downcast fn for each node.
macro_rules! impl_down_cast_fn {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            impl dyn PlanNode {
                $( pub fn [< as_$convention:snake _ $name:snake>](&self) -> Option<&[<$convention $name>]> {
                    self.downcast_ref::<[<$convention $name>]>()
                } )*
            }
        }
    }
}

for_all_plan_nodes! { impl_down_cast_fn }
