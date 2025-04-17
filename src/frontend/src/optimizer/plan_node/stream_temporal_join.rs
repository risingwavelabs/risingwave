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
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::TemporalJoinNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_sqlparser::ast::AsOf;

use super::stream::prelude::*;
use super::utils::{Distill, childless_record, watermark_pretty};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, generic};
use crate::TableCatalog;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::plan_tree_node::PlanTreeNodeUnary;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, StreamExchange, StreamTableScan, TryToStreamPb,
};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTemporalJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,
    eq_join_predicate: EqJoinPredicate,
    append_only: bool,
    is_nested_loop: bool,
}

impl StreamTemporalJoin {
    pub fn new(
        core: generic::Join<PlanRef>,
        eq_join_predicate: EqJoinPredicate,
        is_nested_loop: bool,
    ) -> Self {
        assert!(core.join_type == JoinType::Inner || core.join_type == JoinType::LeftOuter);
        let append_only = core.left.append_only();
        assert!(!is_nested_loop || append_only);

        let right = core.right.clone();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("should be a no shuffle stream exchange");
        assert!(exchange.no_shuffle());
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");
        assert!(matches!(scan.core().as_of, Some(AsOf::ProcessTime)));

        let dist = if is_nested_loop {
            // Use right side distribution directly if it's nested loop temporal join.
            let r2o = core.r2i_col_mapping().composite(&core.i2o_col_mapping());
            r2o.rewrite_provided_distribution(core.right.distribution())
        } else {
            // Use left side distribution directly if it's hash temporal join.
            // https://github.com/risingwavelabs/risingwave/pull/19201#discussion_r1824031780
            let l2o = core.l2i_col_mapping().composite(&core.i2o_col_mapping());
            l2o.rewrite_provided_distribution(core.left.distribution())
        };

        // Use left side watermark directly.
        let watermark_columns = core
            .left
            .watermark_columns()
            .map_clone(&core.l2i_col_mapping())
            .map_clone(&core.i2o_col_mapping());

        let columns_monotonicity = core.i2o_col_mapping().rewrite_monotonicity_map(
            &core
                .l2i_col_mapping()
                .rewrite_monotonicity_map(core.left.columns_monotonicity()),
        );

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            append_only,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            columns_monotonicity,
        );

        Self {
            base,
            core,
            eq_join_predicate,
            append_only,
            is_nested_loop,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub fn append_only(&self) -> bool {
        self.append_only
    }

    pub fn is_nested_loop(&self) -> bool {
        self.eq_join_predicate().has_eq()
    }

    /// Return memo-table catalog and its `pk_indices`.
    /// (`join_key` + `left_pk` + `right_pk`) -> (`right_scan_schema` + `join_key` + `left_pk`)
    ///
    /// Write pattern:
    ///   for each left input row (with insert op), construct the memo table pk and insert the row into the memo table.
    ///
    /// Read pattern:
    ///   for each left input row (with delete op), construct pk prefix (`join_key` + `left_pk`) to fetch rows and delete them from the memo table.
    pub fn infer_memo_table_catalog(&self, right_scan: &StreamTableScan) -> TableCatalog {
        let left_eq_indexes = self.eq_join_predicate.left_eq_indexes();
        let read_prefix_len_hint = left_eq_indexes.len() + self.left().stream_key().unwrap().len();

        // Build internal table
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        // Add right table fields
        let right_scan_schema = right_scan.core().schema();
        for field in right_scan_schema.fields() {
            internal_table_catalog_builder.add_column(field);
        }
        // Add join_key + left_pk
        for field in left_eq_indexes
            .iter()
            .chain(self.core.left.stream_key().unwrap())
            .map(|idx| &self.core.left.schema().fields()[*idx])
        {
            internal_table_catalog_builder.add_column(field);
        }

        let mut pk_indices = vec![];
        pk_indices
            .extend(right_scan_schema.len()..(right_scan_schema.len() + read_prefix_len_hint));
        pk_indices.extend(right_scan.stream_key().unwrap());

        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        let dist_key_len = right_scan
            .core()
            .distribution_key()
            .map(|keys| keys.len())
            .unwrap_or(0);

        let internal_table_dist_keys =
            (right_scan_schema.len()..(right_scan_schema.len() + dist_key_len)).collect();
        internal_table_catalog_builder.build(internal_table_dist_keys, read_prefix_len_hint)
    }
}

impl Distill for StreamTemporalJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("type", Pretty::debug(&self.core.join_type)));
        vec.push(("append_only", Pretty::debug(&self.append_only)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        vec.push(("nested_loop", Pretty::debug(&self.is_nested_loop)));

        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record("StreamTemporalJoin", vec)
    }
}

impl PlanTreeNodeBinary for StreamTemporalJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core, self.eq_join_predicate.clone(), self.is_nested_loop)
    }
}

impl_plan_tree_node_for_binary! { StreamTemporalJoin }

impl TryToStreamPb for StreamTemporalJoin {
    fn try_to_stream_prost_body(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<NodeBody> {
        let left_jk_indices = self.eq_join_predicate.left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate.right_eq_indexes();
        let left_jk_indices_prost = left_jk_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_jk_indices_prost = right_jk_indices.iter().map(|idx| *idx as i32).collect_vec();

        let null_safe_prost = self.eq_join_predicate.null_safes().into_iter().collect();

        let right = self.right();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("should be a no shuffle stream exchange");
        assert!(exchange.no_shuffle());
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");

        Ok(NodeBody::TemporalJoin(Box::new(TemporalJoinNode {
            join_type: self.core.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            table_desc: Some(scan.core().table_desc.try_to_protobuf()?),
            table_output_indices: scan.core().output_col_idx.iter().map(|&i| i as _).collect(),
            memo_table: if self.append_only {
                None
            } else {
                let mut memo_table = self.infer_memo_table_catalog(scan);
                memo_table = memo_table.with_id(state.gen_table_id_wrapped());
                Some(memo_table.to_internal_table_prost())
            },
            is_nested_loop: self.is_nested_loop,
        })))
    }
}

impl ExprRewritable for StreamTemporalJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(
            core,
            self.eq_join_predicate.rewrite_exprs(r),
            self.is_nested_loop,
        )
        .into()
    }
}

impl ExprVisitable for StreamTemporalJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
        self.eq_join_predicate.visit_exprs(v);
    }
}
