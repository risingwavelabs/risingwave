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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::{FieldDisplay};
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::TemporalJoinNode;

use super::utils::formatter_debug_plan_node;
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, StreamNode};
use crate::expr::{Expr, ExprRewriter};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::plan_tree_node::PlanTreeNodeUnary;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{
    EqJoinPredicate, EqJoinPredicateDisplay, StreamExchange, StreamTableScan,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTemporalJoin {
    pub base: PlanBase,
    logical: generic::Join<PlanRef>,
    eq_join_predicate: EqJoinPredicate,
}

impl StreamTemporalJoin {
    pub fn new(logical: generic::Join<PlanRef>, eq_join_predicate: EqJoinPredicate) -> Self {
        assert!(logical.join_type == JoinType::Inner || logical.join_type == JoinType::LeftOuter);
        assert!(logical.left.append_only());
        assert!(logical.right.logical_pk() == eq_join_predicate.right_eq_indexes());
        let right = logical.right.clone();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("should be a no shuffle stream exchange");
        assert!(exchange.no_shuffle());
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");
        assert!(scan.logical().for_system_time_as_of_proctime);

        let l2o = logical
            .l2i_col_mapping()
            .composite(&logical.i2o_col_mapping());
        let dist = l2o.rewrite_provided_distribution(logical.left.distribution());

        // Use left side watermark directly.
        let watermark_columns = logical.i2o_col_mapping().rewrite_bitset(
            &logical
                .l2i_col_mapping()
                .rewrite_bitset(logical.left.watermark_columns()),
        );

        let base = PlanBase::new_stream_with_logical(
            &logical,
            dist,
            true,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
        );

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.logical.join_type
    }

    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for StreamTemporalJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = formatter_debug_plan_node!(f, "StreamTemporalJoin");

        let verbose = self.base.ctx.is_explain_verbose();
        builder.field("type", &self.logical.join_type);

        let concat_schema = self.logical.concat_schema();
        builder.field(
            "predicate",
            &EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            },
        );

        let watermark_columns = &self.base.watermark_columns;
        if self.base.watermark_columns.count_ones(..) > 0 {
            let schema = self.schema();
            builder.field(
                "output_watermarks",
                &watermark_columns
                    .ones()
                    .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
                    .collect_vec(),
            );
        };

        if verbose {
            match IndicesDisplay::from(
                &self.logical.output_indices,
                self.logical.internal_column_num(),
                &concat_schema,
            ) {
                None => builder.field("output", &format_args!("all")),
                Some(id) => builder.field("output", &id),
            };
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for StreamTemporalJoin {
    fn left(&self) -> PlanRef {
        self.logical.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.logical.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.left = left;
        logical.right = right;
        Self::new(logical, self.eq_join_predicate.clone())
    }
}

impl_plan_tree_node_for_binary! { StreamTemporalJoin }

impl StreamNode for StreamTemporalJoin {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
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

        NodeBody::TemporalJoin(TemporalJoinNode {
            join_type: self.logical.join_type as i32,
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            condition: self
                .eq_join_predicate
                .other_cond()
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
            table_desc: Some(scan.logical().table_desc.to_protobuf()),
            table_output_indices: scan
                .logical()
                .output_col_idx
                .iter()
                .map(|&i| i as _)
                .collect(),
        })
    }
}

impl ExprRewritable for StreamTemporalJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical, self.eq_join_predicate.rewrite_exprs(r)).into()
    }
}
