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
use risingwave_common::catalog::{FieldDisplay, Schema};
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::TemporalJoinNode;

use super::{ExprRewritable, LogicalJoin, PlanBase, PlanRef, PlanTreeNodeBinary, StreamNode};
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
    logical: LogicalJoin,
    eq_join_predicate: EqJoinPredicate,
}

impl StreamTemporalJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        assert!(
            logical.join_type() == JoinType::Inner || logical.join_type() == JoinType::LeftOuter
        );
        assert!(logical.left().append_only());
        assert!(logical.right().logical_pk() == eq_join_predicate.right_eq_indexes());
        let right = logical.right();
        let exchange: &StreamExchange = right
            .as_stream_exchange()
            .expect("should be a no shuffle stream exchange");
        assert!(exchange.no_shuffle());
        let exchange_input = exchange.input();
        let scan: &StreamTableScan = exchange_input
            .as_stream_table_scan()
            .expect("should be a stream table scan");
        assert!(scan.logical().for_system_time_as_of_now());

        let ctx = logical.base.ctx.clone();
        let l2o = logical
            .l2i_col_mapping()
            .composite(&logical.i2o_col_mapping());
        let dist = l2o.rewrite_provided_distribution(logical.left().distribution());

        // Use left side watermark directly.
        let watermark_columns = logical.i2o_col_mapping().rewrite_bitset(
            &logical
                .l2i_col_mapping()
                .rewrite_bitset(logical.left().watermark_columns()),
        );

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.logical_pk.to_vec(),
            logical.functional_dependency().clone(),
            dist,
            true,
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
        self.logical.join_type()
    }

    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for StreamTemporalJoin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamTemporalJoin");

        let verbose = self.base.ctx.is_explain_verbose();
        builder.field("type", &self.logical.join_type());

        let mut concat_schema = self.left().schema().fields.clone();
        concat_schema.extend(self.right().schema().fields.clone());
        let concat_schema = Schema::new(concat_schema);
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
            if self
                .logical
                .output_indices()
                .iter()
                .copied()
                .eq(0..self.logical.internal_column_num())
            {
                builder.field("output", &format_args!("all"));
            } else {
                builder.field(
                    "output",
                    &IndicesDisplay {
                        indices: self.logical.output_indices(),
                        input_schema: &concat_schema,
                    },
                );
            }
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for StreamTemporalJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }

    fn right(&self) -> PlanRef {
        self.logical.right()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
        )
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
            join_type: self.logical.join_type() as i32,
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
                .output_indices()
                .iter()
                .map(|&x| x as u32)
                .collect(),
            table_desc: Some(scan.logical().table_desc().to_protobuf()),
            table_output_indices: scan
                .logical()
                .output_col_idx()
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
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_join()
                .unwrap()
                .clone(),
            self.eq_join_predicate.rewrite_exprs(r),
        )
        .into()
    }
}
