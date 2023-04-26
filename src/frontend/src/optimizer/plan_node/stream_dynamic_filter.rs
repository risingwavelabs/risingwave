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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::FieldDisplay;
pub use risingwave_pb::expr::expr_node::Type as ExprType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::DynamicFilterNode;

use super::utils::IndicesDisplay;
use super::{generic, ExprRewritable};
use crate::expr::Expr;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNodeBinary, StreamNode};
use crate::optimizer::PlanRef;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDynamicFilter {
    pub base: PlanBase,
    core: generic::DynamicFilter<PlanRef>,
}

impl StreamDynamicFilter {
    pub fn new(left_index: usize, comparator: ExprType, left: PlanRef, right: PlanRef) -> Self {
        assert_eq!(right.schema().len(), 1);

        let watermark_columns = {
            let mut watermark_columns = FixedBitSet::with_capacity(left.schema().len());
            if right.watermark_columns()[0] {
                match comparator {
                    ExprType::Equal | ExprType::GreaterThan | ExprType::GreaterThanOrEqual => {
                        watermark_columns.set(left_index, true)
                    }
                    _ => {}
                }
            }
            watermark_columns
        };

        // TODO: derive from input
        let base = PlanBase::new_stream(
            left.ctx(),
            left.schema().clone(),
            left.logical_pk().to_vec(),
            left.functional_dependency().clone(),
            left.distribution().clone(),
            false, /* we can have a new abstraction for append only and monotonically increasing
                    * in the future */
            watermark_columns,
        );
        let core = generic::DynamicFilter {
            comparator,
            left_index,
            left,
            right,
        };
        Self { base, core }
    }

    pub fn left_index(&self) -> usize {
        self.core.left_index
    }
}

impl fmt::Display for StreamDynamicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("StreamDynamicFilter");

        self.core.fmt_fields_with_builder(&mut builder);
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
            // For now, output all columns from the left side. Make it explicit here.
            builder.field(
                "output",
                &IndicesDisplay {
                    indices: &(0..self.schema().fields.len()).collect_vec(),
                    input_schema: self.schema(),
                },
            );
        }

        builder.finish()
    }
}

impl PlanTreeNodeBinary for StreamDynamicFilter {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.core.left_index, self.core.comparator, left, right)
    }
}

impl_plan_tree_node_for_binary! { StreamDynamicFilter }

impl StreamNode for StreamDynamicFilter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        use generic::dynamic_filter::*;
        let condition = self
            .core
            .predicate()
            .as_expr_unless_true()
            .map(|x| x.to_expr_proto());
        let left_index = self.core.left_index;
        let left_table = infer_left_internal_table_catalog(&self.base, left_index)
            .with_id(state.gen_table_id_wrapped());
        let right = self.right();
        let right_table = infer_right_internal_table_catalog(right.plan_base())
            .with_id(state.gen_table_id_wrapped());
        NodeBody::DynamicFilter(DynamicFilterNode {
            left_key: left_index as u32,
            condition,
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
        })
    }
}

impl ExprRewritable for StreamDynamicFilter {}
