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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{self, PlanAggCall};
use super::stream::prelude::*;
use super::utils::{childless_record, plan_node_name, Distill};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSimpleAgg {
    pub base: PlanBase<Stream>,
    core: generic::Agg<PlanRef>,

    /// The index of `count(*)` in `agg_calls`.
    row_count_idx: usize,
}

impl StreamSimpleAgg {
    pub fn new(core: generic::Agg<PlanRef>, row_count_idx: usize) -> Self {
        assert_eq!(core.agg_calls[row_count_idx], PlanAggCall::count_star());

        let input = core.input.clone();
        let input_dist = input.distribution();
        let dist = match input_dist {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // Empty because watermark column(s) must be in group key and simple agg have no group key.
        let watermark_columns = FixedBitSet::with_capacity(core.output_len());

        // Simple agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_core(&core, dist, false, false, watermark_columns);
        StreamSimpleAgg {
            base,
            core,
            row_count_idx,
        }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }
}

impl Distill for StreamSimpleAgg {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let name = plan_node_name!("StreamSimpleAgg",
            { "append_only", self.input().append_only() },
        );
        childless_record(name, self.core.fields_pretty())
    }
}

impl PlanTreeNodeUnary for StreamSimpleAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = generic::Agg {
            input,
            ..self.core.clone()
        };
        Self::new(logical, self.row_count_idx)
    }
}
impl_plan_tree_node_for_unary! { StreamSimpleAgg }

impl StreamNode for StreamSimpleAgg {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        let (intermediate_state_table, agg_states, distinct_dedup_tables) =
            self.core.infer_tables(&self.base, None, None);

        PbNodeBody::SimpleAgg(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            distribution_key: self
                .base
                .distribution()
                .dist_column_indices()
                .iter()
                .map(|idx| *idx as u32)
                .collect(),
            is_append_only: self.input().append_only(),
            agg_call_states: agg_states
                .into_iter()
                .map(|s| s.into_prost(state))
                .collect(),
            intermediate_state_table: Some(
                intermediate_state_table
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            distinct_dedup_tables: distinct_dedup_tables
                .into_iter()
                .sorted_by_key(|(i, _)| *i)
                .map(|(key_idx, table)| {
                    (
                        key_idx as u32,
                        table
                            .with_id(state.gen_table_id_wrapped())
                            .to_internal_table_prost(),
                    )
                })
                .collect(),
            row_count_index: self.row_count_idx as u32,
            version: PbAggNodeVersion::Issue13465 as _,
        })
    }
}

impl ExprRewritable for StreamSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.row_count_idx).into()
    }
}

impl ExprVisitable for StreamSimpleAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
