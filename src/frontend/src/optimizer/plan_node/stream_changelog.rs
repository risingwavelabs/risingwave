// Copyright 2024 RisingWave Labs
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

use risingwave_pb::stream_plan::ChangeLogNode;
use risingwave_pb::stream_plan::change_log_node::{Keyed, Mode};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::expr_visitable::ExprVisitable;
use super::stream::StreamPlanNodeMetadata;
use super::stream::prelude::PhysicalPlanRef;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, generic};
use crate::optimizer::StreamPlanRef as PlanRef;
use crate::optimizer::property::{Distribution, MonotonicityMap, StreamKind};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamChangeLog {
    pub base: PlanBase<Stream>,
    core: generic::ChangeLog<PlanRef>,
    distribution_keys: Vec<u32>,
}

impl StreamChangeLog {
    pub fn new_with_dist(
        core: generic::ChangeLog<PlanRef>,
        dist: Distribution,
        distribution_keys: Vec<u32>,
    ) -> Self {
        let input = core.input.clone();
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            // The changelog will convert all delete/update to insert, so it must be append-only here.
            StreamKind::AppendOnly,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            MonotonicityMap::new(), // TODO: derive monotonicity
        );
        StreamChangeLog {
            base,
            core,
            distribution_keys,
        }
    }
}

impl PlanTreeNodeUnary<Stream> for StreamChangeLog {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new_with_dist(
            core,
            self.base.distribution().clone(),
            self.distribution_keys.clone(),
        )
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamChangeLog }
impl_distill_by_unit!(StreamChangeLog, core, "StreamChangeLog");

impl StreamNode for StreamChangeLog {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let mode = self.core.key_indices.as_ref().map(|key| {
            Mode::Keyed(Keyed {
                additional_key_columns: key
                    .iter()
                    .map(|&index| index as u32)
                    .filter(|index| !self.distribution_keys.contains(index))
                    .collect(),
            })
        });
        PbNodeBody::Changelog(Box::new(ChangeLogNode {
            need_op: self.core.need_op,
            distribution_keys: self.distribution_keys.clone(),
            mode,
            stream_keys: self
                .core
                .input
                .expect_stream_key()
                .iter()
                .map(|&index| index as u32)
                .collect(),
        }))
    }
}

impl ExprRewritable<Stream> for StreamChangeLog {}

impl ExprVisitable for StreamChangeLog {}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::{StreamExchange, StreamValues};

    #[test]
    fn test_changelog_separates_input_distribution_and_additional_keys() {
        for (key_indices, distribution_keys, additional_key_columns) in [
            (Some(vec![1, 2]), vec![1], Some(vec![2])),
            (Some(vec![1, 2]), vec![2, 1], Some(vec![])),
            (None, vec![1], None),
        ] {
            let values = StreamValues::new(generic::Values::new_with_stream_key(
                vec![],
                Schema::new(vec![
                    Field::with_name(DataType::Int32, "id"),
                    Field::with_name(DataType::Int32, "tenant"),
                    Field::with_name(DataType::Int32, "business_key"),
                ]),
                OptimizerContext::mock(),
                vec![0],
            ));
            let input = StreamExchange::new(
                values.into(),
                Distribution::HashShard(distribution_keys.clone()),
            );
            let core = generic::ChangeLog::new(
                input.into(),
                key_indices,
                Some(distribution_keys.clone()),
                true,
                true,
            );
            let distribution_keys = distribution_keys
                .into_iter()
                .map(|index| index as u32)
                .collect::<Vec<_>>();
            let changelog = StreamChangeLog::new_with_dist(
                core,
                Distribution::HashShard(vec![4]),
                distribution_keys.clone(),
            );
            let PbNodeBody::Changelog(node) =
                changelog.to_stream_prost_body(&mut BuildFragmentGraphState::default())
            else {
                panic!("expected a changelog node");
            };

            assert_eq!(node.stream_keys, vec![0]);
            assert_eq!(node.distribution_keys, distribution_keys);
            assert_eq!(
                node.mode,
                additional_key_columns.map(|additional_key_columns| Mode::Keyed(Keyed {
                    additional_key_columns,
                }))
            );
        }
    }
}
