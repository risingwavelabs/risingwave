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
use std::ops::BitAnd;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::UnionNode;

use super::{ExprRewritable, PlanRef};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{LogicalUnion, PlanBase, PlanTreeNode, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUnion {
    pub base: PlanBase,
    logical: LogicalUnion,
}

impl StreamUnion {
    pub fn new(logical: LogicalUnion) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.logical_pk.to_vec();
        let schema = logical.schema().clone();
        let inputs = logical.inputs();
        let dist = inputs[0].distribution().clone();
        assert!(logical
            .inputs()
            .iter()
            .all(|input| *input.distribution() == dist));
        let watermark_columns = inputs.iter().fold(
            FixedBitSet::with_capacity(schema.len()),
            |acc_watermark_columns, input| acc_watermark_columns.bitand(input.watermark_columns()),
        );

        let base = PlanBase::new_stream(
            ctx,
            schema,
            pk_indices,
            logical.functional_dependency().clone(),
            dist,
            logical.inputs().iter().all(|x| x.append_only()),
            watermark_columns,
        );
        StreamUnion { base, logical }
    }
}

impl fmt::Display for StreamUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamUnion");
        self.logical.fmt_fields_with_builder(&mut builder);

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

        builder.finish()
    }
}

impl PlanTreeNode for StreamUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        let mut vec = smallvec::SmallVec::new();
        vec.extend(self.logical.inputs().into_iter());
        vec
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        Self::new(LogicalUnion::new_with_source_col(
            self.logical.all(),
            inputs.to_owned(),
            self.logical.source_col(),
        ))
        .into()
    }
}

impl StreamNode for StreamUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Union(UnionNode {})
    }
}

impl ExprRewritable for StreamUnion {}
