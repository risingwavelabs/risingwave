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
use pretty_xmlish::Pretty;
use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::UnionNode;

use super::utils::{formatter_debug_plan_node, watermark_pretty, Distill};
use super::{generic, ExprRewritable, PlanRef};
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNode, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUnion {
    pub base: PlanBase,
    logical: generic::Union<PlanRef>,
}

impl StreamUnion {
    pub fn new(logical: generic::Union<PlanRef>) -> Self {
        let inputs = &logical.inputs;
        let dist = inputs[0].distribution().clone();
        assert!(inputs.iter().all(|input| *input.distribution() == dist));
        let watermark_columns = inputs.iter().fold(
            {
                let mut bitset = FixedBitSet::with_capacity(logical.schema().len());
                bitset.toggle_range(..);
                bitset
            },
            |acc_watermark_columns, input| acc_watermark_columns.bitand(input.watermark_columns()),
        );

        let base = PlanBase::new_stream_with_logical(
            &logical,
            dist,
            inputs.iter().all(|x| x.append_only()),
            inputs.iter().all(|x| x.emit_on_window_close()),
            watermark_columns,
        );
        StreamUnion { base, logical }
    }
}

impl fmt::Display for StreamUnion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = formatter_debug_plan_node!(f, "StreamUnion");
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
impl Distill for StreamUnion {
    fn distill<'a>(&self) -> Pretty<'a> {
        let mut vec = self.logical.fields_pretty();
        if let Some(ow) = watermark_pretty(&self.base.watermark_columns, self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        Pretty::childless_record("StreamUnion", vec)
    }
}

impl PlanTreeNode for StreamUnion {
    fn inputs(&self) -> smallvec::SmallVec<[crate::optimizer::PlanRef; 2]> {
        smallvec::SmallVec::from_vec(self.logical.inputs.clone())
    }

    fn clone_with_inputs(&self, inputs: &[crate::optimizer::PlanRef]) -> PlanRef {
        let mut new = self.logical.clone();
        new.inputs = inputs.to_vec();
        Self::new(new).into()
    }
}

impl StreamNode for StreamUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Union(UnionNode {})
    }
}

impl ExprRewritable for StreamUnion {}
