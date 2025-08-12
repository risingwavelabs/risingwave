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

use std::ops::BitAnd;

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::UnionNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{Distill, childless_record, watermark_pretty};
use super::{ExprRewritable, StreamPlanRef as PlanRef, generic};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanNode;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNode, StreamNode};
use crate::optimizer::property::{Distribution, MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamUnion` implements [`super::LogicalUnion`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUnion {
    pub base: PlanBase<Stream>,
    core: generic::Union<PlanRef>,
}

impl StreamUnion {
    pub fn new(core: generic::Union<PlanRef>) -> Self {
        let inputs = &core.inputs;
        let dist = inputs[0].distribution().clone();
        assert!(inputs.iter().all(|input| *input.distribution() == dist));
        Self::new_with_dist(core, dist)
    }

    pub fn new_with_dist(core: generic::Union<PlanRef>, dist: Distribution) -> Self {
        assert!(
            core.all,
            "After UnionToDistinctRule, union should become union all"
        );
        assert!(core.source_col.is_some());

        let inputs = &core.inputs;
        let ctx = core.ctx();

        let watermark_indices = inputs
            .iter()
            .map(|x| x.watermark_columns().index_set().to_bitset())
            .fold(
                {
                    let mut bitset = FixedBitSet::with_capacity(core.schema().len());
                    bitset.toggle_range(..);
                    bitset
                },
                |acc, x| acc.bitand(&x),
            );
        let mut watermark_columns = WatermarkColumns::new();
        for idx in watermark_indices.ones() {
            // XXX(rc): for the sake of simplicity, we assign each watermark column a new group
            watermark_columns.insert(idx, ctx.next_watermark_group_id());
        }

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            // There's no handling on key conflict in executor implementation. However, we guarantee
            // that there's a `source_col` as a part of stream key, indicating which input the row
            // comes from. As a result, there's in fact no key conflict and we can safely call `merge`.
            (inputs.iter().map(|i| i.stream_kind()))
                .reduce(StreamKind::merge)
                .unwrap_or(/* empty inputs */ StreamKind::AppendOnly),
            inputs.iter().all(|x| x.emit_on_window_close()),
            watermark_columns,
            MonotonicityMap::new(),
        );

        StreamUnion { base, core }
    }
}

impl Distill for StreamUnion {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut vec = self.core.fields_pretty();
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        childless_record("StreamUnion", vec)
    }
}

impl PlanTreeNode<Stream> for StreamUnion {
    fn inputs(&self) -> smallvec::SmallVec<[PlanRef; 2]> {
        smallvec::SmallVec::from_vec(self.core.inputs.clone())
    }

    fn clone_with_inputs(&self, inputs: &[PlanRef]) -> PlanRef {
        let mut new = self.core.clone();
        new.inputs = inputs.to_vec();
        let dist = self.distribution().clone();
        Self::new_with_dist(new, dist).into()
    }
}

impl StreamNode for StreamUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Union(UnionNode {})
    }
}

impl ExprRewritable<Stream> for StreamUnion {}

impl ExprVisitable for StreamUnion {}
