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

use educe::Educe;
use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;

use super::generic::GenericPlanNode;
use super::*;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Distribution, FunctionalDependencySet, Order};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StreamExtra {
    /// The append-only property of the PlanNode's output is a stream-only property. Append-only
    /// means the stream contains only insert operation.
    append_only: bool,
    /// Whether the output is emitted on window close.
    emit_on_window_close: bool,
    /// The watermark column indices of the PlanNode's output. There could be watermark output from
    /// this stream operator.
    watermark_columns: FixedBitSet,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BatchExtra {
    /// The order property of the PlanNode's output, store an `&Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    order: Order,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PhysicalExtraInner {
    Stream(StreamExtra),
    Batch(BatchExtra),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PhysicalExtra {
    /// The distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    dist: Distribution,

    inner: PhysicalExtraInner,
}

impl PhysicalExtra {
    fn stream(&self) -> &StreamExtra {
        match &self.inner {
            PhysicalExtraInner::Stream(extra) => extra,
            _ => panic!("access stream properties from batch plan node"),
        }
    }

    fn batch(&self) -> &BatchExtra {
        match &self.inner {
            PhysicalExtraInner::Batch(extra) => extra,
            _ => panic!("access batch properties from stream plan node"),
        }
    }
}

/// the common fields of all nodes, please make a field named `base` in
/// every planNode and correctly value it when construct the planNode.
///
/// All fields are intentionally made private and immutable, as they should
/// normally be the same as the given [`GenericPlanNode`] when constructing.
///
/// - To access them, use traits including [`GenericPlanRef`],
///   [`PhysicalPlanRef`], [`StreamPlanRef`] and [`BatchPlanRef`].
/// - To mutate them, use methods like `new_*` or `clone_with_*`.
#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct PlanBase {
    // -- common fields --
    #[educe(PartialEq(ignore), Hash(ignore))]
    id: PlanNodeId,
    #[educe(PartialEq(ignore), Hash(ignore))]
    ctx: OptimizerContextRef,

    schema: Schema,
    /// the pk indices of the PlanNode's output, a empty stream key vec means there is no stream key
    // TODO: this is actually a logical and stream only property
    stream_key: Option<Vec<usize>>,
    functional_dependency: FunctionalDependencySet,

    physical_extra: Option<PhysicalExtra>,
}

impl PlanBase {
    fn physical_extra(&self) -> &PhysicalExtra {
        self.physical_extra
            .as_ref()
            .expect("access physical properties from logical plan node")
    }

    fn physical_extra_mut(&mut self) -> &mut PhysicalExtra {
        self.physical_extra
            .as_mut()
            .expect("access physical properties from logical plan node")
    }
}

impl generic::GenericPlanRef for PlanBase {
    fn id(&self) -> PlanNodeId {
        self.id
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn stream_key(&self) -> Option<&[usize]> {
        self.stream_key.as_deref()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> &FunctionalDependencySet {
        &self.functional_dependency
    }
}

impl generic::PhysicalPlanRef for PlanBase {
    fn distribution(&self) -> &Distribution {
        &self.physical_extra().dist
    }
}

impl stream::StreamPlanRef for PlanBase {
    fn append_only(&self) -> bool {
        self.physical_extra().stream().append_only
    }

    fn emit_on_window_close(&self) -> bool {
        self.physical_extra().stream().emit_on_window_close
    }

    fn watermark_columns(&self) -> &FixedBitSet {
        &self.physical_extra().stream().watermark_columns
    }
}

impl batch::BatchPlanRef for PlanBase {
    fn order(&self) -> &Order {
        &self.physical_extra().batch().order
    }
}

impl PlanBase {
    pub fn new_logical(
        ctx: OptimizerContextRef,
        schema: Schema,
        stream_key: Option<Vec<usize>>,
        functional_dependency: FunctionalDependencySet,
    ) -> Self {
        let id = ctx.next_plan_node_id();
        Self {
            id,
            ctx,
            schema,
            stream_key,
            functional_dependency,
            physical_extra: None,
        }
    }

    pub fn new_logical_with_core(core: &impl GenericPlanNode) -> Self {
        Self::new_logical(
            core.ctx(),
            core.schema(),
            core.stream_key(),
            core.functional_dependency(),
        )
    }

    pub fn new_stream(
        ctx: OptimizerContextRef,
        schema: Schema,
        stream_key: Option<Vec<usize>>,
        functional_dependency: FunctionalDependencySet,
        dist: Distribution,
        append_only: bool,
        emit_on_window_close: bool,
        watermark_columns: FixedBitSet,
    ) -> Self {
        let id = ctx.next_plan_node_id();
        assert_eq!(watermark_columns.len(), schema.len());
        Self {
            id,
            ctx,
            schema,
            stream_key,
            functional_dependency,
            physical_extra: Some(PhysicalExtra {
                dist,
                inner: PhysicalExtraInner::Stream(StreamExtra {
                    append_only,
                    emit_on_window_close,
                    watermark_columns,
                }),
            }),
        }
    }

    pub fn new_stream_with_core(
        core: &impl GenericPlanNode,
        dist: Distribution,
        append_only: bool,
        emit_on_window_close: bool,
        watermark_columns: FixedBitSet,
    ) -> Self {
        Self::new_stream(
            core.ctx(),
            core.schema(),
            core.stream_key(),
            core.functional_dependency(),
            dist,
            append_only,
            emit_on_window_close,
            watermark_columns,
        )
    }

    pub fn new_batch(
        ctx: OptimizerContextRef,
        schema: Schema,
        dist: Distribution,
        order: Order,
    ) -> Self {
        let id = ctx.next_plan_node_id();
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        Self {
            id,
            ctx,
            schema,
            stream_key: None,
            functional_dependency,
            physical_extra: Some(PhysicalExtra {
                dist,
                inner: PhysicalExtraInner::Batch(BatchExtra { order }),
            }),
        }
    }

    pub fn new_batch_with_core(
        core: &impl GenericPlanNode,
        dist: Distribution,
        order: Order,
    ) -> Self {
        Self::new_batch(core.ctx(), core.schema(), dist, order)
    }

    pub fn clone_with_new_plan_id(&self) -> Self {
        let mut new = self.clone();
        new.id = self.ctx().next_plan_node_id();
        new
    }

    pub fn clone_with_new_distribution(&self, dist: Distribution) -> Self {
        let mut new = self.clone();
        new.physical_extra_mut().dist = dist;
        new
    }
}

// Mutators for testing only.
#[cfg(test)]
impl PlanBase {
    pub fn functional_dependency_mut(&mut self) -> &mut FunctionalDependencySet {
        &mut self.functional_dependency
    }
}
