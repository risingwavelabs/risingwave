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
pub struct NoExtra;

/// Common extra fields for physical plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PhysicalCommonExtra {
    /// The distribution property of the PlanNode's output, store an `Distribution::any()` here
    /// will not affect correctness, but insert unnecessary exchange in plan
    dist: Distribution,
}

/// Extra fields for stream plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamExtra {
    /// Common fields for physical plan nodes.
    physical: PhysicalCommonExtra,

    /// The append-only property of the PlanNode's output is a stream-only property. Append-only
    /// means the stream contains only insert operation.
    append_only: bool,
    /// Whether the output is emitted on window close.
    emit_on_window_close: bool,
    /// The watermark column indices of the PlanNode's output. There could be watermark output from
    /// this stream operator.
    watermark_columns: FixedBitSet,
}

impl generic::PhysicalSpecific for StreamExtra {
    fn distribution(&self) -> &Distribution {
        &self.physical.dist
    }
}

impl stream::StreamSpecific for StreamExtra {
    fn append_only(&self) -> bool {
        self.append_only
    }

    fn emit_on_window_close(&self) -> bool {
        self.emit_on_window_close
    }

    fn watermark_columns(&self) -> &FixedBitSet {
        &self.watermark_columns
    }
}

/// Extra fields for batch plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BatchExtra {
    /// Common fields for physical plan nodes.
    physical: PhysicalCommonExtra,

    /// The order property of the PlanNode's output, store an `&Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    order: Order,
}

impl generic::PhysicalSpecific for BatchExtra {
    fn distribution(&self) -> &Distribution {
        &self.physical.dist
    }
}

impl batch::BatchSpecific for BatchExtra {
    fn order(&self) -> &Order {
        &self.order
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
#[derive(Educe)]
#[educe(PartialEq, Eq, Hash, Clone, Debug)]
pub struct PlanBase<C: ConventionMarker> {
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

    extra: C::Extra,
}

// impl PlanBase {
//     fn physical_extra(&self) -> &PhysicalExtra {
//         self.physical_extra
//             .as_ref()
//             .expect("access physical properties from logical plan node")
//     }

//     fn physical_extra_mut(&mut self) -> &mut PhysicalExtra {
//         self.physical_extra
//             .as_mut()
//             .expect("access physical properties from logical plan node")
//     }
// }

impl<C: ConventionMarker> generic::GenericPlanRef for PlanBase<C> {
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

impl<C: ConventionMarker> generic::PhysicalSpecific for PlanBase<C>
where
    C::Extra: generic::PhysicalSpecific,
{
    fn distribution(&self) -> &Distribution {
        &self.extra.distribution()
    }
}

impl<C: ConventionMarker> stream::StreamSpecific for PlanBase<C>
where
    C::Extra: stream::StreamSpecific,
{
    fn append_only(&self) -> bool {
        self.extra.append_only()
    }

    fn emit_on_window_close(&self) -> bool {
        self.extra.emit_on_window_close()
    }

    fn watermark_columns(&self) -> &FixedBitSet {
        &self.extra.watermark_columns()
    }
}

impl<C: ConventionMarker> batch::BatchSpecific for PlanBase<C>
where
    C::Extra: batch::BatchSpecific,
{
    fn order(&self) -> &Order {
        &self.extra.order()
    }
}

impl<C: ConventionMarker> PlanBase<C> {
    pub fn clone_with_new_plan_id(&self) -> Self {
        let mut new = self.clone();
        new.id = self.ctx().next_plan_node_id();
        new
    }
}

impl PlanBase<Logical> {
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
            extra: NoExtra,
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
}

impl PlanBase<Stream> {
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
            extra: StreamExtra {
                physical: PhysicalCommonExtra { dist },
                append_only,
                emit_on_window_close,
                watermark_columns,
            },
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
}

impl PlanBase<Batch> {
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
            extra: BatchExtra {
                physical: PhysicalCommonExtra { dist },
                order,
            },
        }
    }

    pub fn new_batch_with_core(
        core: &impl GenericPlanNode,
        dist: Distribution,
        order: Order,
    ) -> Self {
        Self::new_batch(core.ctx(), core.schema(), dist, order)
    }
}

impl PlanBase<Batch> {
    /// Clone the plan node with a new distribution.
    ///
    /// Panics if the plan node is not physical.
    pub fn clone_with_new_distribution(&self, dist: Distribution) -> Self {
        let mut new = self.clone();
        new.extra.physical.dist = dist;
        new
    }
}

// TODO: unify the impls for `PlanBase<Stream>` and `PlanBase<Batch>`.
impl PlanBase<Stream> {
    /// Clone the plan node with a new distribution.
    ///
    /// Panics if the plan node is not physical.
    pub fn clone_with_new_distribution(&self, dist: Distribution) -> Self {
        let mut new = self.clone();
        new.extra.physical.dist = dist;
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
