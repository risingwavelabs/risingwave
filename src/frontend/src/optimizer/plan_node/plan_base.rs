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

use educe::Educe;

use super::generic::GenericPlanNode;
use super::*;
use crate::optimizer::property::{Distribution, WatermarkColumns};

/// No extra fields for logical plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NoExtra;

// Make them public types in a private module to allow using them as public trait bounds,
// while still keeping them private to the super module.
mod physical_common {
    use super::*;

    /// Common extra fields for physical plan nodes.
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PhysicalCommonExtra {
        /// The distribution property of the `PlanNode`'s output, store an `Distribution::any()` here
        /// will not affect correctness, but insert unnecessary exchange in plan
        pub dist: Distribution,
    }

    /// A helper trait to reuse code for accessing the common physical fields of batch and stream
    /// plan bases.
    pub trait GetPhysicalCommon {
        fn physical(&self) -> &PhysicalCommonExtra;
        fn physical_mut(&mut self) -> &mut PhysicalCommonExtra;
    }
}

use physical_common::*;

/// Extra fields for stream plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StreamExtra {
    /// Common fields for physical plan nodes.
    physical: PhysicalCommonExtra,

    /// The append-only property of the `PlanNode`'s output is a stream-only property. Append-only
    /// means the stream contains only insert operation.
    append_only: bool,
    /// Whether the output is emitted on window close.
    emit_on_window_close: bool,
    /// The watermark column indices of the `PlanNode`'s output. There could be watermark output from
    /// this stream operator.
    watermark_columns: WatermarkColumns,
    /// The monotonicity of columns in the output.
    columns_monotonicity: MonotonicityMap,
}

impl GetPhysicalCommon for StreamExtra {
    fn physical(&self) -> &PhysicalCommonExtra {
        &self.physical
    }

    fn physical_mut(&mut self) -> &mut PhysicalCommonExtra {
        &mut self.physical
    }
}

/// Extra fields for batch plan nodes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BatchExtra {
    /// Common fields for physical plan nodes.
    physical: PhysicalCommonExtra,

    /// The order property of the `PlanNode`'s output, store an `&Order::any()` here will not affect
    /// correctness, but insert unnecessary sort in plan
    order: Order,
}

impl GetPhysicalCommon for BatchExtra {
    fn physical(&self) -> &PhysicalCommonExtra {
        &self.physical
    }

    fn physical_mut(&mut self) -> &mut PhysicalCommonExtra {
        &mut self.physical
    }
}

/// The common fields of all plan nodes with different conventions.
///
/// Please make a field named `base` in every planNode and correctly value
/// it when construct the planNode.
///
/// All fields are intentionally made private and immutable, as they should
/// normally be the same as the given [`GenericPlanNode`] when constructing.
///
/// - To access them, use traits including [`GenericPlanRef`],
///   [`PhysicalPlanRef`], [`StreamPlanNodeMetadata`] and [`BatchPlanNodeMetadata`] with
///   compile-time checks.
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
    /// the pk indices of the `PlanNode`'s output, a empty stream key vec means there is no stream key
    // TODO: this is actually a logical and stream only property.
    // - For logical nodes, this is `None` in most time expect for the phase after `logical_rewrite_for_stream`.
    // - For stream nodes, this is always `Some`.
    stream_key: Option<Vec<usize>>,
    functional_dependency: FunctionalDependencySet,

    /// Extra fields for different conventions.
    extra: C::Extra,
}

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

impl<C: ConventionMarker> generic::PhysicalPlanRef for PlanBase<C>
where
    C::Extra: GetPhysicalCommon,
{
    fn distribution(&self) -> &Distribution {
        &self.extra.physical().dist
    }
}

impl stream::StreamPlanNodeMetadata for PlanBase<Stream> {
    fn append_only(&self) -> bool {
        self.extra.append_only
    }

    fn emit_on_window_close(&self) -> bool {
        self.extra.emit_on_window_close
    }

    fn watermark_columns(&self) -> &WatermarkColumns {
        &self.extra.watermark_columns
    }

    fn columns_monotonicity(&self) -> &MonotonicityMap {
        &self.extra.columns_monotonicity
    }
}

impl batch::BatchPlanNodeMetadata for PlanBase<Batch> {
    fn order(&self) -> &Order {
        &self.extra.order
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
        watermark_columns: WatermarkColumns,
        columns_monotonicity: MonotonicityMap,
    ) -> Self {
        let id = ctx.next_plan_node_id();
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
                columns_monotonicity,
            },
        }
    }

    pub fn new_stream_with_core(
        core: &impl GenericPlanNode,
        dist: Distribution,
        append_only: bool,
        emit_on_window_close: bool,
        watermark_columns: WatermarkColumns,
        columns_monotonicity: MonotonicityMap,
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
            columns_monotonicity,
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

impl<C: ConventionMarker> PlanBase<C>
where
    C::Extra: GetPhysicalCommon,
{
    /// Clone the plan node with a new distribution.
    ///
    /// Panics if the plan node is not physical.
    pub fn clone_with_new_distribution(&self, dist: Distribution) -> Self {
        let mut new = self.clone();
        new.extra.physical_mut().dist = dist;
        new
    }
}

// Mutators for testing only.
#[cfg(test)]
impl<C: ConventionMarker> PlanBase<C> {
    pub fn functional_dependency_mut(&mut self) -> &mut FunctionalDependencySet {
        &mut self.functional_dependency
    }
}
