// Copyright 2022 RisingWave Labs
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
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use risingwave_common::catalog::Schema;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::{LogicalPlanRef, PlanNodeId, StreamPlanRef};
use crate::optimizer::property::FunctionalDependencySet;
use crate::optimizer::{OptimizerContextRef, ShareEntryRef, ShareId, ShareVersion};

/// Operations implemented by the two conventions that support a share node.
#[doc(hidden)]
pub trait SharePlanRef: GenericPlanRef + Clone + Sized {
    fn register_share(ctx: &OptimizerContextRef, input: Self) -> (ShareId, ShareEntryRef<Self>);
    fn share_entry(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareEntryRef<Self>;
    fn current_share_version(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareVersion;
    fn resolve_share(ctx: &OptimizerContextRef, share_id: ShareId, version: ShareVersion) -> Self;
    fn share_plan_node_id(ctx: &OptimizerContextRef, share_id: ShareId) -> PlanNodeId;
    fn update_share(
        ctx: &OptimizerContextRef,
        share_id: ShareId,
        base_version: ShareVersion,
        input: Self,
    ) -> ShareVersion;
}

impl SharePlanRef for LogicalPlanRef {
    fn register_share(ctx: &OptimizerContextRef, input: Self) -> (ShareId, ShareEntryRef<Self>) {
        ctx.register_logical_share(input)
    }

    fn share_entry(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareEntryRef<Self> {
        ctx.logical_share_entry(share_id)
    }

    fn current_share_version(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareVersion {
        ctx.current_logical_share_version(share_id)
    }

    fn resolve_share(ctx: &OptimizerContextRef, share_id: ShareId, version: ShareVersion) -> Self {
        ctx.resolve_logical_share(share_id, version)
    }

    fn share_plan_node_id(ctx: &OptimizerContextRef, share_id: ShareId) -> PlanNodeId {
        ctx.logical_share_plan_node_id(share_id)
    }

    fn update_share(
        ctx: &OptimizerContextRef,
        share_id: ShareId,
        base_version: ShareVersion,
        input: Self,
    ) -> ShareVersion {
        ctx.update_logical_share(share_id, base_version, input)
    }
}

impl SharePlanRef for StreamPlanRef {
    fn register_share(ctx: &OptimizerContextRef, input: Self) -> (ShareId, ShareEntryRef<Self>) {
        ctx.register_stream_share(input)
    }

    fn share_entry(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareEntryRef<Self> {
        ctx.stream_share_entry(share_id)
    }

    fn current_share_version(ctx: &OptimizerContextRef, share_id: ShareId) -> ShareVersion {
        ctx.current_stream_share_version(share_id)
    }

    fn resolve_share(ctx: &OptimizerContextRef, share_id: ShareId, version: ShareVersion) -> Self {
        ctx.resolve_stream_share(share_id, version)
    }

    fn share_plan_node_id(ctx: &OptimizerContextRef, share_id: ShareId) -> PlanNodeId {
        ctx.stream_share_plan_node_id(share_id)
    }

    fn update_share(
        ctx: &OptimizerContextRef,
        share_id: ShareId,
        base_version: ShareVersion,
        input: Self,
    ) -> ShareVersion {
        ctx.update_stream_share(share_id, base_version, input)
    }
}

/// Immutable handle to a shared subplan stored in [`OptimizerContextRef`].
pub struct Share<PlanRef> {
    share_id: ShareId,
    version: ShareVersion,
    ctx: OptimizerContextRef,
    /// Keeps the weak context registry entry alive without making the context own a plan that
    /// points back to itself.
    _entry: ShareEntryRef<PlanRef>,
}

impl<PlanRef> Clone for Share<PlanRef> {
    fn clone(&self) -> Self {
        Self {
            share_id: self.share_id,
            version: self.version,
            ctx: self.ctx.clone(),
            _entry: self._entry.clone(),
        }
    }
}

impl<PlanRef> fmt::Debug for Share<PlanRef> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Share")
            .field("share_id", &self.share_id)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl<PlanRef> PartialEq for Share<PlanRef> {
    fn eq(&self, other: &Self) -> bool {
        self.share_id == other.share_id
    }
}

impl<PlanRef> Eq for Share<PlanRef> {}

impl<PlanRef> Hash for Share<PlanRef> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.share_id.hash(state);
    }
}

impl<PlanRef: SharePlanRef> Share<PlanRef> {
    pub fn new(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let (share_id, entry) = PlanRef::register_share(&ctx, input);
        let version = PlanRef::current_share_version(&ctx, share_id);
        Self {
            share_id,
            version,
            ctx,
            _entry: entry,
        }
    }

    pub fn from_share_id(ctx: OptimizerContextRef, share_id: ShareId) -> Self {
        let entry = PlanRef::share_entry(&ctx, share_id);
        let version = PlanRef::current_share_version(&ctx, share_id);
        // Resolve once to fail at the construction boundary instead of a later traversal.
        let _ = PlanRef::resolve_share(&ctx, share_id, version);
        Self {
            share_id,
            version,
            ctx,
            _entry: entry,
        }
    }

    pub fn share_id(&self) -> ShareId {
        self.share_id
    }

    pub fn version(&self) -> ShareVersion {
        self.version
    }

    pub fn plan_node_id(&self) -> PlanNodeId {
        PlanRef::share_plan_node_id(&self.ctx, self.share_id)
    }

    pub fn input(&self) -> PlanRef {
        PlanRef::resolve_share(&self.ctx, self.share_id, self.version)
    }

    /// Update the context entry while keeping this share's stable identity.
    pub fn update_input(&self, input: PlanRef) -> Self {
        debug_assert!(Rc::ptr_eq(&self.ctx, &input.ctx()));
        let version = PlanRef::update_share(&self.ctx, self.share_id, self.version, input);
        Self {
            share_id: self.share_id,
            version,
            ctx: self.ctx.clone(),
            _entry: self._entry.clone(),
        }
    }

    /// Fork this share into a new identity for whole-plan cloning and rewriting.
    pub fn fork_with_input(&self, input: PlanRef) -> Self {
        debug_assert!(Rc::ptr_eq(&self.ctx, &input.ctx()));
        Self::new(input)
    }
}

impl<PlanRef: SharePlanRef> GenericPlanNode for Share<PlanRef> {
    fn schema(&self) -> Schema {
        self.input().schema().clone()
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.input().stream_key()?.to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input().functional_dependency().clone()
    }
}
