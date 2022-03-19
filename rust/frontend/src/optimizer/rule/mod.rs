// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//! Define all [`Rule`]

use super::PlanRef;

/// A one-to-one transform for the PlanNode, every [`Rule`] should downcast and check if the node
/// matches the rule
#[allow(unused)]
#[allow(clippy::result_unit_err)]
pub trait Rule: Send + Sync + 'static {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join;
pub use project_join::*;
mod filter_join;
pub use filter_join::*;
mod filter_project;
pub use filter_project::*;
