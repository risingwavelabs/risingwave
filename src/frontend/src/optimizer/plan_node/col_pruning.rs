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

use paste::paste;

use super::*;
pub use crate::expr::CollectInputRef;
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable {
    /// Transform the plan node to only output the required columns ordered by index number.
    ///
    /// `required_cols` must be a subset of the range `0..self.schema().len()`.
    ///
    /// After calling `prune_col` on the children, their output schema may change, so
    /// the caller may need to transform its [`InputRef`](crate::expr::InputRef) using
    /// [`ColIndexMapping`](crate::utils::ColIndexMapping).
    ///
    /// When implementing this method for a node, it may require its children to produce additional
    /// columns besides `required_cols`. In this case, it may need to insert a
    /// [`LogicalProject`](super::LogicalProject) above to have a correct schema.
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef;
}

/// Implements [`ColPrunable`] for batch and streaming node.
macro_rules! impl_prune_col {
    ($( { $convention:ident, $name:ident }),*) => {
        paste!{
            $(impl ColPrunable for [<$convention $name>] {
                fn prune_col(&self, _required_cols: &[usize]) -> PlanRef {
                    panic!("column pruning is only allowed on logical plan")
                }
            })*
        }
    }
}
for_batch_plan_nodes! { impl_prune_col }
for_stream_plan_nodes! { impl_prune_col }
