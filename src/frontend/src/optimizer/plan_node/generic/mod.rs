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

use std::hash::Hash;

use risingwave_common::catalog::Schema;

use super::{stream, EqJoinPredicate};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

pub mod dynamic_filter;
pub use dynamic_filter::*;
mod hop_window;
pub use hop_window::*;
mod agg;
pub use agg::*;
mod project_set;
pub use project_set::*;
mod join;
pub use join::*;
mod project;
pub use project::*;
mod filter;
pub use filter::*;
mod expand;
pub use expand::*;
mod source;
pub use source::*;
mod scan;
pub use scan::*;
mod union;
pub use union::*;
mod top_n;
pub use top_n::*;
mod share;
pub use share::*;
mod dedup;
pub use dedup::*;
mod intersect;
pub use intersect::*;
mod over_window;
pub use over_window::*;
mod except;
pub use except::*;
mod update;
pub use update::*;
mod delete;
pub use delete::*;
mod insert;
pub use insert::*;

pub trait GenericPlanRef : Eq + Hash {
    fn schema(&self) -> &Schema;
    fn logical_pk(&self) -> &[usize];
    fn functional_dependency(&self) -> &FunctionalDependencySet;
    fn ctx(&self) -> OptimizerContextRef;
}

pub trait GenericPlanNode {
    /// return (schema, `logical_pk`, fds)
    fn logical_properties(&self) -> (Schema, Option<Vec<usize>>, FunctionalDependencySet) {
        (
            self.schema(),
            self.logical_pk(),
            self.functional_dependency(),
        )
    }
    fn functional_dependency(&self) -> FunctionalDependencySet;
    fn schema(&self) -> Schema;
    fn logical_pk(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}
