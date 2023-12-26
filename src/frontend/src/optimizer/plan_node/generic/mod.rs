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

use std::borrow::Cow;
use std::hash::Hash;

use pretty_xmlish::XmlNode;
use risingwave_common::catalog::Schema;

use super::{stream, EqJoinPredicate, PlanNodeId};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Distribution, FunctionalDependencySet};

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
mod sys_scan;
pub use sys_scan::*;

mod cdc_scan;
pub use cdc_scan::*;

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
mod limit;
pub use limit::*;
mod max_one_row;
pub use max_one_row::*;

pub trait DistillUnit {
    fn distill_with_name<'a>(&self, name: impl Into<Cow<'a, str>>) -> XmlNode<'a>;
}

macro_rules! impl_distill_unit_from_fields {
    ($name:ident, $bound:path) => {
        use std::borrow::Cow;

        use pretty_xmlish::XmlNode;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        impl<PlanRef: $bound> DistillUnit for $name<PlanRef> {
            fn distill_with_name<'a>(&self, name: impl Into<Cow<'a, str>>) -> XmlNode<'a> {
                XmlNode::simple_record(name, self.fields_pretty(), vec![])
            }
        }
    };
}
pub(super) use impl_distill_unit_from_fields;

#[auto_impl::auto_impl(&)]
pub trait GenericPlanRef: Eq + Hash {
    fn id(&self) -> PlanNodeId;
    fn schema(&self) -> &Schema;
    fn stream_key(&self) -> Option<&[usize]>;
    fn functional_dependency(&self) -> &FunctionalDependencySet;
    fn ctx(&self) -> OptimizerContextRef;
}

#[auto_impl::auto_impl(&)]
pub trait PhysicalPlanRef: GenericPlanRef {
    fn distribution(&self) -> &Distribution;
}

pub trait GenericPlanNode {
    fn functional_dependency(&self) -> FunctionalDependencySet;
    fn schema(&self) -> Schema;
    fn stream_key(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}
