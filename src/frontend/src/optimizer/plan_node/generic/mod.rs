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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, FieldDisplay, Schema, TableDesc};
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as AggCallStateProst};

use super::utils::{IndicesDisplay, TableCatalogBuilder};
use super::{stream, EqJoinPredicate};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::{Expr, ExprDisplay, ExprImpl, InputRef, InputRefDisplay};
use crate::optimizer::property::{Direction, Order};
use crate::session::OptimizerContextRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};
use crate::TableCatalog;

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
pub trait GenericPlanRef {
    fn schema(&self) -> &Schema;
    fn logical_pk(&self) -> &[usize];
    fn ctx(&self) -> OptimizerContextRef;
}

pub trait GenericPlanNode {
    fn schema(&self) -> Schema;
    fn logical_pk(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}
