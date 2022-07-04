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

use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};

use super::{
    ColPrunable, LogicalFilter, LogicalProject, PlanBase, PlanRef, PredicatePushdown, StreamSink,
    ToBatch, ToStream,
};
use crate::session::OptimizerContextRef;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalSink` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalSink {
    pub base: PlanBase,
    pub sink_name: String,
}

impl LogicalSink {
    pub fn new(sink_name: Rc<String>, ctx: OptimizerContextRef) -> Self {
        todo!();
    }

    pub fn sink_name(&self) -> String {
        self.sink_name.clone()
    }
}

impl_plan_tree_node_for_leaf! {LogicalSink}

impl fmt::Display for LogicalSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        todo!();
    }
}

impl ColPrunable for LogicalSink {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        todo!();
    }
}
impl PredicatePushdown for LogicalSink {
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        todo!();
    }
}

impl ToBatch for LogicalSink {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(RwError::from(ErrorCode::NotImplemented(
            "there is no batch sink operator".to_string(),
            None.into(),
        )))
    }
}

impl ToStream for LogicalSink {
    fn to_stream(&self) -> Result<PlanRef> {
        Ok(StreamSink::new(self.clone()).into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        todo!();
    }
}
