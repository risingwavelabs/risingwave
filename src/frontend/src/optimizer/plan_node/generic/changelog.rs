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

use pretty_xmlish::{Str, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::{DistillUnit, GenericPlanNode};
use crate::OptimizerContextRef;
use crate::optimizer::plan_node::stream::prelude::GenericPlanRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;

pub const CHANGELOG_OP: &str = "changelog_op";
pub const _CHANGELOG_ROW_ID: &str = "_changelog_row_id";
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangeLog<PlanRef> {
    pub input: PlanRef,
    // If there is no op in the output result, it is false, example 'create materialized view mv1 as with sub as changelog from t1 select v1 from sub;'
    pub need_op: bool,
    // Before rewrite. If there is no changelog_row_id in the output result, it is false.
    // After rewrite. It is always true.
    pub need_changelog_row_id: bool,
}
impl<PlanRef: GenericPlanRef> DistillUnit for ChangeLog<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![])
    }
}
impl<PlanRef: GenericPlanRef> ChangeLog<PlanRef> {
    pub fn new(input: PlanRef, need_op: bool, need_changelog_row_id: bool) -> Self {
        ChangeLog {
            input,
            need_op,
            need_changelog_row_id,
        }
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let mut map = vec![None; self.input.schema().len()];
        (0..self.input.schema().len()).for_each(|i| map[i] = Some(i));
        ColIndexMapping::new(map, self.schema().len())
    }
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for ChangeLog<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = self.input.schema().fields.clone();
        if self.need_op {
            fields.push(Field::with_name(
                risingwave_common::types::DataType::Int16,
                CHANGELOG_OP,
            ));
        }
        if self.need_changelog_row_id {
            fields.push(Field::with_name(
                risingwave_common::types::DataType::Serial,
                _CHANGELOG_ROW_ID,
            ));
        }
        Schema::new(fields)
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        if self.need_changelog_row_id {
            let keys = vec![self.schema().len() - 1];
            Some(keys)
        } else {
            None
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let i2o = self.i2o_col_mapping();
        i2o.rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }
}
