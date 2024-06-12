// Copyright 2024 RisingWave Labs
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
use crate::optimizer::plan_node::stream::prelude::GenericPlanRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::ColIndexMappingRewriteExt;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChangedLog<PlanRef> {
    pub input: PlanRef,
    pub need_op: bool,
    pub need_changed_log_row_id: bool,
}
impl<PlanRef: GenericPlanRef> DistillUnit for ChangedLog<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![])
    }
}
impl<PlanRef: GenericPlanRef> ChangedLog<PlanRef> {
    pub fn new(input: PlanRef, need_op: bool, need_changed_log_row_id: bool) -> Self {
        ChangedLog {
            input,
            need_op,
            need_changed_log_row_id,
        }
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let mut map = vec![None; self.input.schema().len()];
        (0..self.input.schema().len()).for_each(|i| map[i] = Some(i));
        ColIndexMapping::new(map, self.schema().len())
    }
}
impl<PlanRef: GenericPlanRef> GenericPlanNode for ChangedLog<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = self.input.schema().fields.clone();
        if self.need_op {
            fields.push(Field::with_name(
                risingwave_common::types::DataType::Int16,
                "op",
            ));
        }
        if self.need_changed_log_row_id {
            fields.push(Field::with_name(
                risingwave_common::types::DataType::Serial,
                "_changed_log_row_id",
            ));
        }
        Schema::new(fields)
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        match self.input.stream_key() {
            Some(keys) => {
                let mut keys = keys.to_vec();
                if self.need_changed_log_row_id {
                    keys.push(self.schema().len() - 1);
                }
                Some(keys)
            }
            None => {
                if self.need_changed_log_row_id {
                    let keys = vec![self.schema().len() - 1];
                    Some(keys)
                } else {
                    None
                }
            }
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
