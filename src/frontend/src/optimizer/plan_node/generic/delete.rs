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
use std::fmt;

use pretty_xmlish::Pretty;
use risingwave_common::catalog::{Schema, TableVersionId};

use super::{DistillUnit, GenericPlanRef};
use crate::catalog::TableId;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Delete<PlanRef> {
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub returning: bool,
}

impl<PlanRef: GenericPlanRef> Delete<PlanRef> {
    pub fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    pub fn schema(&self) -> &Schema {
        self.input.schema()
    }
}

impl<PlanRef> Delete<PlanRef> {
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            returning,
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ table: {}{} }}",
            name,
            self.table_name,
            if self.returning {
                ", returning: true"
            } else {
                ""
            }
        )
    }
}

impl<PlanRef> DistillUnit for Delete<PlanRef> {
    fn distill_with_name<'a>(&self, name: &'a str) -> Pretty<'a> {
        let mut vec = Vec::with_capacity(if self.returning { 2 } else { 1 });
        vec.push(("table", Pretty::Text(self.table_name.clone().into())));
        if self.returning {
            vec.push(("returning", Pretty::display(&true)));
        }
        Pretty::childless_record(name, vec)
    }
}
