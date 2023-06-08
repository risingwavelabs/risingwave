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

use itertools::Itertools;
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::{Schema, TableVersionId};

use super::GenericPlanRef;
use crate::catalog::TableId;
use crate::expr::ExprImpl;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Insert<PlanRef> {
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub input: PlanRef,
    pub column_indices: Vec<usize>, // columns in which to insert
    pub default_columns: Vec<(usize, ExprImpl)>, // columns to be set to default
    pub row_id_index: Option<usize>,
    pub returning: bool,
}

impl<PlanRef: GenericPlanRef> Insert<PlanRef> {
    pub fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    pub fn schema(&self) -> &Schema {
        self.input.schema()
    }

    pub fn fields_pretty<'a>(&self, verbose: bool) -> StrAssocArr<'a> {
        let mut capacity = 1;
        if self.returning {
            capacity += 1;
        }
        if verbose {
            capacity += 1;
            if !self.default_columns.is_empty() {
                capacity += 1;
            }
        }
        let mut vec = Vec::with_capacity(capacity);
        vec.push(("table", Pretty::from(self.table_name.clone())));
        if self.returning {
            vec.push(("returning", Pretty::debug(&true)));
        }
        if verbose {
            let collect = (self.column_indices.iter().enumerate())
                .map(|(k, v)| Pretty::from(format!("{}:{}", k, v)))
                .collect();
            vec.push(("mapping", Pretty::Array(collect)));
            if !self.default_columns.is_empty() {
                let collect = self
                    .default_columns
                    .iter()
                    .map(|(k, v)| Pretty::from(format!("{}<-{:?}", k, v)))
                    .collect();
                vec.push(("default", Pretty::Array(collect)));
            }
        }
        vec
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let verbose = self.ctx().is_explain_verbose();

        let ret = if self.returning {
            ", returning: true"
        } else {
            ""
        };
        write!(f, "{} {{ table: {}{}", name, self.table_name, ret)?;
        if verbose {
            let collect = self
                .column_indices
                .iter()
                .enumerate()
                .map(|(k, v)| format!("{}:{}", k, v))
                .join(", ");
            write!(f, ", mapping: [{}]", collect)?;
            if !self.default_columns.is_empty() {
                let collect = self
                    .default_columns
                    .iter()
                    .map(|(k, v)| format!("{}<-{:?}", k, v))
                    .join(", ");
                write!(f, ", default: [{}]", collect)?;
            }
        }
        write!(f, " }}")
    }
}

impl<PlanRef> Insert<PlanRef> {
    /// Create a [`LogicalInsert`] node. Used internally by optimizer.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_indices: Vec<usize>,
        default_columns: Vec<(usize, ExprImpl)>,
        row_id_index: Option<usize>,
        returning: bool,
    ) -> Self {
        Self {
            table_name,
            table_id,
            table_version_id,
            input,
            column_indices,
            default_columns,
            row_id_index,
            returning,
        }
    }
}
