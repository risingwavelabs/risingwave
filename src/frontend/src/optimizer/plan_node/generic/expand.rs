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
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::{GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// [`Expand`] expand one row multiple times according to `column_subsets` and also keep
/// original columns of input. It can be used to implement distinct aggregation and group set.
///
/// This is the schema of `Expand`:
/// | expanded columns(i.e. some columns are set to null) | original columns of input | flag |.
///
/// Aggregates use expanded columns as their arguments and original columns for their filter. `flag`
/// is used to distinguish between different `subset`s in `column_subsets`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Expand<PlanRef> {
    // `column_subsets` has many `subset`s which specifies the columns that need to be
    // reserved and other columns will be filled with NULL.
    pub column_subsets: Vec<Vec<usize>>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> Expand<PlanRef> {
    fn output_len(&self) -> usize {
        self.input.schema().len() * 2 + 1
    }

    fn flag_index(&self) -> usize {
        self.output_len() - 1
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Expand<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = self.input.schema().clone().into_fields();
        fields.extend(fields.clone());
        fields.push(Field::with_name(DataType::Int64, "flag"));
        Schema::new(fields)
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let input_schema_len = self.input.schema().len();
        let mut pk_indices = self
            .input
            .logical_pk()
            .iter()
            .map(|&pk| pk + input_schema_len)
            .collect_vec();
        // The last column should be the flag.
        pk_indices.push(input_schema_len * 2);
        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let input_fd = self
            .input
            .functional_dependency()
            .clone()
            .into_dependencies();
        let output_len = self.output_len();
        let flag_index = self.flag_index();

        self.input
            .functional_dependency()
            .as_dependencies()
            .iter()
            .map(|_input_fd| {})
            .collect_vec();

        let mut current_fd = FunctionalDependencySet::new(output_len);
        for mut fd in input_fd {
            fd.grow(output_len);
            fd.set_from(flag_index, true);
            current_fd.add_functional_dependency(fd);
        }
        current_fd
    }
}

impl<PlanRef: GenericPlanRef> Expand<PlanRef> {
    pub fn column_subsets_display(&self) -> Vec<Vec<FieldDisplay<'_>>> {
        self.column_subsets
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .map(|&i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
                    .collect()
            })
            .collect()
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        write!(
            f,
            "{} {{ column_subsets: {:?} }}",
            name,
            self.column_subsets_display()
        )
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let map = (0..input_len)
            .map(|source| Some(source + input_len))
            .collect_vec();
        ColIndexMapping::with_target_size(map, self.output_len())
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.i2o_col_mapping()
            .inverse()
            .expect("must be invertible")
    }
}
