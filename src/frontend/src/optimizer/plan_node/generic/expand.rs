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

use itertools::Itertools;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
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
    pub fn output_len(&self) -> usize {
        self.input.schema().len() * 2 + 1
    }

    fn flag_index(&self) -> usize {
        self.output_len() - 1
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Expand<PlanRef> {
    fn schema(&self) -> Schema {
        let mut fields = self
            .input
            .schema()
            .fields()
            .iter()
            .map(|f| Field::with_name(f.data_type(), format!("{}_expanded", f.name)))
            .collect::<Vec<_>>();
        fields.extend(self.input.schema().fields().iter().cloned());
        fields.push(Field::with_name(DataType::Int64, "flag"));
        Schema::new(fields)
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let input_schema_len = self.input.schema().len();
        let mut pk_indices = self
            .input
            .stream_key()?
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

impl<PlanRef: GenericPlanRef> DistillUnit for Expand<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![("column_subsets", self.column_subsets_pretty())])
    }
}

impl<PlanRef: GenericPlanRef> Expand<PlanRef> {
    fn column_subsets_pretty<'a>(&self) -> Pretty<'a> {
        Pretty::Array(
            self.column_subsets
                .iter()
                .map(|subset| {
                    subset
                        .iter()
                        .map(|&i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
                        .map(|d| Pretty::display(&d))
                        .collect()
                })
                .map(Pretty::Array)
                .collect(),
        )
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let map = (0..input_len)
            .map(|source| Some(source + input_len))
            .collect_vec();
        ColIndexMapping::new(map, self.output_len())
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.i2o_col_mapping()
            .inverse()
            .expect("must be invertible")
    }

    pub fn decompose(self) -> (PlanRef, Vec<Vec<usize>>) {
        (self.input, self.column_subsets)
    }
}
