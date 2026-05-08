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

use educe::Educe;
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::{ColumnCatalog, Field, Schema, TableVersionId};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;

use super::{GenericPlanNode, GenericPlanRef};
use crate::OptimizerContextRef;
use crate::catalog::TableId;
use crate::expr::{ExprImpl, ExprRewriter};
use crate::optimizer::property::FunctionalDependencySet;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Insert<PlanRef: Eq + Hash> {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub table_name: String, // explain-only
    pub table_id: TableId,
    pub table_version_id: TableVersionId,
    pub table_visible_columns: Vec<ColumnCatalog>,
    pub input: PlanRef,
    pub column_indices: Vec<usize>, // columns in which to insert
    pub default_columns: Vec<(usize, ExprImpl)>, // columns to be set to default
    pub generated_columns: Vec<(usize, ExprImpl)>, // generated columns for RETURNING only
    pub row_id_index: Option<usize>,
    pub returning: bool,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Insert<PlanRef> {
    fn functional_dependency(&self) -> FunctionalDependencySet {
        FunctionalDependencySet::new(self.output_len())
    }

    fn schema(&self) -> Schema {
        if self.returning {
            // We cannot directly use `self.input.schema()` here since it may omit some columns that
            // will be filled with default values.
            Schema::new(
                self.table_visible_columns
                    .iter()
                    .map(|c| Field::from(&c.column_desc))
                    .collect(),
            )
        } else {
            Schema::new(vec![Field::unnamed(DataType::Int64)])
        }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        None
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> Insert<PlanRef> {
    pub fn clone_with_input<OtherPlanRef: Eq + Hash>(
        &self,
        input: OtherPlanRef,
    ) -> Insert<OtherPlanRef> {
        Insert {
            table_name: self.table_name.clone(),
            table_id: self.table_id,
            table_version_id: self.table_version_id,
            table_visible_columns: self.table_visible_columns.clone(),
            input,
            column_indices: self.column_indices.clone(),
            default_columns: self.default_columns.clone(),
            generated_columns: self.generated_columns.clone(),
            row_id_index: self.row_id_index,
            returning: self.returning,
        }
    }

    pub fn output_len(&self) -> usize {
        if self.returning {
            self.table_visible_columns.len()
        } else {
            1
        }
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
            if !self.generated_columns.is_empty() {
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
            if !self.generated_columns.is_empty() {
                let collect = self
                    .generated_columns
                    .iter()
                    .map(|(k, v)| Pretty::from(format!("{}<-{:?}", k, v)))
                    .collect();
                vec.push(("generated", Pretty::Array(collect)));
            }
        }
        vec
    }
}

impl<PlanRef: Eq + Hash> Insert<PlanRef> {
    /// Create a [`Insert`] node. Used internally by optimizer.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: PlanRef,
        table_name: String,
        table_id: TableId,
        table_version_id: TableVersionId,
        table_visible_columns: Vec<ColumnCatalog>,
        column_indices: Vec<usize>,
        default_columns: Vec<(usize, ExprImpl)>,
        generated_columns: Vec<(usize, ExprImpl)>,
        row_id_index: Option<usize>,
        returning: bool,
    ) -> Self {
        let generated_columns = if generated_columns.is_empty() {
            let visible_non_generated_indices = table_visible_columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| (!col.is_generated()).then_some(idx))
                .collect::<Vec<_>>();
            let generated_column_input_ref_mapping = ColIndexMapping::with_remaining_columns(
                &visible_non_generated_indices,
                table_visible_columns.len(),
            );
            table_visible_columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    col.column_desc
                        .generated_or_default_column
                        .as_ref()
                        .and_then(|generated_or_default| match generated_or_default {
                            GeneratedOrDefaultColumn::GeneratedColumn(generated) => {
                                Some((idx, generated))
                            }
                            GeneratedOrDefaultColumn::DefaultColumn(_) => None,
                        })
                })
                .map(|(idx, generated)| {
                    (
                        idx,
                        generated_column_input_ref_mapping.clone().rewrite_expr(
                            ExprImpl::from_expr_proto(generated.expr.as_ref().unwrap())
                                .expect("expr in generated columns corrupted"),
                        ),
                    )
                })
                .collect()
        } else {
            generated_columns
        };
        Self {
            table_name,
            table_id,
            table_version_id,
            table_visible_columns,
            input,
            column_indices,
            default_columns,
            generated_columns,
            row_id_index,
            returning,
        }
    }
}
