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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::generic::*;
use super::EqJoinPredicate;
use crate::expr::{Expr, ExprDisplay};
use crate::session::OptimizerContextRef;
use crate::utils::ColIndexMapping;

impl<PlanRef: GenericPlanRef> GenericPlanNode for Project<PlanRef> {
    fn schema(&self) -> Schema {
        let o2i = self.o2i_col_mapping();
        let exprs = &self.exprs;
        let input_schema = self.input.schema();
        let fields = exprs
            .iter()
            .enumerate()
            .map(|(id, expr)| {
                // Get field info from o2i.
                let (name, sub_fields, type_name) = match o2i.try_map(id) {
                    Some(input_idx) => {
                        let field = input_schema.fields()[input_idx].clone();
                        (field.name, field.sub_fields, field.type_name)
                    }
                    None => (
                        format!("{:?}", ExprDisplay { expr, input_schema }),
                        vec![],
                        String::new(),
                    ),
                };
                Field::with_struct(expr.return_type(), name, sub_fields, type_name)
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        self.input
            .logical_pk()
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Agg<PlanRef> {
    fn schema(&self) -> Schema {
        let fields = self
            .group_key
            .iter()
            .cloned()
            .map(|i| self.input.schema().fields()[i].clone())
            .chain(self.agg_calls.iter().map(|agg_call| {
                let plan_agg_call_display = PlanAggCallDisplay {
                    plan_agg_call: agg_call,
                    input_schema: self.input.schema(),
                };
                let name = format!("{:?}", plan_agg_call_display);
                Field::with_name(agg_call.return_type.clone(), name)
            }))
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some((0..self.group_key.len()).into_iter().collect_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for HopWindow<PlanRef> {
    fn schema(&self) -> Schema {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        let original_schema: Schema = self
            .input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        self.output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let window_start_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len());
        let window_end_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len() + 1);
        if window_start_index.is_none() && window_end_index.is_none() {
            None
        } else {
            let mut pk = self
                .input
                .logical_pk()
                .iter()
                .filter_map(|&pk_idx| self.output_indices.iter().position(|&idx| idx == pk_idx))
                .collect_vec();
            if let Some(start_idx) = window_start_index {
                pk.push(start_idx);
            };
            if let Some(end_idx) = window_end_index {
                pk.push(end_idx);
            };
            Some(pk)
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Filter<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Join<PlanRef> {
    fn schema(&self) -> Schema {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let i2l = self.i2l_col_mapping();
        let i2r = self.i2r_col_mapping();
        let fields = self
            .output_indices
            .iter()
            .map(|&i| match (i2l.try_map(i), i2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(
                    "left len {}, right len {}, i {}, lmap {:?}, rmap {:?}",
                    left_schema.len(),
                    right_schema.len(),
                    i,
                    i2l,
                    i2r
                ),
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let _left_len = self.left.schema().len();
        let _right_len = self.right.schema().len();
        let left_pk = self.left.logical_pk();
        let right_pk = self.right.logical_pk();
        let l2i = self.l2i_col_mapping();
        let r2i = self.r2i_col_mapping();
        let full_out_col_num = self.internal_column_num();
        let i2o = ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

        let pk_indices = left_pk
            .iter()
            .map(|index| l2i.try_map(*index))
            .chain(right_pk.iter().map(|index| r2i.try_map(*index)))
            .flatten()
            .map(|index| i2o.try_map(index))
            .collect::<Option<Vec<_>>>();

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        pk_indices.and_then(|mut pk_indices| {
            let left_len = self.left.schema().len();
            let right_len = self.right.schema().len();
            let eq_predicate = EqJoinPredicate::create(left_len, right_len, self.on.clone());

            let l2i = self.l2i_col_mapping();
            let r2i = self.r2i_col_mapping();
            let full_out_col_num = self.internal_column_num();
            let i2o =
                ColIndexMapping::with_remaining_columns(&self.output_indices, full_out_col_num);

            for (lk, rk) in eq_predicate.eq_indexes() {
                if let Some(lk) = l2i.try_map(lk) {
                    let out_k = i2o.try_map(lk)?;
                    if !pk_indices.contains(&out_k) {
                        pk_indices.push(out_k);
                    }
                }
                if let Some(rk) = r2i.try_map(rk) {
                    let out_k = i2o.try_map(rk)?;
                    if !pk_indices.contains(&out_k) {
                        pk_indices.push(out_k);
                    }
                }
            }
            Some(pk_indices)
        })
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.left.ctx()
    }
}

impl GenericPlanNode for Scan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.table_desc.columns[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let id_to_op_idx = Self::get_id_to_op_idx_mapping(&self.output_col_idx, &self.table_desc);
        self.table_desc
            .stream_key
            .iter()
            .map(|&c| {
                id_to_op_idx
                    .get(&self.table_desc.columns[c].column_id)
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        unimplemented!()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for TopN<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl GenericPlanNode for Source {
    fn schema(&self) -> Schema {
        let fields = self
            .catalog
            .columns
            .iter()
            .map(|c| (&c.column_desc).into())
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let mut id_to_idx = HashMap::new();
        self.catalog
            .columns
            .iter()
            .enumerate()
            .for_each(|(idx, c)| {
                id_to_idx.insert(c.column_id(), idx);
            });
        self.catalog
            .pk_col_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied())
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        unimplemented!()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for ProjectSet<PlanRef> {
    fn schema(&self) -> Schema {
        let input_schema = self.input.schema();
        let o2i = self.o2i_col_mapping();
        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(self.select_list.iter().enumerate().map(|(idx, expr)| {
            let idx = idx + 1;
            // Get field info from o2i.
            let (name, sub_fields, type_name) = match o2i.try_map(idx) {
                Some(input_idx) => {
                    let field = input_schema.fields()[input_idx].clone();
                    (field.name, field.sub_fields, field.type_name)
                }
                None => (
                    format!("{:?}", ExprDisplay { expr, input_schema }),
                    vec![],
                    String::new(),
                ),
            };
            Field::with_struct(expr.return_type(), name, sub_fields, type_name)
        }));

        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        let mut pk = self
            .input
            .logical_pk()
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        // add `projected_row_id` to pk
        pk.push(0);
        Some(pk)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
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
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Union<PlanRef> {
    fn schema(&self) -> Schema {
        self.inputs[0].schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        // Union all its inputs pks + source_col if exists
        let mut pk_indices = vec![];
        for input in &self.inputs {
            for pk in input.logical_pk() {
                if !pk_indices.contains(pk) {
                    pk_indices.push(*pk);
                }
            }
        }
        if let Some(source_col) = self.source_col {
            pk_indices.push(source_col)
        }
        Some(pk_indices)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.inputs[0].ctx()
    }
}
