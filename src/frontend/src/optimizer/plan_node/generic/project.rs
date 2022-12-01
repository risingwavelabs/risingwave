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

use std::fmt;

use itertools::Itertools;
use pretty::RcDoc;
use risingwave_common::catalog::{Field, Schema};

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{Expr, ExprDisplay, ExprImpl};
use crate::optimizer::plan_node::explain::{NodeExplain, field_doc_iter};
use crate::session::OptimizerContextRef;
use crate::utils::ColIndexMapping;

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    pub input: PlanRef,
}

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

impl<PlanRef: GenericPlanRef> Project<PlanRef> {
    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        Project { exprs, input }
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let exprs = &self.exprs;
        let input_len = self.input.schema().len();
        let mut map = vec![None; exprs.len()];
        for (i, expr) in exprs.iter().enumerate() {
            map[i] = match expr {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }
}

impl<'a, PlanRef: GenericPlanRef> NodeExplain<'a> for Project<PlanRef> {
    fn distill_fields(&self) -> RcDoc<'a, ()> {
        RcDoc::intersperse(
            [field_doc_iter(
                "exprs",
                self.exprs.iter().map(|expr| ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                }),
            )],
            RcDoc::line(),
        )
    }
}
