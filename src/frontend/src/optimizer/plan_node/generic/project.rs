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
use risingwave_common::catalog::{Field, Schema};

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{
    assert_input_ref, Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor, InputRef,
};
use crate::session::OptimizerContextRef;
use crate::utils::ColIndexMapping;

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    pub input: PlanRef,
    // we need some check when construct the `Project::new`
    _private: (),
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
    fn check_expr_type(expr: &ExprImpl) -> std::result::Result<(), &'static str> {
        if expr.has_subquery() {
            return Err("subquery");
        }
        if expr.has_agg_call() {
            return Err("aggregate function");
        }
        if expr.has_table_function() {
            return Err("table function");
        }
        if expr.has_window_function() {
            return Err("window function");
        }
        Ok(())
    }

    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        for expr in &exprs {
            assert_input_ref!(expr, input.schema().fields().len());
            Self::check_expr_type(expr)
                .map_err(|expr| format!("{expr} should not in Project operator"))
                .unwrap();
        }
        Project {
            exprs,
            input,
            _private: (),
        }
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        builder.field(
            "exprs",
            &self
                .exprs
                .iter()
                .map(|expr| ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                })
                .collect_vec(),
        );
        builder.finish()
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
