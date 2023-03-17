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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{assert_input_ref, Expr, ExprDisplay, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt};

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

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::manual_non_exhaustive)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    pub input: PlanRef,
    // we need some check when construct the `Project::new`
    _private: (),
}

impl<PlanRef> Project<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.exprs = self
            .exprs
            .iter()
            .map(|e| r.rewrite_expr(e.clone()))
            .collect();
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Project<PlanRef> {
    fn schema(&self) -> Schema {
        let o2i = self.o2i_col_mapping();
        let exprs = &self.exprs;
        let input_schema = self.input.schema();
        let ctx = self.ctx();
        let fields = exprs
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                // Get field info from o2i.
                let (name, sub_fields, type_name) = match o2i.try_map(i) {
                    Some(input_idx) => {
                        let field = input_schema.fields()[input_idx].clone();
                        (field.name, field.sub_fields, field.type_name)
                    }
                    None => match expr {
                        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => (
                            format!("{:?}", ExprDisplay { expr, input_schema }),
                            vec![],
                            String::new(),
                        ),
                        _ => (
                            format!("$expr{}", ctx.next_expr_display_id()),
                            vec![],
                            String::new(),
                        ),
                    },
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

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let i2o = self.i2o_col_mapping();
        i2o.rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }
}

impl<PlanRef: GenericPlanRef> Project<PlanRef> {
    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        for expr in &exprs {
            assert_input_ref!(expr, input.schema().fields().len());
            check_expr_type(expr)
                .map_err(|expr| format!("{expr} should not in Project operator"))
                .unwrap();
        }
        Project {
            exprs,
            input,
            _private: (),
        }
    }

    /// Creates a `Project` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        if mapping.target_size() == 0 {
            // The mapping is empty, so the parent actually doesn't need the output of the input.
            // This can happen when the parent node only selects constant expressions.
            return Self::new(vec![], input);
        };
        let mut input_refs = vec![None; mapping.target_size()];
        for (src, tar) in mapping.mapping_pairs() {
            assert_eq!(input_refs[tar], None);
            input_refs[tar] = Some(src);
        }
        let input_schema = input.schema();
        let exprs: Vec<ExprImpl> = input_refs
            .into_iter()
            .map(|i| i.unwrap())
            .map(|i| InputRef::new(i, input_schema.fields()[i].data_type()).into())
            .collect();

        Self::new(exprs, input)
    }

    /// Creates a `Project` which select some columns from the input.
    pub fn with_out_fields(input: PlanRef, out_fields: &FixedBitSet) -> Self {
        Self::with_out_col_idx(input, out_fields.ones())
    }

    /// Creates a `Project` which select some columns from the input.
    pub fn with_out_col_idx(input: PlanRef, out_fields: impl Iterator<Item = usize>) -> Self {
        let input_schema = input.schema();
        let exprs = out_fields
            .map(|index| InputRef::new(index, input_schema[index].data_type()).into())
            .collect();
        Self::new(exprs, input)
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>, schema: &Schema) {
        builder.field(
            "exprs",
            &self
                .exprs
                .iter()
                .zip_eq_fast(schema.fields().iter())
                .map(|(expr, field)| AliasedExpr {
                    expr: ExprDisplay {
                        expr,
                        input_schema: self.input.schema(),
                    },
                    alias: {
                        match expr {
                            ExprImpl::InputRef(_) | ExprImpl::Literal(_) => None,
                            _ => Some(field.name.clone()),
                        }
                    },
                })
                .collect_vec(),
        );
    }

    pub fn fmt_with_name(
        &self,
        f: &mut fmt::Formatter<'_>,
        name: &str,
        schema: &Schema,
    ) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        self.fmt_fields_with_builder(&mut builder, schema);
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

    pub fn is_all_inputref(&self) -> bool {
        self.exprs
            .iter()
            .all(|expr| matches!(expr, ExprImpl::InputRef(_)))
    }

    pub fn is_identity(&self) -> bool {
        self.exprs.len() == self.input.schema().len()
        && self
            .exprs
            .iter()
            .zip_eq_fast(self.input.schema().fields())
            .enumerate()
            .all(|(i, (expr, field))| {
                matches!(expr, ExprImpl::InputRef(input_ref) if **input_ref == InputRef::new(i, field.data_type()))
            })
    }

    pub fn try_as_projection(&self) -> Option<Vec<usize>> {
        self.exprs
            .iter()
            .map(|expr| match expr {
                ExprImpl::InputRef(input_ref) => Some(input_ref.index),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    }
}

/// Construct a `Project` and dedup expressions.
/// expressions
#[derive(Default)]
pub struct ProjectBuilder {
    exprs: Vec<ExprImpl>,
    exprs_index: HashMap<ExprImpl, usize>,
}

impl ProjectBuilder {
    /// add an expression to the `LogicalProject` and return the column index of the project's
    /// output
    pub fn add_expr(&mut self, expr: &ExprImpl) -> std::result::Result<usize, &'static str> {
        check_expr_type(expr)?;
        if let Some(idx) = self.exprs_index.get(expr) {
            Ok(*idx)
        } else {
            let index = self.exprs.len();
            self.exprs.push(expr.clone());
            self.exprs_index.insert(expr.clone(), index);
            Ok(index)
        }
    }

    pub fn expr_index(&self, expr: &ExprImpl) -> Option<usize> {
        check_expr_type(expr).ok()?;
        self.exprs_index.get(expr).copied()
    }

    /// build the `LogicalProject` from `LogicalProjectBuilder`
    pub fn build<PlanRef: GenericPlanRef>(self, input: PlanRef) -> Project<PlanRef> {
        Project::new(self.exprs, input)
    }
}

/// Auxiliary struct for displaying `expr AS alias`
pub struct AliasedExpr<'a> {
    pub expr: ExprDisplay<'a>,
    pub alias: Option<String>,
}

impl fmt::Debug for AliasedExpr<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(alias) => write!(f, "{:?} as {}", self.expr, alias),
            None => write!(f, "{:?}", self.expr),
        }
    }
}
