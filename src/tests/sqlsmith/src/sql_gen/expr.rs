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

use std::sync::Arc;

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_common::types::{DataType, DataTypeName, StructType};

use risingwave_frontend::expr::{agg_func_sigs, cast_sigs, func_sigs};
use risingwave_sqlparser::ast::{
    Expr, Ident, OrderByExpr, Value,
};


use crate::sql_gen::types::{
    data_type_to_ast_data_type,
};
use crate::sql_gen::{SqlGenerator, SqlGeneratorContext};

static STRUCT_FIELD_NAMES: [&str; 26] = [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
    "t", "u", "v", "w", "x", "y", "z",
];

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// In generating expression, there are two execution modes:
    /// 1) Can have Aggregate expressions (`can_agg` = true)
    ///    We can have aggregate of all bound columns (those present in GROUP BY and otherwise).
    ///    Not all GROUP BY columns need to be aggregated.
    /// 2) Can't have Aggregate expressions (`can_agg` = false)
    ///    Only columns present in GROUP BY can be selected.
    ///
    /// `inside_agg` indicates if we are calling `gen_expr` inside an aggregate.
    pub(crate) fn gen_expr(&mut self, typ: &DataType, context: SqlGeneratorContext) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            // Weight it more towards columns, scalar has much higher chance of being generated,
            // since it is usually used as fail-safe expression.
            return match self.rng.gen_bool(0.2) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ, context),
            };
        }

        if *typ == DataType::Boolean && self.rng.gen_bool(0.05) {
            return match self.rng.gen_bool(0.5) {
                true => {
                    let (ty, expr) = self.gen_arbitrary_expr(context);
                    let n = self.rng.gen_range(1..=10);
                    Expr::InList {
                        expr: Box::new(Expr::Nested(Box::new(expr))),
                        list: self.gen_n_exprs_with_type(n, &ty, context),
                        negated: self.flip_coin(),
                    }
                }
                false => {
                    // TODO: InSubquery expression may not be always bound in all context.
                    // Parts labelled workaround can be removed or
                    // generalized if it is bound in all contexts.
                    // https://github.com/risingwavelabs/risingwave/issues/1343
                    let old_ctxt = self.new_local_context(); // WORKAROUND
                    let (query, column) = self.gen_single_item_query();
                    let ty = column.data_type;
                    let expr = self.gen_simple_scalar(&ty); // WORKAROUND
                    let in_subquery_expr = Expr::InSubquery {
                        expr: Box::new(Expr::Nested(Box::new(expr))),
                        subquery: Box::new(query),
                        negated: self.flip_coin(),
                    };
                    self.restore_context(old_ctxt); // WORKAROUND
                    in_subquery_expr
                }
            };
        }

        // NOTE:
        // We generate AST first, then use its `Display` trait
        // to generate an sql string.
        // That may erase nesting context.
        // For instance `IN(a, b)` is `a IN b`.
        // this can lead to ambiguity, if `a` is an
        // INFIX/POSTFIX compound expression too:
        // - `a1 IN a2 IN b`
        // - `a1 >= a2 IN b`
        // ...
        // We just nest compound expressions to avoid this.
        let range = if context.can_gen_agg() { 99 } else { 90 };
        match self.rng.gen_range(0..=range) {
            0..=70 => Expr::Nested(Box::new(self.gen_func(typ, context))),
            71..=80 => self.gen_exists(typ, context),
            81..=90 => self.gen_explicit_cast(typ, context),
            91..=99 => self.gen_agg(typ),
            _ => unreachable!(),
        }
    }

    fn gen_data_type(&mut self) -> DataType {
        // Depth of struct/list nesting
        let depth = self.rng.gen_range(0..=1);
        self.gen_data_type_inner(depth)
    }

    fn gen_data_type_inner(&mut self, depth: usize) -> DataType {
        match self.rng.gen_bool(0.8) {
            true if !self.bound_columns.is_empty() => self
                .bound_columns
                .choose(&mut self.rng)
                .unwrap()
                .data_type
                .clone(),
            _ => {
                use {DataType as S, DataTypeName as T};
                let mut candidate_ret_types = vec![
                    T::Boolean,
                    T::Int16,
                    T::Int32,
                    T::Int64,
                    T::Decimal,
                    T::Float32,
                    T::Float64,
                    T::Varchar,
                    T::Date,
                    T::Timestamp,
                    // ENABLE: https://github.com/risingwavelabs/risingwave/issues/5826
                    // T::Timestamptz,
                    T::Time,
                    T::Interval,
                ];
                if depth > 0 {
                    candidate_ret_types.push(T::Struct);
                    candidate_ret_types.push(T::List);
                }
                let typ_name = candidate_ret_types.choose(&mut self.rng).unwrap();
                match typ_name {
                    T::Boolean => S::Boolean,
                    T::Int16 => S::Int16,
                    T::Int32 => S::Int32,
                    T::Int64 => S::Int64,
                    T::Decimal => S::Decimal,
                    T::Float32 => S::Float32,
                    T::Float64 => S::Float64,
                    T::Varchar => S::Varchar,
                    T::Date => S::Date,
                    T::Timestamp => S::Timestamp,
                    T::Timestamptz => S::Timestamptz,
                    T::Time => S::Time,
                    T::Interval => S::Interval,
                    T::Struct => self.gen_struct_data_type(depth - 1),
                    T::List => self.gen_list_data_type(depth - 1),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn gen_list_data_type(&mut self, depth: usize) -> DataType {
        DataType::List(Box::new(self.gen_data_type_inner(depth)))
    }

    fn gen_struct_data_type(&mut self, depth: usize) -> DataType {
        let num_fields = self.rng.gen_range(1..4);
        let fields = (0..num_fields)
            .map(|_| self.gen_data_type_inner(depth))
            .collect();
        let field_names = STRUCT_FIELD_NAMES[0..num_fields]
            .iter()
            .map(|s| (*s).into())
            .collect();
        DataType::Struct(Arc::new(StructType {
            fields,
            field_names,
        }))
    }

    /// Generates an arbitrary expression, but biased towards datatypes present in bound columns.
    pub(crate) fn gen_arbitrary_expr(&mut self, context: SqlGeneratorContext) -> (DataType, Expr) {
        let ret_type = self.gen_data_type();
        let expr = self.gen_expr(&ret_type, context);
        (ret_type, expr)
    }

    fn gen_col(&mut self, typ: &DataType, context: SqlGeneratorContext) -> Expr {
        let columns = if context.is_inside_agg() {
            if self.bound_relations.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_relations
                .choose(self.rng)
                .unwrap()
                .get_qualified_columns()
        } else {
            if self.bound_columns.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_columns.clone()
        };

        let matched_cols = columns
            .iter()
            .filter(|col| col.data_type == *typ)
            .collect::<Vec<_>>();
        if matched_cols.is_empty() {
            self.gen_simple_scalar(typ)
        } else {
            let col_def = matched_cols.choose(&mut self.rng).unwrap();
            Expr::Identifier(Ident::new_unchecked(&col_def.name))
        }
    }

    /// Generates `n` expressions of type `ret`.
    pub(crate) fn gen_n_exprs_with_type(
        &mut self,
        n: usize,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Vec<Expr> {
        (0..n).map(|_| self.gen_expr(ret, context)).collect()
    }

    fn gen_exists(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        if *ret != DataType::Boolean || context.can_gen_agg() {
            return self.gen_simple_scalar(ret);
        };
        // Generating correlated subquery tends to create queries which cannot be unnested.
        // we still want to test it, but reduce the chance it occurs.
        let (subquery, _) = match self.rng.gen_bool(0.05) {
            true => self.gen_correlated_query(),
            false => self.gen_local_query(),
        };
        Expr::Exists(Box::new(subquery))
    }

    pub(crate) fn gen_order_by(&mut self) -> Vec<OrderByExpr> {
        if self.bound_columns.is_empty() || !self.is_distinct_allowed {
            return vec![];
        }
        let mut order_by = vec![];
        while self.flip_coin() {
            let column = self.bound_columns.choose(&mut self.rng).unwrap();
            order_by.push(OrderByExpr {
                expr: Expr::Identifier(Ident::new_unchecked(&column.name)),
                asc: Some(self.rng.gen_bool(0.5)),
                nulls_first: None,
            })
        }
        order_by
    }
}


pub(crate) fn typed_null(ty: &DataType) -> Expr {
    Expr::Cast {
        expr: Box::new(sql_null()),
        data_type: data_type_to_ast_data_type(ty),
    }
}

/// Generates a `NULL` value.
pub(crate) fn sql_null() -> Expr {
    Expr::Value(Value::Null)
}

// TODO(kwannoel):
// Add variadic function signatures. Can add these functions
// to a FUNC_TABLE too.
pub fn print_function_table() -> String {
    let func_str = func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    let agg_func_str = agg_func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    let cast_str = cast_sigs()
        .map(|sig| {
            format!(
                "{:?} CAST {:?} -> {:?}",
                sig.context, sig.from_type, sig.to_type,
            )
        })
        .sorted()
        .join("\n");

    format!(
        "
==== FUNCTION SIGNATURES
{}

==== AGGREGATE FUNCTION SIGNATURES
{}

==== CAST SIGNATURES
{}
",
        func_str, agg_func_str, cast_str
    )
}
