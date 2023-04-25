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
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{DataType, DataTypeName};
use risingwave_expr::agg::AggKind;
use risingwave_frontend::expr::{agg_func_sigs, cast_sigs, func_sigs, CastContext, ExprType};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, OrderByExpr,
    TrimWhereField, UnaryOperator, Value,
};

use crate::sql_gen::types::{
    data_type_to_ast_data_type, AGG_FUNC_TABLE, EXPLICIT_CAST_TABLE, FUNC_TABLE,
    IMPLICIT_CAST_TABLE, INVARIANT_FUNC_SET,
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
        DataType::List {
            datatype: Box::new(self.gen_data_type_inner(depth)),
        }
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

    fn gen_explicit_cast(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        self.gen_explicit_cast_inner(ret, context)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generate casts from a cast map.
    /// TODO: Assign casts have to be tested via `INSERT`.
    fn gen_explicit_cast_inner(
        &mut self,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Option<Expr> {
        let casts = EXPLICIT_CAST_TABLE.get(ret)?;
        let cast_sig = casts.choose(&mut self.rng).unwrap();

        match cast_sig.context {
            CastContext::Explicit => {
                let expr = self.gen_expr(&cast_sig.from_type, context).into();
                let data_type = data_type_to_ast_data_type(&cast_sig.to_type);
                Some(Expr::Cast { expr, data_type })
            }

            // TODO: Generate this when e2e inserts are generated.
            // T::Assign
            _ => unreachable!(),
        }
    }

    /// NOTE: This can result in ambiguous expressions.
    /// Should only be used in unambiguous context.
    fn gen_implicit_cast(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        self.gen_expr(ret, context)
    }

    fn gen_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        match self.rng.gen_bool(0.1) {
            true => self.gen_variadic_func(ret, context),
            false => self.gen_fixed_func(ret, context),
        }
    }

    /// Generates functions with variable arity:
    /// `CASE`, `COALESCE`, `CONCAT`, `CONCAT_WS`
    fn gen_variadic_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        use DataType as T;
        match ret {
            T::Varchar => match self.rng.gen_range(0..=3) {
                0 => self.gen_case(ret, context),
                1 => self.gen_coalesce(ret, context),
                2 => self.gen_concat(context),
                3 => self.gen_concat_ws(context),
                _ => unreachable!(),
            },
            _ => match self.rng.gen_bool(0.5) {
                true => self.gen_case(ret, context),
                false => self.gen_coalesce(ret, context),
            },
            // TODO: gen_regexpr
            // TODO: gen functions which return list, struct
        }
    }

    fn gen_case(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let n = self.rng.gen_range(1..4);
        Expr::Case {
            operand: None,
            conditions: self.gen_n_exprs_with_type(n, &DataType::Boolean, context),
            results: self.gen_n_exprs_with_type(n, ret, context),
            else_result: Some(Box::new(self.gen_expr(ret, context))),
        }
    }

    fn gen_coalesce(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let non_null = self.gen_expr(ret, context);
        let position = self.rng.gen_range(0..10);
        let mut args = (0..10).map(|_| Expr::Value(Value::Null)).collect_vec();
        args[position] = non_null;
        Expr::Function(make_simple_func("coalesce", &args))
    }

    fn gen_concat(&mut self, context: SqlGeneratorContext) -> Expr {
        Expr::Function(make_simple_func("concat", &self.gen_concat_args(context)))
    }

    fn gen_concat_ws(&mut self, context: SqlGeneratorContext) -> Expr {
        let sep = self.gen_expr(&DataType::Varchar, context);
        let mut args = self.gen_concat_args(context);
        args.insert(0, sep);
        Expr::Function(make_simple_func("concat_ws", &args))
    }

    fn gen_concat_args(&mut self, context: SqlGeneratorContext) -> Vec<Expr> {
        let n = self.rng.gen_range(1..4);
        (0..n)
            .map(|_| {
                if self.rng.gen_bool(0.1) {
                    self.gen_explicit_cast(&DataType::Varchar, context)
                } else {
                    self.gen_expr(&DataType::Varchar, context)
                }
            })
            .collect()
    }

    /// Generates `n` expressions of type `ret`.
    fn gen_n_exprs_with_type(
        &mut self,
        n: usize,
        ret: &DataType,
        context: SqlGeneratorContext,
    ) -> Vec<Expr> {
        (0..n).map(|_| self.gen_expr(ret, context)).collect()
    }

    fn gen_fixed_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let funcs = match FUNC_TABLE.get(ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let can_implicit_cast = INVARIANT_FUNC_SET.contains(&func.func);
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| {
                if let Some(from_tys) = IMPLICIT_CAST_TABLE.get(t)
                        && can_implicit_cast && self.flip_coin() {
                    let from_ty = &from_tys.choose(&mut self.rng).unwrap().from_type;
                        self.gen_implicit_cast(from_ty, context)
                } else {
                    self.gen_expr(t, context)
                }
            })
            .collect();
        let expr = if exprs.len() == 1 {
            make_unary_op(func.func, &exprs[0])
        } else if exprs.len() == 2 {
            make_bin_op(func.func, &exprs)
        } else {
            None
        };
        expr.or_else(|| make_general_expr(func.func, exprs))
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
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

    fn gen_agg(&mut self, ret: &DataType) -> Expr {
        let funcs = match AGG_FUNC_TABLE.get(ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();

        let context = SqlGeneratorContext::new();
        let context = context.set_inside_agg();
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(t, context))
            .collect();

        let distinct = self.flip_coin() && self.is_distinct_allowed;
        let filter = if self.flip_coin() {
            let context = SqlGeneratorContext::new_with_can_agg(false);
            // ENABLE: https://github.com/risingwavelabs/risingwave/issues/4762
            // Prevent correlated query with `FILTER`
            let old_ctxt = self.new_local_context();
            let expr = Some(Box::new(self.gen_expr(&DataType::Boolean, context)));
            self.restore_context(old_ctxt);
            expr
        } else {
            None
        };
        let order_by = if self.flip_coin() && !distinct {
            self.gen_order_by()
        } else {
            vec![]
        };
        self.make_agg_expr(func.func, &exprs, distinct, filter, order_by)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generates aggregate expressions. For internal / unsupported aggregators, we return `None`.
    fn make_agg_expr(
        &mut self,
        func: AggKind,
        exprs: &[Expr],
        distinct: bool,
        filter: Option<Box<Expr>>,
        order_by: Vec<OrderByExpr>,
    ) -> Option<Expr> {
        use AggKind as A;
        match func {
            A::StringAgg => {
                // distinct and non_distinct_string_agg are incompatible according to
                // https://github.com/risingwavelabs/risingwave/blob/a703dc7d725aa995fecbaedc4e9569bc9f6ca5ba/src/frontend/src/optimizer/plan_node/logical_agg.rs#L394
                if self.is_distinct_allowed && !distinct {
                    None
                } else {
                    Some(Expr::Function(make_agg_func(
                        "string_agg",
                        exprs,
                        distinct,
                        filter,
                        order_by,
                    )))
                }
            }
            A::FirstValue => None,
            A::ApproxCountDistinct => {
                if self.is_distinct_allowed {
                    None
                } else {
                    // It does not make sense to have `distinct`.
                    // That requires precision, which `approx_count_distinct` does not provide.
                    Some(Expr::Function(make_agg_func(
                        "approx_count_distinct",
                        exprs,
                        false,
                        filter,
                        order_by,
                    )))
                }
            }
            other => Some(Expr::Function(make_agg_func(
                &other.to_string(),
                exprs,
                distinct,
                filter,
                order_by,
            ))),
            A::Unspecified => unreachable!(),
        }
    }
}

fn make_unary_op(func: ExprType, expr: &Expr) -> Option<Expr> {
    use {ExprType as E, UnaryOperator as U};
    let unary_op = match func {
        E::Neg => U::Minus,
        E::Not => U::Not,
        E::BitwiseNot => U::PGBitwiseNot,
        _ => return None,
    };
    Some(Expr::UnaryOp {
        op: unary_op,
        expr: Box::new(expr.clone()),
    })
}

fn make_general_expr(func: ExprType, exprs: Vec<Expr>) -> Option<Expr> {
    use ExprType as E;

    match func {
        E::Trim | E::Ltrim | E::Rtrim => Some(make_trim(func, exprs)),
        E::IsNull => Some(Expr::IsNull(Box::new(exprs[0].clone()))),
        E::IsNotNull => Some(Expr::IsNotNull(Box::new(exprs[0].clone()))),
        E::IsTrue => Some(Expr::IsTrue(Box::new(exprs[0].clone()))),
        E::IsNotTrue => Some(Expr::IsNotTrue(Box::new(exprs[0].clone()))),
        E::IsFalse => Some(Expr::IsFalse(Box::new(exprs[0].clone()))),
        E::IsNotFalse => Some(Expr::IsNotFalse(Box::new(exprs[0].clone()))),
        E::Position => Some(Expr::Function(make_simple_func("strpos", &exprs))),
        E::RoundDigit => Some(Expr::Function(make_simple_func("round", &exprs))),
        E::Pow => Some(Expr::Function(make_simple_func("pow", &exprs))),
        E::Repeat => Some(Expr::Function(make_simple_func("repeat", &exprs))),
        E::CharLength => Some(Expr::Function(make_simple_func("char_length", &exprs))),
        E::Substr => Some(Expr::Function(make_simple_func("substr", &exprs))),
        E::Length => Some(Expr::Function(make_simple_func("length", &exprs))),
        E::Upper => Some(Expr::Function(make_simple_func("upper", &exprs))),
        E::Lower => Some(Expr::Function(make_simple_func("lower", &exprs))),
        E::Replace => Some(Expr::Function(make_simple_func("replace", &exprs))),
        E::Md5 => Some(Expr::Function(make_simple_func("md5", &exprs))),
        E::ToChar => Some(Expr::Function(make_simple_func("to_char", &exprs))),
        E::SplitPart => Some(Expr::Function(make_simple_func("split_part", &exprs))),
        E::Encode => Some(Expr::Function(make_simple_func("encode", &exprs))),
        E::Decode => Some(Expr::Function(make_simple_func("decode", &exprs))),
        // TODO: Tracking issue: https://github.com/risingwavelabs/risingwave/issues/112
        // E::Translate => Some(Expr::Function(make_simple_func("translate", &exprs))),
        E::Overlay => Some(make_overlay(exprs)),
        _ => None,
    }
}

fn make_trim(func: ExprType, exprs: Vec<Expr>) -> Expr {
    use ExprType as E;

    let trim_type = match func {
        E::Trim => TrimWhereField::Both,
        E::Ltrim => TrimWhereField::Leading,
        E::Rtrim => TrimWhereField::Trailing,
        _ => unreachable!(),
    };
    let trim_what = if exprs.len() > 1 {
        Some(Box::new(exprs[1].clone()))
    } else {
        None
    };
    Expr::Trim {
        expr: Box::new(exprs[0].clone()),
        trim_where: Some(trim_type),
        trim_what,
    }
}

fn make_overlay(exprs: Vec<Expr>) -> Expr {
    if exprs.len() == 3 {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: None,
        }
    } else {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: Some(Box::new(exprs[3].clone())),
        }
    }
}

/// Generates simple functions such as `length`, `round`, `to_char`. These operate on datums instead
/// of columns / rows.
fn make_simple_func(func_name: &str, exprs: &[Expr]) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new_unchecked(func_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
    }
}

/// This is the function that generate aggregate function.
/// DISTINCT, ORDER BY or FILTER is allowed in aggregation functions。
fn make_agg_func(
    func_name: &str,
    exprs: &[Expr],
    distinct: bool,
    filter: Option<Box<Expr>>,
    order_by: Vec<OrderByExpr>,
) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new_unchecked(func_name)]),
        args,
        over: None,
        distinct,
        order_by,
        filter,
    }
}

fn make_bin_op(func: ExprType, exprs: &[Expr]) -> Option<Expr> {
    use {BinaryOperator as B, ExprType as E};
    let bin_op = match func {
        E::Add => B::Plus,
        E::Subtract => B::Minus,
        E::Multiply => B::Multiply,
        E::Divide => B::Divide,
        E::Modulus => B::Modulo,
        E::GreaterThan => B::Gt,
        E::GreaterThanOrEqual => B::GtEq,
        E::LessThan => B::Lt,
        E::LessThanOrEqual => B::LtEq,
        E::Equal => B::Eq,
        E::NotEqual => B::NotEq,
        E::And => B::And,
        E::Or => B::Or,
        E::Like => B::Like,
        E::BitwiseAnd => B::BitwiseAnd,
        E::BitwiseOr => B::BitwiseOr,
        E::BitwiseXor => B::PGBitwiseXor,
        E::BitwiseShiftLeft => B::PGBitwiseShiftLeft,
        E::BitwiseShiftRight => B::PGBitwiseShiftRight,
        _ => return None,
    };
    Some(Expr::BinaryOp {
        left: Box::new(exprs[0].clone()),
        op: bin_op,
        right: Box::new(exprs[1].clone()),
    })
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
