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
                    && can_implicit_cast
                    && self.flip_coin()
                {
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

        let distinct = self.flip_coin() && self.is_distinct_allowed && !exprs.is_empty();
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

        // Only can generate ORDER BY if distinct_allowed is banned globally in the generator.
        // This avoids ORDER BY + Distinct aggregate from being generated.
        // See https://github.com/risingwavelabs/risingwave/issues/9860.
        let order_by = if self.flip_coin() && !distinct && !self.is_distinct_allowed {
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
        E::Sha1 => Some(Expr::Function(make_simple_func("sha1", &exprs))),
        E::Sha224 => Some(Expr::Function(make_simple_func("sha224", &exprs))),
        E::Sha256 => Some(Expr::Function(make_simple_func("sha256", &exprs))),
        E::Sha384 => Some(Expr::Function(make_simple_func("sha384", &exprs))),
        E::Sha512 => Some(Expr::Function(make_simple_func("sha512", &exprs))),
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
    let args = if exprs.is_empty() {
        // The only agg without args is `count`.
        // `select proname from pg_proc where array_length(proargtypes, 1) = 0 and prokind = 'a';`
        vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
    } else {
        exprs
            .iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
            .collect()
    };

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
