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
use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_expr::expr::AggKind;
use risingwave_frontend::expr::{
    agg_func_sigs, cast_sigs, func_sigs, AggFuncSig, CastContext, CastSig, DataTypeName, ExprType,
    FuncSign,
};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    TrimWhereField, UnaryOperator, Value,
};

use crate::utils::data_type_name_to_ast_data_type;
use crate::SqlGenerator;

lazy_static::lazy_static! {
    static ref FUNC_TABLE: HashMap<DataTypeName, Vec<FuncSign>> = {
        init_op_table()
    };
}

lazy_static::lazy_static! {
    static ref AGG_FUNC_TABLE: HashMap<DataTypeName, Vec<AggFuncSig>> = {
        init_agg_table()
    };
}

lazy_static::lazy_static! {
    static ref CAST_TABLE: HashMap<DataTypeName, Vec<CastSig>> = {
        init_cast_table()
    };
}

fn init_op_table() -> HashMap<DataTypeName, Vec<FuncSign>> {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
}

fn init_agg_table() -> HashMap<DataTypeName, Vec<AggFuncSig>> {
    let mut funcs = HashMap::<DataTypeName, Vec<AggFuncSig>>::new();
    agg_func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
}

/// Build a cast map from return types to viable cast-signatures.
/// TODO: Generate implicit casts.
/// NOTE: We avoid cast from varchar to other datatypes apart from itself.
/// This is because arbitrary strings may not be able to cast,
/// creating large number of invalid queries.
fn init_cast_table() -> HashMap<DataTypeName, Vec<CastSig>> {
    let mut casts = HashMap::<DataTypeName, Vec<CastSig>>::new();
    cast_sigs()
        .filter(|cast| cast.context == CastContext::Explicit)
        .filter(|cast| {
            cast.from_type != DataTypeName::Varchar || cast.to_type == DataTypeName::Varchar
        })
        .for_each(|cast| casts.entry(cast.to_type).or_default().push(cast));
    casts
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// In generating expression, there are two execution modes:
    /// 1) Can have Aggregate expressions (`can_agg` = true)
    ///    We can have aggregate of all bound columns (those present in GROUP BY and otherwise).
    ///    Not all GROUP BY columns need to be aggregated.
    /// 2) Can't have Aggregate expressions (`can_agg` = false)
    ///    Only columns present in GROUP BY can be selected.
    ///
    /// `inside_agg` indicates if we are calling `gen_expr` inside an aggregate.
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ, inside_agg),
            };
        }

        if !can_agg {
            assert!(!inside_agg);
        }

        let range = if can_agg & !inside_agg { 99 } else { 90 };

        match self.rng.gen_range(0..=range) {
            0..=80 => self.gen_func(typ, can_agg, inside_agg),
            81..=90 => self.gen_cast(typ, can_agg, inside_agg),
            91..=99 => self.gen_agg(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            _ => unreachable!(),
        }
    }

    fn gen_col(&mut self, typ: DataTypeName, inside_agg: bool) -> Expr {
        let columns = if inside_agg {
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
            .filter(|col| col.data_type == typ)
            .collect::<Vec<_>>();
        if matched_cols.is_empty() {
            self.gen_simple_scalar(typ)
        } else {
            let col_def = matched_cols.choose(&mut self.rng).unwrap();
            Expr::Identifier(Ident::new(&col_def.name))
        }
    }

    fn gen_cast(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        self.gen_cast_inner(ret, can_agg, inside_agg)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generate casts from a cast map.
    fn gen_cast_inner(
        &mut self,
        ret: DataTypeName,
        can_agg: bool,
        inside_agg: bool,
    ) -> Option<Expr> {
        let casts = CAST_TABLE.get(&ret)?;
        let cast_sig = casts.choose(&mut self.rng).unwrap();
        let expr = self
            .gen_expr(cast_sig.from_type, can_agg, inside_agg)
            .into();
        let data_type = data_type_name_to_ast_data_type(cast_sig.to_type)?;
        Some(Expr::Cast { expr, data_type })
    }

    fn gen_func(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        match self.rng.gen_bool(0.1) {
            true => self.gen_variadic_func(ret, can_agg, inside_agg),
            false => self.gen_fixed_func(ret, can_agg, inside_agg),
        }
    }

    /// Generates functions with variable arity:
    /// CASE, COALESCE, CONCAT, CONCAT_WS
    fn gen_variadic_func(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        use DataTypeName as T;
        match ret {
            T::Varchar => match self.rng.gen_range(0..=3) {
                0 => self.gen_case(ret, can_agg, inside_agg),
                1 => self.gen_coalesce(ret, can_agg, inside_agg),
                2 => self.gen_concat(can_agg, inside_agg),
                3 => self.gen_concat_ws(can_agg, inside_agg),
                _ => unreachable!(),
            },
            _ => match self.rng.gen_bool(0.5) {
                true => self.gen_case(ret, can_agg, inside_agg),
                false => self.gen_coalesce(ret, can_agg, inside_agg),
            },
        }
    }

    fn gen_case(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        let n = self.rng.gen_range(1..10);
        Expr::Case {
            operand: None,
            conditions: self.gen_n_exprs_with_type(n, DataTypeName::Boolean, can_agg, inside_agg),
            results: self.gen_n_exprs_with_type(n, ret, can_agg, inside_agg),
            else_result: Some(Box::new(self.gen_expr(ret, can_agg, inside_agg))),
        }
    }

    fn gen_coalesce(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        let non_null = self.gen_expr(ret, can_agg, inside_agg);
        let position = self.rng.gen_range(0..10);
        let mut args = (0..10).map(|_| Expr::Value(Value::Null)).collect_vec();
        args[position] = non_null;
        Expr::Function(make_simple_func("coalesce", &args))
    }

    fn gen_concat(&mut self, can_agg: bool, inside_agg: bool) -> Expr {
        Expr::Function(make_simple_func(
            "concat",
            &self.gen_concat_args(can_agg, inside_agg),
        ))
    }

    fn gen_concat_ws(&mut self, can_agg: bool, inside_agg: bool) -> Expr {
        let sep = self.gen_expr(DataTypeName::Varchar, can_agg, inside_agg);
        let mut args = self.gen_concat_args(can_agg, inside_agg);
        args.insert(0, sep);
        Expr::Function(make_simple_func("concat_ws", &args))
    }

    // TODO: Gen implicit cast here.
    // Tracked by: https://github.com/risingwavelabs/risingwave/issues/3896.
    fn gen_concat_args(&mut self, can_agg: bool, inside_agg: bool) -> Vec<Expr> {
        let n = self.rng.gen_range(1..10);
        self.gen_n_exprs_with_type(n, DataTypeName::Varchar, can_agg, inside_agg)
    }

    /// Generates `n` expressions of type `ret`.
    fn gen_n_exprs_with_type(
        &mut self,
        n: usize,
        ret: DataTypeName,
        can_agg: bool,
        inside_agg: bool,
    ) -> Vec<Expr> {
        (0..n)
            .map(|_| self.gen_expr(ret, can_agg, inside_agg))
            .collect()
    }

    fn gen_fixed_func(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, can_agg, inside_agg))
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

    fn gen_agg(&mut self, ret: DataTypeName) -> Expr {
        // TODO: workaround for <https://github.com/risingwavelabs/risingwave/issues/4508>
        if ret == DataTypeName::Interval {
            return self.gen_simple_scalar(ret);
        }
        let funcs = match AGG_FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();

        // Common sense that the aggregation is allowed in the overall expression
        let can_agg = true;
        // show then the expression inside this function is in aggregate function
        let inside_agg = true;
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, can_agg, inside_agg))
            .collect();

        let distinct = self.flip_coin() && self.is_distinct_allowed;
        self.make_agg_expr(func.func, &exprs, distinct)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    /// Generates aggregate expressions. For internal / unsupported aggregators, we return `None`.
    fn make_agg_expr(&mut self, func: AggKind, exprs: &[Expr], distinct: bool) -> Option<Expr> {
        use AggKind as A;

        match func {
            A::Sum => Some(Expr::Function(make_agg_func("sum", exprs, distinct))),
            A::Min => Some(Expr::Function(make_agg_func("min", exprs, distinct))),
            A::Max => Some(Expr::Function(make_agg_func("max", exprs, distinct))),
            A::Count => Some(Expr::Function(make_agg_func("count", exprs, distinct))),
            A::Avg => Some(Expr::Function(make_agg_func("avg", exprs, distinct))),
            A::StringAgg => Some(Expr::Function(make_agg_func("string_agg", exprs, distinct))),
            A::SingleValue => None,
            A::ApproxCountDistinct => {
                if distinct {
                    None
                } else {
                    Some(Expr::Function(make_agg_func(
                        "approx_count_distinct",
                        exprs,
                        false,
                    )))
                }
            }
            // TODO(yuchao): `array_agg` support is still WIP, see #4657.
            A::ArrayAgg => None,
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
        E::Position => Some(Expr::Function(make_simple_func("position", &exprs))),
        E::RoundDigit => Some(Expr::Function(make_simple_func("round", &exprs))),
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
    let trim_where = if exprs.len() > 1 {
        Some((trim_type, Box::new(exprs[1].clone())))
    } else {
        None
    };
    Expr::Trim {
        expr: Box::new(exprs[0].clone()),
        trim_where,
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
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
    }
}

/// This is the function that generate aggregate function.
/// DISTINCT , ORDER BY or FILTER is allowed in aggregation functions。
/// Currently, distinct is allowed only, other and others rule is TODO: <https://github.com/risingwavelabs/risingwave/issues/3933>
fn make_agg_func(func_name: &str, exprs: &[Expr], _distinct: bool) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    // Distinct Aggregate shall be workaround until the following issue is resolved
    // https://github.com/risingwavelabs/risingwave/issues/4220
    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
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

pub(crate) fn sql_null() -> Expr {
    Expr::Value(Value::Null)
}

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
                sig.context, sig.to_type, sig.from_type,
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
