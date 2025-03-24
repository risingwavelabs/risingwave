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
use rand::Rng;
use rand::seq::IndexedRandom;
use risingwave_common::types::DataType;
use risingwave_frontend::expr::ExprType;
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgList, Ident,
    ObjectName, TrimWhereField, UnaryOperator, Value,
};

use crate::sql_gen::types::{FUNC_TABLE, IMPLICIT_CAST_TABLE, INVARIANT_FUNC_SET};
use crate::sql_gen::{SqlGenerator, SqlGeneratorContext};

impl<R: Rng> SqlGenerator<'_, R> {
    pub fn gen_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        match self.rng.random_bool(0.1) {
            true => self.gen_special_func(ret, context),
            false => self.gen_fixed_func(ret, context),
        }
    }

    /// Generates functions with special properties, e.g.
    /// `CASE`, `COALESCE`, `CONCAT`, `CONCAT_WS`, `OVERLAY`.
    /// These require custom logic for arguments.
    /// For instance, `OVERLAY` requires a positive length argument,
    /// and `CONCAT` and `CONCAT_WS` require variable number of arguments.
    fn gen_special_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        use DataType as T;
        match ret {
            T::Varchar => match self.rng.random_range(0..=4) {
                0 => self.gen_case(ret, context),
                1 => self.gen_coalesce(ret, context),
                2 => self.gen_concat(context),
                3 => self.gen_concat_ws(context),
                4 => self.gen_overlay(context),
                _ => unreachable!(),
            },
            T::Bytea => self.gen_decode(context),
            _ => match self.rng.random_bool(0.5) {
                true => self.gen_case(ret, context),
                false => self.gen_coalesce(ret, context),
            },
            // TODO: gen_regexpr
            // TODO: gen functions which return list, struct
        }
    }

    /// We do custom generation for the `OVERLAY` function call.
    /// See: [`https://github.com/risingwavelabs/risingwave/issues/10695`] for rationale.
    fn gen_overlay(&mut self, context: SqlGeneratorContext) -> Expr {
        let expr = Box::new(self.gen_expr(&DataType::Varchar, context));
        let new_substring = Box::new(self.gen_expr(&DataType::Varchar, context));
        let start = Box::new(self.gen_range_scalar(&DataType::Int32, 1, 10).unwrap());
        let count = if self.flip_coin() {
            None
        } else {
            Some(Box::new(
                self.gen_range_scalar(&DataType::Int32, 1, 10).unwrap(),
            ))
        };
        Expr::Overlay {
            expr,
            new_substring,
            start,
            count,
        }
    }

    fn gen_case(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let n = self.rng.random_range(1..4);
        Expr::Case {
            operand: None,
            conditions: self.gen_n_exprs_with_type(n, &DataType::Boolean, context),
            results: self.gen_n_exprs_with_type(n, ret, context),
            else_result: Some(Box::new(self.gen_expr(ret, context))),
        }
    }

    fn gen_coalesce(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let non_null = self.gen_expr(ret, context);
        let position = self.rng.random_range(0..10);
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
        let n = self.rng.random_range(1..4);
        (0..n)
            .map(|_| {
                if self.rng.random_bool(0.1) {
                    self.gen_explicit_cast(&DataType::Varchar, context)
                } else {
                    self.gen_expr(&DataType::Varchar, context)
                }
            })
            .collect()
    }

    fn gen_decode(&mut self, context: SqlGeneratorContext) -> Expr {
        let input_string = self.gen_expr(&DataType::Bytea, context);
        let encoding = &["base64", "hex", "escape"].choose(&mut self.rng).unwrap();
        let args = vec![
            input_string,
            Expr::Value(Value::SingleQuotedString(encoding.to_string())),
        ];
        let encoded_string = Expr::Function(make_simple_func("encode", &args));
        let args = vec![
            encoded_string,
            Expr::Value(Value::SingleQuotedString(encoding.to_string())),
        ];
        Expr::Function(make_simple_func("decode", &args))
    }

    fn gen_fixed_func(&mut self, ret: &DataType, context: SqlGeneratorContext) -> Expr {
        let funcs = match FUNC_TABLE.get(ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let can_implicit_cast = INVARIANT_FUNC_SET.contains(&func.name.as_scalar());
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| {
                if let Some(from_tys) = IMPLICIT_CAST_TABLE.get(t.as_exact())
                    && can_implicit_cast
                    && self.flip_coin()
                {
                    let from_ty = &from_tys.choose(&mut self.rng).unwrap().from_type;
                    self.gen_implicit_cast(from_ty, context)
                } else {
                    self.gen_expr(t.as_exact(), context)
                }
            })
            .collect();
        let expr = if exprs.len() == 1 {
            make_unary_op(func.name.as_scalar(), &exprs[0])
        } else if exprs.len() == 2 {
            make_bin_op(func.name.as_scalar(), &exprs)
        } else {
            None
        };
        expr.or_else(|| make_general_expr(func.name.as_scalar(), exprs))
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
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

/// General expressions do not fall under unary / binary op, so they are constructed differently.
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
        // NOTE(kwannoel): I disabled `Overlay`, its arguments require special handling.
        // We generate it in `gen_special_func` instead.
        // E::Overlay => Some(make_overlay(exprs)),
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

/// Generates simple functions such as `length`, `round`, `to_char`. These operate on datums instead
/// of columns / rows.
pub fn make_simple_func(func_name: &str, exprs: &[Expr]) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        scalar_as_agg: false,
        name: ObjectName(vec![Ident::new_unchecked(func_name)]),
        arg_list: FunctionArgList::args_only(args),
        over: None,
        filter: None,
        within_group: None,
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
        E::Like => B::PGLikeMatch,
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
