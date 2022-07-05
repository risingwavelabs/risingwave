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

use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_frontend::expr::{func_sigs, DataTypeName, ExprType, FuncSign};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    TrimWhereField, UnaryOperator, Value,
};

use crate::SqlGenerator;

lazy_static::lazy_static! {
    static ref FUNC_TABLE: HashMap<DataTypeName, Vec<FuncSign>> = {
        init_op_table()
    };
}

fn init_op_table() -> HashMap<DataTypeName, Vec<FuncSign>> {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ),
            };
        }
        match self.rng.gen_range(0..=99) {
            0..=99 => self.gen_func(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            _ => unreachable!(),
        }
    }

    fn gen_col(&mut self, typ: DataTypeName) -> Expr {
        if self.bound_relations.is_empty() {
            return self.gen_simple_scalar(typ);
        }
        let rel = self.bound_relations.choose(&mut self.rng).unwrap();
        let matched_cols = rel
            .columns
            .iter()
            .filter(|col| col.data_type == typ)
            .collect::<Vec<_>>();
        if matched_cols.is_empty() {
            self.gen_simple_scalar(typ)
        } else {
            let col_def = matched_cols.choose(&mut self.rng).unwrap();
            Expr::Identifier(Ident::new(format!("{}.{}", rel.name, col_def.name)))
        }
    }

    fn gen_func(&mut self, ret: DataTypeName) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func.inputs_type.iter().map(|t| self.gen_expr(*t)).collect();
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

    fn can_recurse(&mut self) -> bool {
        self.rng.gen_bool(0.3)
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
        E::Position => Some(Expr::Function(make_func("position", &exprs))),
        E::RoundDigit => Some(Expr::Function(make_func("round", &exprs))),
        E::Repeat => Some(Expr::Function(make_func("repeat", &exprs))),
        E::CharLength => Some(Expr::Function(make_func("char_length", &exprs))),
        E::Substr => Some(Expr::Function(make_func("substr", &exprs))),
        E::Length => Some(Expr::Function(make_func("length", &exprs))),
        E::Upper => Some(Expr::Function(make_func("upper", &exprs))),
        E::Lower => Some(Expr::Function(make_func("lower", &exprs))),
        E::Replace => Some(Expr::Function(make_func("replace", &exprs))),
        E::Md5 => Some(Expr::Function(make_func("md5", &exprs))),
        E::ToChar => Some(Expr::Function(make_func("to_char", &exprs))),
        E::Overlay => Some(Expr::Function(make_func("overlay", &exprs))),
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

fn make_func(func_name: &str, exprs: &[Expr]) -> Function {
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
