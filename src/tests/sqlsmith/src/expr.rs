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
use risingwave_frontend::expr::{func_sigs, DataTypeName, ExprType, FuncSign, AggCall};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    TrimWhereField, UnaryOperator, Value,
};

use crate::SqlGenerator;
use risingwave_expr::expr::AggKind;
// use risingwave_pb::expr::agg_call::Type as AggType;

lazy_static::lazy_static! {
    static ref FUNC_TABLE: HashMap<DataTypeName, Vec<FuncSign>> = {
        init_op_table()
    };
}

pub struct AggFuncSign {
    pub func: AggKind,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

impl AggFuncSign{
    pub fn new(func: AggKind, inputs_type: Vec<DataTypeName>, ret_type: DataTypeName) -> Self{
        Self {
            func,
            inputs_type,
            ret_type,
        }
    }
}
lazy_static::lazy_static! {
    static ref AGG_FUNC_TABLE: HashMap<DataTypeName, Vec<AggFuncSign>> = {
        init_agg_table()
    };
}

fn init_op_table() -> HashMap<DataTypeName, Vec<FuncSign>> {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    //println!("{:#?}", funcs);
    funcs
}


fn init_agg_table() -> HashMap<DataTypeName, Vec<AggFuncSign>>{
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    build_agg_map()
}

fn build_agg_map() -> HashMap<DataTypeName, Vec<AggFuncSign>> {
    use {DataTypeName as T, AggKind as A};
    let mut map = HashMap::<DataTypeName, Vec<AggFuncSign>>::new();
    
    let all_types = [
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
        T::Timestampz,
        T::Time,
        T::Interval,
    ];

    for agg in [A::Sum, A::Min, A::Max, A::Count, A::Avg, A::StringAgg, A::SingleValue, A::ApproxCountDistinct] {
        for input in all_types{
            match AggCall::infer_return_type(&agg, [input] ){
                Ok(v) => map.entry(v.clone()).or_default().push(AggFuncSign::new(agg,vec![input],v)),
                Err(e)=> continue,
            }
        }
        
    }
    map
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
 
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName, agg: bool) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ, agg),
            };
        }
        
        let mut range = 99;
        if agg{
            range = 90;
        }
        

        match self.rng.gen_range(0..=range) {
            0..=90 => self.gen_func(typ),
            91..=99 => self.gen_agg(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            _ => unreachable!(),
        }
    }

    fn gen_col(&mut self, typ: DataTypeName, agg: bool) -> Expr {
        let mut columns =  &self.bound_columns; 
        if agg{
            columns = &self.bound_relations.choose(&mut self.rng).unwrap().columns;
        }

        if columns.is_empty() {
            return self.gen_simple_scalar(typ);
        }
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

    fn gen_func(&mut self, ret: DataTypeName) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func.inputs_type.iter().map(|t| self.gen_expr(*t, false)).collect();
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

    fn gen_agg(&mut self, ret: DataTypeName) -> Expr{
        let funcs = match AGG_FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let expr: Vec<Expr> = func.inputs_type.iter().map(|t| self.gen_expr(*t, true)).collect();
        assert!(expr.len()==1);
        
        make_agg_expr(func.func, expr[0], self.flip_coin()).unwrap_or_else(|| self.gen_simple_scalar(ret))
        


        //gen_expr(typ, true);

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
        E::Position => Some(Expr::Function(make_func("position", &exprs, false))),
        E::RoundDigit => Some(Expr::Function(make_func("round", &exprs, false))),
        E::Repeat => Some(Expr::Function(make_func("repeat", &exprs, false))),
        E::CharLength => Some(Expr::Function(make_func("char_length", &exprs, false))),
        E::Substr => Some(Expr::Function(make_func("substr", &exprs, false))),
        E::Length => Some(Expr::Function(make_func("length", &exprs, false))),
        E::Upper => Some(Expr::Function(make_func("upper", &exprs, false))),
        E::Lower => Some(Expr::Function(make_func("lower", &exprs, false))),
        E::Replace => Some(Expr::Function(make_func("replace", &exprs, false))),
        E::Md5 => Some(Expr::Function(make_func("md5", &exprs, false))),
        E::ToChar => Some(Expr::Function(make_func("to_char", &exprs, false))),
        E::Overlay => Some(make_overlay(exprs)),
        _ => None,
    }
}


fn make_agg_expr(func: AggKind, expr: Expr, distinct:bool) -> Option<Expr> {
    use AggKind as A;
    
    match func {
        A::Sum => Some(Expr::Function(make_func("sum", &[expr], distinct))),
        A::Min => Some(Expr::Function(make_func("min", &[expr], distinct))),
        A::Max => Some(Expr::Function(make_func("max", &[expr], distinct))),
        A::Count => Some(Expr::Function(make_func("count", &[expr], distinct))),
        A::Avg => Some(Expr::Function(make_func("avg", &[expr], distinct))),
        A::StringAgg => Some(Expr::Function(make_func("stringAgg", &[expr], distinct))),
        A::SingleValue => Some(Expr::Function(make_func("singleValue", &[expr], distinct))),
        A::ApproxCountDistinct => Some(Expr::Function(make_func("approxCountDistinct", &[expr], distinct))),
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

// DINSTINCT , ORDER BY or FILTER is allowed in aggregation functions, 
fn make_func(func_name: &str, exprs: &[Expr], distinct:bool) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct: distinct,
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
    func_sigs()
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
        .join("\n")
}
