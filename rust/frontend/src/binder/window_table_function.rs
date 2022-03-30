use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::error::ErrorCode;
use risingwave_sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, ObjectName};

use super::{Binder, BoundBaseTable, Result};
use crate::expr::ExprImpl;

#[derive(Copy, Clone, Debug)]
pub enum WindowTableFunctionKind {
    Tumble,
    Hop,
}

impl FromStr for WindowTableFunctionKind {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "tumble" => Ok(WindowTableFunctionKind::Tumble),
            "hop" => Ok(WindowTableFunctionKind::Hop),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct BoundWindowTableFunction {
    pub(crate) input: BoundBaseTable,
    pub(crate) kind: WindowTableFunctionKind,
    pub(crate) args: Vec<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_window_table_function(
        &mut self,
        kind: WindowTableFunctionKind,
        args: Vec<FunctionArg>,
    ) -> Result<BoundWindowTableFunction> {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = args.get(0) {
            let input = match expr {
                Expr::Identifier(ident) => self.bind_table(ObjectName(vec![ident.clone()])),
                Expr::CompoundIdentifier(idents) => self.bind_table(ObjectName(idents.clone())),
                _ => Err(ErrorCode::BindError(
                    "the 1st arg of window table function should be table".to_string(),
                )
                .into()),
            }?;
            let exprs: Vec<_> = args
                .into_iter()
                .skip(1)
                .map(|arg| self.bind_function_arg(arg))
                .flatten_ok()
                .try_collect()?;
            Ok(BoundWindowTableFunction {
                input,
                kind,
                args: exprs,
            })
        } else {
            Err(ErrorCode::BindError(
                "the 1st arg of window table function should be table".to_string(),
            )
            .into())
        }
    }
}
