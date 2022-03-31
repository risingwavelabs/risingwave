use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
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
        if s.eq_ignore_ascii_case("tumble") {
            Ok(WindowTableFunctionKind::Tumble)
        } else if s.eq_ignore_ascii_case("hop") {
            Ok(WindowTableFunctionKind::Hop)
        } else {
            Err(())
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
        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = args.get(0) else {
            return Err(ErrorCode::BindError(
                "the 1st arg of window table function should be table".to_string(),
            )
            .into());
        };
        let table_name = match expr {
            Expr::Identifier(ident) => Ok::<_, RwError>(ObjectName(vec![ident.clone()])),
            Expr::CompoundIdentifier(idents) => Ok(ObjectName(idents.clone())),
            _ => Err(ErrorCode::BindError(
                "the 1st arg of window table function should be table".to_string(),
            )
            .into()),
        }?;
        let (schema_name, table_name) = Self::resolve_table_name(table_name)?;
        let table_catalog =
            self.catalog
                .get_table_by_name(&self.db_name, &schema_name, &table_name)?;

        let table_id = table_catalog.id();
        let table_desc = table_catalog.table_desc();
        let columns = table_catalog.columns().to_vec();
        if columns.iter().any(|col| {
            col.name().eq_ignore_ascii_case("window_start")
                || col.name().eq_ignore_ascii_case("window_end")
        }) {
            return Err(ErrorCode::BindError(
                "column names `window_start` and `window_end` are not allowed in window table function's input."
                .into())
            .into());
        }

        let columns = columns
            .iter()
            .map(|c| {
                (
                    c.column_desc.name.clone(),
                    c.column_desc.data_type.clone(),
                    c.is_hidden,
                )
            })
            .chain(
                [
                    ("window_start".to_string(), DataType::Timestamp, false),
                    ("window_end".to_string(), DataType::Timestamp, false),
                ]
                .into_iter(),
            );
        self.bind_context(columns, table_name.clone())?;

        let base = BoundBaseTable {
            name: table_name,
            table_desc,
            table_id,
        };
        let exprs: Vec<_> = args
            .into_iter()
            .skip(1)
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        Ok(BoundWindowTableFunction {
            input: base,
            kind,
            args: exprs,
        })
    }
}
