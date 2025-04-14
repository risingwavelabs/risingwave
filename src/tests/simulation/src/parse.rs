use anyhow::Result;
use risingwave_common::bail;
use risingwave_sqlparser::ast::{
    CreateSinkStatement, Ident, SetVariableValue, SetVariableValueSingle, Statement, Value,
};
use risingwave_sqlparser::parser::Parser;

use crate::slt::SqlCmd;

fn ensure_one_item<T>(items: Vec<T>) -> Result<T> {
    if items.len() != 1 {
        bail!("expected a single item");
    }

    Ok(items.into_iter().next().unwrap())
}

pub fn extract_sql_command(sql: &str) -> Result<crate::slt::SqlCmd> {
    let statements = Parser::parse_sql(sql)?;
    let statement = ensure_one_item(statements)?;
    let cmd = match statement {
        Statement::CreateView {
            materialized: true,
            name,
            ..
        } => {
            let base_name = name.base_name();
            SqlCmd::CreateMaterializedView { name: base_name }
        }
        Statement::CreateTable { query, .. } => SqlCmd::Create {
            is_create_table_as: query.is_some(),
        },
        Statement::CreateSink {
            stmt: CreateSinkStatement {
                into_table_name, ..
            },
        } => SqlCmd::CreateSink {
            is_sink_into_table: into_table_name.is_some(),
        },
        Statement::SetVariable {
            variable, value, ..
        } if variable.real_value() == Ident::new_unchecked("background_ddl").real_value() => {
            let enable = match value {
                SetVariableValue::Single(SetVariableValueSingle::Literal(Value::Boolean(e))) => e,
                _ => bail!("incorrect value for background_ddl"),
            };
            SqlCmd::SetBackgroundDdl { enable }
        }
        Statement::Drop(_) => SqlCmd::Drop,
        Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
            SqlCmd::Dml
        }
        Statement::Flush => SqlCmd::Flush,
        stmt if stmt.is_create() => SqlCmd::Create {
            is_create_table_as: false,
        },
        _ => SqlCmd::Others,
    };
    Ok(cmd)
}
