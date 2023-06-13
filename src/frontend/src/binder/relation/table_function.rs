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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{FunctionArg, TableAlias};

use super::{Binder, Relation, Result};
use crate::binder::statement::RewriteExprsRecursive;
use crate::expr::{ExprImpl, InputRef};

impl Binder {
    pub(super) fn bind_table_function(
        &self,
        name: ObjectName,
        alias: Option<TableAlias>,
        args: Vec<FunctionArg>,
    ) -> Result<Relation> {
        let func_name = &name.0[0].real_value();
        if func_name.eq_ignore_ascii_case(RW_INTERNAL_TABLE_FUNCTION_NAME) {
            return self.bind_internal_table(args, alias);
        }
        if func_name.eq_ignore_ascii_case(PG_GET_KEYWORDS_FUNC_NAME)
            || name.real_value().eq_ignore_ascii_case(
                format!("{}.{}", PG_CATALOG_SCHEMA_NAME, PG_GET_KEYWORDS_FUNC_NAME).as_str(),
            )
        {
            return self.bind_relation_by_name_inner(
                Some(PG_CATALOG_SCHEMA_NAME),
                PG_KEYWORDS_TABLE_NAME,
                alias,
                false,
            );
        }
        if let Ok(kind) = WindowTableFunctionKind::from_str(func_name) {
            return Ok(Relation::WindowTableFunction(Box::new(
                self.bind_window_table_function(alias, kind, args)?,
            )));
        }
        if is_watermark_func(func_name) {
            return Ok(Relation::Watermark(Box::new(
                self.bind_watermark(alias, args)?,
            )));
        };

        let args: Vec<ExprImpl> = args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        let tf = if let Some(func) = self
            .catalog
            .first_valid_schema(
                &self.db_name,
                &self.search_path,
                &self.auth_context.user_name,
            )?
            .get_function_by_name_args(
                func_name,
                &args.iter().map(|arg| arg.return_type()).collect_vec(),
            )
            && matches!(func.kind, FunctionKind::Table { .. })
        {
            TableFunction::new_user_defined(func.clone(), args)
        } else if let Ok(table_function_type) = TableFunctionType::from_str(func_name) {
            TableFunction::new(table_function_type, args)?
        } else {
            return Err(ErrorCode::NotImplemented(
                format!("unknown table function: {}", func_name),
                1191.into(),
            )
            .into());
        };
        let columns = if let DataType::Struct(s) = tf.return_type() {
            let schema = Schema::from(&s);
            schema.fields.into_iter().map(|f| (false, f)).collect_vec()
        } else {
            vec![(false, Field::with_name(tf.return_type(), tf.name()))]
        };

        self.bind_table_to_context(columns, tf.name(), alias)?;

        Ok(Relation::TableFunction(Box::new(tf)))
    }
}
