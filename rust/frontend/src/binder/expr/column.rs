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
//
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Ident;

use crate::binder::Binder;
use crate::expr::{ExprImpl, InputRef};

impl Binder {
    pub fn bind_column(&mut self, idents: &[Ident]) -> Result<ExprImpl> {
        // TODO: check quote style of `ident`.
        let (_schema_name, table_name, column_name) = match idents {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => {
                return Err(
                    ErrorCode::InternalError(format!("Too many idents: {:?}", idents)).into(),
                )
            }
        };
        let index = match table_name {
            Some(table_name) => self
                .context
                .get_index_with_table_name(column_name, table_name)?,
            None => self.context.get_index(column_name)?,
        };
        let column = &self.context.columns[index];
        Ok(InputRef::new(column.index, column.data_type.clone()).into())
    }
}
