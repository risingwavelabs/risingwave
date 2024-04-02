// Copyright 2024 RisingWave Labs
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

use std::collections::VecDeque;
use std::sync::Arc;

use futures::StreamExt;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::StatementType;
use pgwire::types::Row;
use risingwave_sqlparser::ast::ObjectName;

use super::PgResponseStream;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::query::{create_stream, BatchPlanFragmenterResult};
use crate::session::SessionImpl;

impl SessionImpl {
    pub async fn add_cursor(&self, cursor_name: ObjectName, cursor: QueryCursor) -> Result<()> {
        if self
            .cursors
            .lock()
            .await
            .try_insert(cursor_name.clone(), cursor)
            .is_err()
        {
            return Err(ErrorCode::CatalogError(
                format!("cursor \"{cursor_name}\" already exists").into(),
            )
            .into());
        }
        Ok(())
    }

    pub async fn drop_all_cursors(&self) {
        self.cursors.lock().await.clear();
    }

    pub async fn drop_cursor(&self, cursor_name: ObjectName) -> Result<()> {
        match self.cursors.lock().await.remove(&cursor_name) {
            Some(_) => Ok(()),
            None => Err(ErrorCode::CatalogError(
                format!("cursor \"{cursor_name}\" does not exist").into(),
            )
            .into()),
        }
    }

    pub async fn cursor_next(
        &self,
        cursor_name: &ObjectName,
        count: Option<i32>,
    ) -> Result<(Vec<Row>, Vec<PgFieldDescriptor>)> {
        if let Some(cursor) = self.cursors.lock().await.get_mut(cursor_name) {
            Ok((cursor.next(count).await?, cursor.pg_descs()))
        } else {
            Err(
                ErrorCode::CatalogError(format!("cursor \"{cursor_name}\" does not exist").into())
                    .into(),
            )
        }
    }
}
