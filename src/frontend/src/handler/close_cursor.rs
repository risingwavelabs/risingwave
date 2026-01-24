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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_sqlparser::ast::CloseCursorStatement;

use super::RwPgResponse;
use crate::error::Result;
use crate::handler::HandlerArgs;

pub async fn handle_close_cursor(
    handle_args: HandlerArgs,
    stmt: CloseCursorStatement,
) -> Result<RwPgResponse> {
    let session = handle_args.session.clone();
    let cursor_manager = session.get_cursor_manager();
    if let Some(cursor_name) = stmt.cursor_name {
        cursor_manager
            .remove_cursor(&cursor_name.real_value())
            .await?;
    } else {
        cursor_manager.remove_all_cursor().await;
    }
    Ok(PgResponse::empty_result(StatementType::CLOSE_CURSOR))
}
