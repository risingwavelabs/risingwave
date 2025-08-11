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

use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{DataType, Ident, Statement};

use crate::handler::RwPgResponse;

#[expect(clippy::unused_async)]
pub async fn handle_prepare(
    _name: Ident,
    _data_types: Vec<DataType>,
    _statement: Box<Statement>,
) -> crate::error::Result<RwPgResponse> {
    const MESSAGE: &str = "\
        `PREPARE` is not supported yet.\n\
        For compatibility, this statement will still succeed but no changes are actually made.";

    Ok(RwPgResponse::builder(StatementType::PREPARE)
        .notice(MESSAGE)
        .into())
}

#[expect(clippy::unused_async)]
pub async fn handle_deallocate(
    _name: Option<Ident>,
    _prepare: bool,
) -> crate::error::Result<RwPgResponse> {
    const MESSAGE: &str = "\
        `DEALLOCATE` is not supported yet.\n\
        For compatibility, this statement will still succeed but no changes are actually made.";

    Ok(RwPgResponse::builder(StatementType::DEALLOCATE)
        .notice(MESSAGE)
        .into())
}
