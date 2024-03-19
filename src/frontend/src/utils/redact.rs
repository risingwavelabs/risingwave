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

use anyhow::Context;
use risingwave_sqlparser::ast::REDACT_SQL_OPTION;

use crate::error::Result;

pub fn redact_definition(definition: &str) -> Result<String> {
    let [stmt]: [_; 1] = risingwave_sqlparser::parser::Parser::parse_sql(definition)
        .context("unable to parse definition")?
        .try_into()
        .unwrap();
    Ok(REDACT_SQL_OPTION.sync_scope(true, || stmt.to_string()))
}
