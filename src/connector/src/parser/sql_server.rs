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

use std::sync::LazyLock;

use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ScalarImpl;
use thiserror_ext::AsReport;
use tiberius::Row;

use crate::parser::util::log_error;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

pub fn sql_server_row_to_owned_row(row: &mut Row, schema: &Schema) -> OwnedRow {
    let mut datums: Vec<Option<ScalarImpl>> = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = match row.try_get::<ScalarImpl, usize>(i) {
            Ok(datum) => datum,
            Err(err) => {
                log_error!(name, err, "parse column failed");
                None
            }
        };
        datums.push(datum);
    }
    println!(
        "WKXLOG sql_server_row_to_owned_row schema: {:?}, datums: {:?}",
        schema, datums
    );
    OwnedRow::new(datums)
}
