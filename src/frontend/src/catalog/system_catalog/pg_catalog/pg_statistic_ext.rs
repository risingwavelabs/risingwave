// Copyright 2023 Singularity Data
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
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_statistic_ext` defines index access method operator classes.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-statistic_ext.html`].
pub const PG_STATISTIC_EXT_TABLE_NAME: &str = "pg_statistic_ext";
pub static PG_STATISTIC_EXT_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
    (DataType::Int32, "oid"),
    (DataType::Varchar, "stxrelid"),
    (DataType::Int32, "stxname"),
    (DataType::Int32, "stxnamespace"),
    (DataType::Int32, "stxowner"),
    (DataType::Int16, "stxstattarget"),
    (
        DataType::List {
            datatype: Box::new(DataType::Int32),
        },
        "stxkeys",
    ),
    (
        DataType::List {
            datatype: Box::new(DataType::Varchar),
        },
        "stxkind",
    ),
    (
        DataType::List {
            datatype: Box::new(DataType::Varchar),
        },
        "stxexprs",
    ),
]
});
