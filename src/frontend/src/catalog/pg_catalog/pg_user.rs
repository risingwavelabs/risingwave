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

use risingwave_common::types::DataType;

use crate::catalog::pg_catalog::PgCatalogColumnsDef;

/// The catalog `pg_user` provides access to information about database users.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-user.html`]
pub const PG_USER_TABLE_NAME: &str = "pg_user";
pub const PG_USER_ID_INDEX: usize = 0;
pub const PG_USER_NAME_INDEX: usize = 1;

pub const PG_USER_COLUMNS: &[PgCatalogColumnsDef] = &[
    (DataType::Int32, "usesysid"),
    (DataType::Varchar, "name"),
    (DataType::Boolean, "usecreatedb"),
    (DataType::Boolean, "usesuper"),
    (DataType::Varchar, "passwd"),
];
