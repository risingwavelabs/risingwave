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

/// The catalog `pg_proc` defines index access method operator classes.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-proc.html`].
/// [`DataType`] Compatibility Mapping (PG -> RW)
/// `oid` -> `Int32`
/// `Float4` -> `Float32`
/// `Char` -> `Varchar`
/// `text` -> `Varchar`
/// `pg_node_tree` -> `Varchar[]`
/// `aclitem` -> `Varchar` Or should this be a bitmap?
pub const PG_PROC_TABLE_NAME: &str = "pg_proc";
pub static PG_PROC_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "oid"),
        (DataType::Varchar, "proname"),
        (DataType::Int32, "pronamespace"),
        (DataType::Int32, "proowner"),
        (DataType::Int32, "prolang"),
        (DataType::Float32, "procost"),
        (DataType::Float32, "prorows"),
        (DataType::Int32, "provariadic"),
        (DataType::Int32, "prosupport"),
        (DataType::Varchar, "prokind"),
        (DataType::Boolean, "prosecdef"),
        (DataType::Boolean, "proleakproof"),
        (DataType::Boolean, "proisstrict"),
        (DataType::Boolean, "proretset"),
        (DataType::Varchar, "provolatile"),
        (DataType::Varchar, "proparallel"),
        (DataType::Int32, "pronargs"),
        (DataType::Int32, "pronargdefaults"),
        (DataType::Int32, "prorettype"),
        (
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
            "proargtypes",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
            "proallargtypes",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "proargmodes",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "proargnames",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "proargdefaults",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
            "protrftypes",
        ),
        (DataType::Varchar, "prosrc"),
        (DataType::Varchar, "probin"),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "prosqlbody",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "proconfig",
        ),
        (
            DataType::List {
                datatype: Box::new(DataType::Varchar),
            },
            "proacl",
        ),
    ]
});
