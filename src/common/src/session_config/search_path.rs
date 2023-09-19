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

use super::{ConfigEntry, CONFIG_KEYS, SEARCH_PATH};
use crate::catalog::{DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME, RW_CATALOG_SCHEMA_NAME};
use crate::error::RwError;

pub const USER_NAME_WILD_CARD: &str = "\"$user\"";

/// see <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
///
/// 1. when we `select` or `drop` object and don't give a specified schema, it will search the
/// object from the valid items in schema `rw_catalog`, `pg_catalog` and `search_path`. If schema
/// `rw_catalog` and `pg_catalog` are not in `search_path`, we will search them firstly. If they're
/// in `search_path`, we will follow the order in `search_path`.
///
/// 2. when we `create` a `source` or `mv` and don't give a specified schema, it will use the first
/// valid schema in `search_path`.
///
/// 3. when we `create` a `index` or `sink`, it will use the schema of the associated table.
#[derive(Clone)]
pub struct SearchPath {
    origin_str: String,
    /// The path will implicitly includes `rw_catalog` and `pg_catalog` if user does specify them.
    path: Vec<String>,
    real_path: Vec<String>,
}

impl SearchPath {
    pub fn real_path(&self) -> &[String] {
        &self.real_path
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }
}

impl Default for SearchPath {
    fn default() -> Self {
        [USER_NAME_WILD_CARD, DEFAULT_SCHEMA_NAME]
            .as_slice()
            .try_into()
            .unwrap()
    }
}

impl ConfigEntry for SearchPath {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[SEARCH_PATH]
    }
}

impl TryFrom<&[&str]> for SearchPath {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        let mut real_path = vec![];
        for p in value {
            let p = p.trim();
            if !p.is_empty() {
                real_path.push(p.to_string());
            }
        }
        let string = real_path.join(", ");

        let mut path = real_path.clone();
        let rw_catalog = RW_CATALOG_SCHEMA_NAME.to_string();
        if !real_path.contains(&rw_catalog) {
            path.insert(0, rw_catalog);
        }

        let pg_catalog = PG_CATALOG_SCHEMA_NAME.to_string();
        if !real_path.contains(&pg_catalog) {
            path.insert(0, pg_catalog);
        }

        Ok(Self {
            origin_str: string,
            path,
            real_path,
        })
    }
}

impl std::fmt::Display for SearchPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.origin_str)
    }
}
