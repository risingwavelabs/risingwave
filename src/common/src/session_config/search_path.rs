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

use std::convert::Infallible;
use std::str::FromStr;

use super::SESSION_CONFIG_LIST_SEP;
use crate::catalog::{DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME, RW_CATALOG_SCHEMA_NAME};

pub const USER_NAME_WILD_CARD: &str = "\"$user\"";

/// see <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
///
/// 1. when we `select` or `drop` object and don't give a specified schema, it will search the
///    object from the valid items in schema `rw_catalog`, `pg_catalog` and `search_path`. If schema
///    `rw_catalog` and `pg_catalog` are not in `search_path`, we will search them firstly. If they're
///    in `search_path`, we will follow the order in `search_path`.
///
/// 2. when we `create` a `source` or `mv` and don't give a specified schema, it will use the first
///    valid schema in `search_path`.
///
/// 3. when we `create` a `index` or `sink`, it will use the schema of the associated table.
#[derive(Clone, Debug, PartialEq)]
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
            .join(SESSION_CONFIG_LIST_SEP)
            .parse()
            .unwrap()
    }
}

impl FromStr for SearchPath {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let paths = s.split(SESSION_CONFIG_LIST_SEP).map(|path| path.trim());
        let mut real_path = vec![];
        for p in paths {
            let p = p.trim();
            if !p.is_empty() {
                real_path.push(p.to_owned());
            }
        }
        let string = real_path.join(SESSION_CONFIG_LIST_SEP);

        let mut path = real_path.clone();
        let rw_catalog = RW_CATALOG_SCHEMA_NAME.to_owned();
        if !real_path.contains(&rw_catalog) {
            path.insert(0, rw_catalog);
        }

        let pg_catalog = PG_CATALOG_SCHEMA_NAME.to_owned();
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
