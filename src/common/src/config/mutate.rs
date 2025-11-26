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

use anyhow::Context;
use itertools::Itertools as _;
use serde::Serialize;
use toml::{Table, Value};

def_anyhow_newtype! {
    pub ConfigMutateError,

    toml::ser::Error => transparent,
}

enum Op {
    Upsert(Value),
    Delete,
}

fn mutate(map: &mut Table, path: &str, op: Op) -> Result<(), ConfigMutateError> {
    let segments = path.split('.').collect_vec();
    let (key, segments) = segments.split_last().context("empty path")?;

    let mut map = map;
    for segment in segments {
        use toml::map::Entry;

        let sub_map = match map.entry(segment.to_owned()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => match &op {
                // Create new tables if not exists.
                Op::Upsert(_) => entry.insert(Value::Table(Table::new())),
                // Delete from a non-existing path is a no-op.
                Op::Delete => return Ok(()),
            },
        }
        .as_table_mut()
        .with_context(|| format!("expect a table at {segment}"))?;

        map = sub_map;
    }

    match op {
        Op::Upsert(value) => {
            map.insert(key.to_string(), value);
        }
        Op::Delete => {
            map.remove(*key);
        }
    }

    Ok(())
}

/// Extension trait for [`toml::Table`] to mutate values at a given dot-separated path.
#[easy_ext::ext(TomlTableMutateExt)]
impl Table {
    /// Upsert a value at the given dot-separated path.
    pub fn upsert(&mut self, path: &str, value: impl Serialize) -> Result<(), ConfigMutateError> {
        let value = Value::try_from(value)?;
        mutate(self, path, Op::Upsert(value))
    }

    /// Delete a value at the given path.
    pub fn delete(&mut self, path: &str) -> Result<(), ConfigMutateError> {
        mutate(self, path, Op::Delete)
    }
}
