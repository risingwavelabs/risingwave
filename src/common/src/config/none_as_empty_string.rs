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

//! Serialize and deserialize `Option<T>` as empty string if it is `None`. This is useful to:
//!
//! - Demonstrate config entries whose default values are `None` in `example.toml`
//! - Allow users to reset config entries to default by explicitly overriding them to empty string
//!   in `ALTER .. SET CONFIG`.
//!
//! Note that using this utility on a `String` may confuse explicit empty string and `None`.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S, T>(value: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    match value {
        Some(t) => t.serialize(serializer),
        None => serializer.serialize_str(""),
    }
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    use serde_content::{Deserializer, Value};
    let v = Value::deserialize(deserializer)?;

    if let Value::String(s) = &v
        && s.is_empty()
    {
        return Ok(None);
    }

    let t = Deserializer::new(v)
        .deserialize()
        .map_err(D::Error::custom)?;

    Ok(Some(t))
}
