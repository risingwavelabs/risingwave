// Copyright 2026 RisingWave Labs
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
//! - Allow users to override a config entry that's already set to `Some` back to `None` in
//!   per-job configuration via `ALTER .. SET CONFIG`
//!
//! Note that using this utility on a `String` should be carefully considered, as it will
//! confuse explicit empty string and `None`.

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

    // Collect as `serde_content::Value` first to check if it is empty string.
    let v = Value::deserialize(deserializer)?;

    if let Value::String(s) = &v
        && s.is_empty()
    {
        return Ok(None);
    }

    // If it's not empty string, deserialize it again as `T`.
    let t = Deserializer::new(v)
        .human_readable()
        .coerce_numbers()
        .deserialize()
        .map_err(D::Error::custom)?;

    Ok(Some(t))
}

#[cfg(test)]
mod tests {
    use expect_test::expect;
    use serde_default::DefaultFromSerde;

    use super::*;

    fn default_b() -> Option<usize> {
        Some(42)
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, DefaultFromSerde)]
    struct Config {
        #[serde(with = "super")]
        #[serde(default)]
        a: Option<usize>,

        #[serde(with = "super")]
        #[serde(default = "default_b")]
        b: Option<usize>,
    }

    #[test]
    fn test_basic() {
        let config = Config::default();
        let toml = toml::to_string(&config).unwrap();
        expect![[r#"
            a = ""
            b = 42
        "#]]
        .assert_eq(&toml);

        let config2: Config = toml::from_str(&toml).unwrap();
        assert_eq!(config2, config);
    }
}
