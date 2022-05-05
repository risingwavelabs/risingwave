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

use std::collections::HashMap;
use std::str::FromStr;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

const UPSTREAM_SOURCE_KEY: &str = "connector";

#[derive(Clone, Debug)]
pub struct Properties(pub HashMap<String, String>);

impl Properties {
    pub fn new(map: HashMap<String, String>) -> Self {
        Properties(map)
    }

    fn get_inner(&self, key: &str, context: &str) -> Result<String> {
        self.0
            .get(key)
            .ok_or_else(|| {
                RwError::from(ProtocolError(format!(
                    "Must specify property \"{}\" in WITH clause{}",
                    key, context
                )))
            })
            .map(|s| s.to_string())
    }

    /// Returns error if no such property.
    pub fn get(&self, key: &str) -> Result<String> {
        self.get_inner(key, "")
    }

    /// It's an alternative of `get` but returns kafka-specifc error hints.
    pub fn get_kafka(&self, key: &str) -> Result<String> {
        self.get_inner(key, " when using Kafka source")
    }

    /// It's an alternative of `get` but returns kinesis-specifc error hints.
    pub fn get_kinesis(&self, key: &str) -> Result<String> {
        self.get_inner(key, " when using Kinesis source")
    }

    /// It's an alternative of `get` but returns nexmark-specifc error hints.
    pub fn get_nexmark(&self, key: &str) -> Result<String> {
        self.get_inner(key, " when using Nexmark source")
    }

    pub fn get_connector_type(&self) -> Result<String> {
        self.get_inner(UPSTREAM_SOURCE_KEY, "when get connector type")
    }

    /// Returns the value for the given key automatically parsed, or a default
    /// value if the key does not exist.
    pub fn get_as_or<T: FromStr + std::fmt::Display>(&self, key: &str, default: T) -> Result<T> {
        let value = self
            .get_inner(key, "")
            .unwrap_or_else(|_| default.to_string());
        value.parse::<T>().map_err(|_| {
            RwError::from(ProtocolError(format!(
                "Invalid value \"{}\" of key \"{}\"",
                value, key
            )))
        })
    }

    /// Returns the value for the given key or a default value if the key does
    /// not exist.
    pub fn get_or(&self, key: &str, default: &str) -> String {
        self.get_inner(key, "")
            .unwrap_or_else(|_| default.to_string())
    }
}

/// [`AnyhowProperties`] returns [`anyhow::Result`] if key is not found.
#[derive(Clone)]
pub struct AnyhowProperties(pub HashMap<String, String>);

impl AnyhowProperties {
    pub fn new(map: HashMap<String, String>) -> Self {
        AnyhowProperties(map)
    }

    fn get_inner(&self, key: &str, context: &str) -> anyhow::Result<String> {
        self.0
            .get(key)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Must specify property \"{}\" in WITH clause{}",
                    key,
                    context
                )
            })
            .map(|s| s.to_string())
    }

    /// Returns error if no such property.
    pub fn get(&self, key: &str) -> anyhow::Result<String> {
        self.get_inner(key, "")
    }

    /// It's an alternative of `get` but returns pulsar-specifc error hints.
    pub fn get_pulsar(&self, key: &str) -> anyhow::Result<String> {
        self.get_inner(key, " when using Pulsar source")
    }

    /// It's an alternative of `get` but returns kafka-specifc error hints.
    pub fn get_kafka(&self, key: &str) -> anyhow::Result<String> {
        self.get_inner(key, " when using Kafka source")
    }

    /// It's an alternative of `get` but returns nexmark-specifc error hints.
    pub fn get_nexmark(&self, key: &str) -> anyhow::Result<String> {
        self.get_inner(key, " when using Nexmark source")
    }

    pub fn get_connector_type(&self) -> anyhow::Result<String> {
        self.get_inner(UPSTREAM_SOURCE_KEY, "when get connector type")
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use crate::{AnyhowProperties, Properties};

    #[test]
    fn test_properties() {
        let props = Properties::new(hashmap! {
            "a".to_string() => "b".to_string(),
        });
        assert_eq!(props.get_kafka("a").unwrap(), "b".to_string());
        assert_eq!(
            props.get_kafka("1").unwrap_err().to_string(),
            "protocol error: Must specify property \"1\" in WITH clause when using Kafka source"
        );
    }

    #[test]
    fn test_anyhow_properties() {
        let props = AnyhowProperties::new(hashmap! {
            "a".to_string() => "b".to_string(),
        });
        assert_eq!(props.get_kafka("a").unwrap(), "b".to_string());
        assert_eq!(
            props.get_kafka("1").unwrap_err().to_string(),
            "Must specify property \"1\" in WITH clause when using Kafka source"
        );
    }
}
