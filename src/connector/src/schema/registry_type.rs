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

use super::{InvalidOptionError, invalid_option_error};
use crate::Get;

pub const SCHEMA_REGISTRY_TYPE_KEY: &str = "schema.registry.type";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SchemaRegistryType {
    #[default]
    Confluent,
    Pulsar,
}

impl SchemaRegistryType {
    pub fn from_options(options: &impl Get) -> Result<Self, InvalidOptionError> {
        let Some(value) = options.get(SCHEMA_REGISTRY_TYPE_KEY) else {
            return Ok(Self::default());
        };

        if value.eq_ignore_ascii_case("confluent") {
            Ok(Self::Confluent)
        } else if value.eq_ignore_ascii_case("pulsar") {
            Ok(Self::Pulsar)
        } else {
            Err(invalid_option_error!(
                "unsupported `{SCHEMA_REGISTRY_TYPE_KEY}` value `{value}`; expected `confluent` or `pulsar`"
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn schema_registry_type_defaults_to_confluent() {
        assert_eq!(
            SchemaRegistryType::from_options(&BTreeMap::new()).unwrap(),
            SchemaRegistryType::Confluent
        );
    }

    #[test]
    fn schema_registry_type_is_case_insensitive() {
        let options = BTreeMap::from([(SCHEMA_REGISTRY_TYPE_KEY.to_owned(), "PULSAR".to_owned())]);
        assert_eq!(
            SchemaRegistryType::from_options(&options).unwrap(),
            SchemaRegistryType::Pulsar
        );
    }

    #[test]
    fn schema_registry_type_rejects_unknown_value() {
        let options = BTreeMap::from([(SCHEMA_REGISTRY_TYPE_KEY.to_owned(), "unknown".to_owned())]);
        assert!(SchemaRegistryType::from_options(&options).is_err());
    }
}
