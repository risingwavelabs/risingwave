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

//! The source and sink connectors this build supports, with the `WITH` options each takes, read
//! from the generated `with_options_source.yaml` and `with_options_sink.yaml`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sink::Sink;
// `for_all_sinks!` names the filesystem sink unqualified.
use crate::sink::file_sink::fs::FsSink;
use crate::source::SourceProperties;

const SOURCE_OPTIONS_YAML: &str = include_str!("../with_options_source.yaml");
const SINK_OPTIONS_YAML: &str = include_str!("../with_options_sink.yaml");

/// One `WITH` option, as `./risedev generate-with-options` records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorOption {
    pub name: String,
    pub field_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comments: String,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alias: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectorInfo {
    /// The value of `connector = '...'`.
    pub name: &'static str,
    /// `None` when the connector's properties are not described by `#[derive(WithOptions)]`.
    pub options: Option<Vec<ConnectorOption>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Connectors {
    pub sources: Vec<ConnectorInfo>,
    pub sinks: Vec<ConnectorInfo>,
}

#[derive(Deserialize)]
struct StructOptions {
    fields: Vec<ConnectorOption>,
}

macro_rules! define_dispatched_sources {
    ({ $({ $variant:ident, $prop:ty, $split:ty }),* $(,)? }) => {
        /// `(connector name, properties type name)` for every source the build dispatches.
        fn dispatched_sources() -> Vec<(&'static str, &'static str)> {
            vec![$((<$prop as SourceProperties>::SOURCE_NAME, std::any::type_name::<$prop>())),*]
        }
    };
}

macro_rules! define_dispatched_sinks {
    ({ $({ $variant:ident, $sink:ty, $config:ty }),* $(,)? }) => {
        /// `(connector name, config type name)` for every sink the build dispatches.
        fn dispatched_sinks() -> Vec<(&'static str, &'static str)> {
            vec![$((<$sink as Sink>::SINK_NAME, std::any::type_name::<$config>())),*]
        }
    };
}

crate::for_all_sources! { define_dispatched_sources }
crate::for_all_sinks! { define_dispatched_sinks }

/// Every source and sink connector, sorted by name.
pub fn connectors() -> Connectors {
    Connectors {
        sources: describe(dispatched_sources(), SOURCE_OPTIONS_YAML),
        sinks: describe(dispatched_sinks(), SINK_OPTIONS_YAML),
    }
}

fn describe(entries: Vec<(&'static str, &'static str)>, yaml: &str) -> Vec<ConnectorInfo> {
    let options: BTreeMap<String, StructOptions> =
        serde_yaml::from_str(yaml).expect("the generated with_options yaml parses");
    let mut connectors: Vec<ConnectorInfo> = entries
        .into_iter()
        .map(|(name, type_name)| ConnectorInfo {
            name,
            options: options
                .get(struct_name(type_name))
                .map(|found| found.fields.clone()),
        })
        .collect();
    connectors.sort_by_key(|connector| connector.name);
    connectors
}

/// `a::b::KafkaConfig` → `KafkaConfig`; `a::CdcProperties<a::Mysql>` → `CdcProperties`.
fn struct_name(type_name: &str) -> &str {
    let path = type_name.split('<').next().unwrap_or(type_name);
    path.rsplit("::").next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn find<'a>(list: &'a [ConnectorInfo], name: &str) -> &'a ConnectorInfo {
        list.iter()
            .find(|connector| connector.name == name)
            .unwrap_or_else(|| panic!("connector {name} is listed"))
    }

    #[test]
    fn test_every_dispatched_connector_is_listed() {
        let listed = connectors();
        for (name, _) in dispatched_sinks() {
            find(&listed.sinks, name);
        }
        for (name, _) in dispatched_sources() {
            find(&listed.sources, name);
        }
    }

    #[test]
    fn test_options_come_from_the_generated_yaml() {
        let listed = connectors();
        let kafka = find(&listed.sinks, "kafka").options.as_ref().unwrap();
        assert!(
            kafka
                .iter()
                .any(|option| option.name == "properties.bootstrap.server" && option.required)
        );
        assert!(find(&listed.sources, "nats").options.is_some());
    }

    #[test]
    fn test_a_connector_without_with_options_has_none() {
        assert_eq!(find(&connectors().sinks, "jdbc").options, None);
    }

    #[test]
    fn test_json_has_sources_and_sinks() {
        let json = serde_json::to_value(connectors()).unwrap();
        assert!(
            json["sources"]
                .as_array()
                .is_some_and(|list| !list.is_empty())
        );
        assert!(
            json["sinks"]
                .as_array()
                .is_some_and(|list| !list.is_empty())
        );
    }

    #[test]
    fn test_struct_name() {
        assert_eq!(struct_name("a::b::KafkaConfig"), "KafkaConfig");
        assert_eq!(struct_name("a::CdcProperties<a::Mysql>"), "CdcProperties");
        assert_eq!(struct_name("()"), "()");
    }
}
