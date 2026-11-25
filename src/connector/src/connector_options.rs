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

//! The source and sink connectors a user can create with this build, with the `WITH` options each
//! takes, read from the generated `with_options_source.yaml` and `with_options_sink.yaml`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sink::Sink;
// `for_all_sinks!` names the filesystem sink unqualified.
use crate::sink::file_sink::fs::FsSink;
use crate::sink::test_sink::TEST_SINK_NAME;
use crate::sink::trivial::TABLE_SINK;
use crate::source::test_source::TEST_CONNECTOR;
use crate::source::{LEGACY_S3_CONNECTOR, OPENDAL_S3_CONNECTOR, SourceProperties};

const SOURCE_OPTIONS_YAML: &str = include_str!("../with_options_source.yaml");
const SINK_OPTIONS_YAML: &str = include_str!("../with_options_sink.yaml");

/// Dispatched names no `connector = '...'` creates: the test connectors, and `table`, which only
/// `CREATE SINK INTO` creates.
const INTERNAL_SOURCES: [&str; 1] = [TEST_CONNECTOR];
const INTERNAL_SINKS: [&str; 2] = [TEST_SINK_NAME, TABLE_SINK];

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
    /// Whether `ALTER ... CONNECTOR WITH` can change it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_alter_on_fly: bool,
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

/// A connector the build dispatches. `enabled` is false for the stand-in compiled when its
/// feature is disabled, which only returns errors.
struct Dispatched {
    name: &'static str,
    options_type: &'static str,
    enabled: bool,
}

macro_rules! define_dispatched_sources {
    ({ $({ $variant:ident, $prop:ty, $split:ty }),* $(,)? }) => {
        fn dispatched_sources() -> Vec<Dispatched> {
            vec![$(Dispatched {
                name: <$prop as SourceProperties>::SOURCE_NAME,
                options_type: std::any::type_name::<$prop>(),
                enabled: <$prop as SourceProperties>::ENABLED,
            }),*]
        }
    };
}

macro_rules! define_dispatched_sinks {
    ({ $({ $variant:ident, $sink:ty, $config:ty }),* $(,)? }) => {
        fn dispatched_sinks() -> Vec<Dispatched> {
            vec![$(Dispatched {
                name: <$sink as Sink>::SINK_NAME,
                options_type: std::any::type_name::<$config>(),
                enabled: <$sink as Sink>::ENABLED,
            }),*]
        }
    };
}

crate::for_all_sources! { define_dispatched_sources }
crate::for_all_sinks! { define_dispatched_sinks }

/// Every source and sink connector a user can create with this build, sorted by name.
pub fn connectors() -> Connectors {
    Connectors {
        sources: describe(
            s3_as_written(dispatched_sources()),
            &INTERNAL_SOURCES,
            SOURCE_OPTIONS_YAML,
        ),
        sinks: describe(dispatched_sinks(), &INTERNAL_SINKS, SINK_OPTIONS_YAML),
    }
}

/// The frontend rejects `s3_v2` and rewrites `s3` to it, so `s3` is listed with `s3_v2`'s options.
fn s3_as_written(mut sources: Vec<Dispatched>) -> Vec<Dispatched> {
    sources.retain(|source| source.name != LEGACY_S3_CONNECTOR);
    for source in &mut sources {
        if source.name == OPENDAL_S3_CONNECTOR {
            source.name = LEGACY_S3_CONNECTOR;
        }
    }
    sources
}

fn describe(entries: Vec<Dispatched>, internal: &[&str], yaml: &str) -> Vec<ConnectorInfo> {
    let options: BTreeMap<String, StructOptions> =
        serde_yaml::from_str(yaml).expect("the generated with_options yaml parses");
    let mut connectors: Vec<ConnectorInfo> = entries
        .into_iter()
        .filter(|entry| entry.enabled && !internal.contains(&entry.name))
        .map(|entry| ConnectorInfo {
            name: entry.name,
            options: options
                .get(struct_name(entry.options_type))
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

    fn names(list: &[ConnectorInfo]) -> Vec<&'static str> {
        list.iter().map(|connector| connector.name).collect()
    }

    fn without_options(list: &[ConnectorInfo]) -> Vec<&'static str> {
        list.iter()
            .filter(|connector| connector.options.is_none())
            .map(|connector| connector.name)
            .collect()
    }

    /// Fails if an options struct stops matching its yaml entry, e.g. after a rename.
    #[test]
    fn test_only_connectors_without_with_options_have_none() {
        let listed = connectors();
        // CDC properties are passed through to Debezium.
        assert_eq!(
            without_options(&listed.sources),
            [
                "mariadb-cdc",
                "mongodb-cdc",
                "mysql-cdc",
                "oracle-cdc",
                "postgres-cdc",
                "sqlserver-cdc"
            ]
        );
        // The generator records `S3Config` only inside `SnowflakeConfig`, which flattens it.
        assert_eq!(
            without_options(&listed.sinks),
            ["blackhole", "cassandra", "jdbc", "s3"]
        );
    }

    #[test]
    fn test_no_option_is_named_by_a_rust_raw_identifier() {
        let listed = connectors();
        let names: Vec<&str> = listed
            .sources
            .iter()
            .chain(&listed.sinks)
            .flat_map(|connector| connector.options.iter().flatten())
            .map(|option| option.name.as_str())
            .collect();
        assert!(names.contains(&"type"));
        assert!(
            names.iter().all(|name| !name.starts_with("r#")),
            "{names:?}"
        );
    }

    #[test]
    fn test_json_shape() {
        let json = serde_json::to_value(connectors()).unwrap();
        let kafka = json["sinks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|sink| sink["name"] == "kafka")
            .unwrap();
        let option = |name: &str| {
            kafka["options"]
                .as_array()
                .unwrap()
                .iter()
                .find(|option| option["name"] == name)
                .unwrap()
                .clone()
        };
        assert_eq!(
            option("topic"),
            serde_json::json!({
                "name": "topic",
                "field_type": "String",
                "required": true,
                "alias": ["kafka.topic"],
            })
        );
        assert_eq!(
            option("properties.sync.call.timeout"),
            serde_json::json!({
                "name": "properties.sync.call.timeout",
                "field_type": "Duration",
                "required": false,
                "default": "Duration :: from_secs (5)",
                "allow_alter_on_fly": true,
            })
        );
        assert_eq!(
            option("properties.ssl.ca.location")["comments"],
            "Path to CA certificate file for verifying the broker's key."
        );
        assert!(json["sources"].is_array());
    }

    #[test]
    fn test_struct_name() {
        assert_eq!(struct_name("a::b::KafkaConfig"), "KafkaConfig");
        assert_eq!(struct_name("a::CdcProperties<a::Mysql>"), "CdcProperties");
        assert_eq!(struct_name("()"), "()");
    }

    #[test]
    fn test_s3_is_listed_once_under_its_written_name_with_the_s3_v2_options() {
        let sources = connectors().sources;
        let names = names(&sources);
        assert_eq!(names.iter().filter(|name| **name == "s3").count(), 1);
        assert!(!names.contains(&"s3_v2"));
        let yaml: BTreeMap<String, StructOptions> =
            serde_yaml::from_str(SOURCE_OPTIONS_YAML).unwrap();
        assert_eq!(
            find(&sources, "s3").options.as_ref(),
            Some(&yaml["OpendalS3Properties"].fields)
        );
    }

    #[test]
    fn test_internal_connectors_are_dispatched_but_not_listed() {
        let dispatched_sinks: Vec<&str> = dispatched_sinks().iter().map(|s| s.name).collect();
        let dispatched_sources: Vec<&str> = dispatched_sources().iter().map(|s| s.name).collect();
        let listed = connectors();
        for sink in ["test", "table"] {
            assert!(dispatched_sinks.contains(&sink));
            assert!(!names(&listed.sinks).contains(&sink));
        }
        assert!(dispatched_sources.contains(&"test"));
        assert!(!names(&listed.sources).contains(&"test"));
    }

    struct Disabled;

    impl crate::sink::utils::dummy::FeatureNotEnabledSinkMarker for Disabled {
        const SINK_NAME: &'static str = "disabled";
    }

    #[test]
    fn test_a_disabled_feature_stand_in_is_not_listed() {
        const { assert!(!<crate::sink::utils::dummy::FeatureNotEnabledSink<Disabled> as Sink>::ENABLED) };
        let listed = describe(
            vec![Dispatched {
                name: "disabled",
                options_type: "a::DisabledConfig",
                enabled: false,
            }],
            &[],
            SINK_OPTIONS_YAML,
        );
        assert!(listed.is_empty());
    }
}
