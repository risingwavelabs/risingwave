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

// THIS FILE IS AUTO_GENERATED. DO NOT EDIT
// UPDATE WITH: ./risedev generate-with-options

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use crate::error::ConnectorError;

macro_rules! use_source_properties {
    ({ $({ $variant_name:ident, $prop_name:ty, $split:ty }),* }) => {
        $(
            #[allow(unused_imports)]
            pub(super) use $prop_name;
        )*
    };
}

mod source_properties {
    use crate::for_all_sources;
    use crate::source::base::SourceProperties;

    for_all_sources!(use_source_properties);

    /// Implements a function that maps a source name string to the Rust type name of the corresponding property type.
    /// Usage: `impl_source_name_to_prop_type_name!();` will generate:
    /// ```ignore
    /// pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str>
    /// ```
    macro_rules! impl_source_name_to_prop_type_name_inner {
        ({ $({$variant:ident, $prop_name:ty, $split:ty}),* }) => {
            pub fn source_name_to_prop_type_name(source_name: &str) -> Option<&'static str> {
                match source_name {
                    $(
                        <$prop_name>::SOURCE_NAME => Some(std::any::type_name::<$prop_name>()),
                    )*
                    _ => None,
                }
            }
        };
    }

    macro_rules! impl_source_name_to_prop_type_name {
        () => {
            $crate::for_all_sources! { impl_source_name_to_prop_type_name_inner }
        };
    }

    impl_source_name_to_prop_type_name!();
}

mod sink_properties {
    use crate::use_all_sink_configs;
    use crate::sink::Sink;
    use crate::sink::file_sink::fs::FsSink;

    use_all_sink_configs!();

    macro_rules! impl_sink_name_to_config_type_name_inner {
        ({ $({ $variant_name:ident, $sink_type:ty, $config_type:ty }),* }) => {
            pub fn sink_name_to_config_type_name(sink_name: &str) -> Option<&'static str> {
                match sink_name {
                $(
                    <$sink_type>::SINK_NAME => Some(std::any::type_name::<$config_type>()),
                )*
                    _ => None,
                }
            }
        };
    }

    macro_rules! impl_sink_name_to_config_type_name {
        () => {
            $crate::for_all_sinks! { impl_sink_name_to_config_type_name_inner }
        };
    }

    impl_sink_name_to_config_type_name!();
}

/// Map of source connector names to their `allow_alter_on_fly` field names
pub static SOURCE_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use source_properties::*;
    let mut map = HashMap::new();
    // KafkaProperties
    map.try_insert(
        std::any::type_name::<KafkaProperties>().to_owned(),
        [
            "properties.sync.call.timeout".to_owned(),
            "properties.security.protocol".to_owned(),
            "properties.ssl.endpoint.identification.algorithm".to_owned(),
            "properties.sasl.mechanism".to_owned(),
            "properties.sasl.username".to_owned(),
            "properties.sasl.password".to_owned(),
            "properties.message.max.bytes".to_owned(),
            "properties.receive.message.max.bytes".to_owned(),
            "properties.statistics.interval.ms".to_owned(),
            "properties.client.id".to_owned(),
            "properties.enable.ssl.certificate.verification".to_owned(),
            "properties.queued.min.messages".to_owned(),
            "properties.queued.max.messages.kbytes".to_owned(),
            "properties.fetch.wait.max.ms".to_owned(),
            "properties.fetch.queue.backoff.ms".to_owned(),
            "properties.fetch.max.bytes".to_owned(),
            "properties.enable.auto.commit".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Map of sink connector names to their `allow_alter_on_fly` field names
pub static SINK_ALLOW_ALTER_ON_FLY_FIELDS: LazyLock<HashMap<String, HashSet<String>>> = LazyLock::new(|| {
    use sink_properties::*;
    let mut map = HashMap::new();
    // KafkaConfig
    map.try_insert(
        std::any::type_name::<KafkaConfig>().to_owned(),
        [
            "properties.sync.call.timeout".to_owned(),
            "properties.security.protocol".to_owned(),
            "properties.ssl.endpoint.identification.algorithm".to_owned(),
            "properties.sasl.mechanism".to_owned(),
            "properties.sasl.username".to_owned(),
            "properties.sasl.password".to_owned(),
            "properties.message.max.bytes".to_owned(),
            "properties.receive.message.max.bytes".to_owned(),
            "properties.statistics.interval.ms".to_owned(),
            "properties.client.id".to_owned(),
            "properties.enable.ssl.certificate.verification".to_owned(),
        ].into_iter().collect(),
    ).unwrap();
    map
});

/// Get all source connector names that have `allow_alter_on_fly` fields
pub fn get_source_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {
    SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Get all sink connector names that have `allow_alter_on_fly` fields
pub fn get_sink_connectors_with_allow_alter_on_fly_fields() -> Vec<&'static str> {
    SINK_ALLOW_ALTER_ON_FLY_FIELDS.keys().map(|s| s.as_str()).collect()
}

/// Checks if all given fields are allowed to be altered on the fly for the specified source connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_source_allow_alter_on_fly_fields(
    connector_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {
    // Convert connector name to the type name key
    let Some(type_name) = source_properties::source_name_to_prop_type_name(connector_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown source connector: {connector_name}"
        )));
    };
    let Some(allowed_fields) = SOURCE_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for connector: {connector_name}"
        )));
    };
    for field in fields {
        if !allowed_fields.contains(field) {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{field}' is not allowed to be altered on the fly for connector: {connector_name}"
            )));
        }
    }
    Ok(())
}

/// Checks if all given fields are allowed to be altered on the fly for the specified sink connector.
/// Returns Ok(()) if all fields are allowed, otherwise returns a `ConnectorError`.
pub fn check_sink_allow_alter_on_fly_fields(
    sink_name: &str,
    fields: &[String],
) -> crate::error::ConnectorResult<()> {
    // Convert sink name to the type name key
    let Some(type_name) = sink_properties::sink_name_to_config_type_name(sink_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Unknown sink connector: {sink_name}"
        )));
    };
    let Some(allowed_fields) = SINK_ALLOW_ALTER_ON_FLY_FIELDS.get(type_name) else {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "No allow_alter_on_fly fields registered for sink: {sink_name}"
        )));
    };
    for field in fields {
        if !allowed_fields.contains(field) {
            return Err(ConnectorError::from(anyhow::anyhow!(
                "Field '{field}' is not allowed to be altered on the fly for sink: {sink_name}"
            )));
        }
    }
    Ok(())
}

