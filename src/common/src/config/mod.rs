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

//! This module defines the structure of the configuration file `risingwave.toml`.
//!
//! [`RwConfig`] corresponds to the whole config file and each other config struct corresponds to a
//! section in `risingwave.toml`.

use std::fs;

use anyhow::Context;
use educe::Educe;
pub use risingwave_common_proc_macro::OverrideConfig;
use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};

// Re-export all configuration types
pub use batch::*;
pub use compaction::*;

pub use frontend::*;
pub use meta::*;
pub use object_store::*;
pub use server::*;
pub use storage::*;
pub use streaming::*;
pub use system::*;
pub use types::*;
pub use udf::*;

// Submodules
mod batch;
mod compaction;

mod frontend;
mod meta;
mod object_store;
mod server;
mod storage;
mod streaming;
mod system;
mod types;
mod udf;

pub fn load_config(path: &str, cli_override: impl OverrideConfig) -> RwConfig
where
{
    let mut config = if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        RwConfig::default()
    } else {
        let config_str = fs::read_to_string(path)
            .with_context(|| format!("failed to open config file at `{path}`"))
            .unwrap();
        toml::from_str(config_str.as_str())
            .context("failed to parse config file")
            .unwrap()
    };
    cli_override.r#override(&mut config);
    config
}

pub trait OverrideConfig {
    fn r#override(&self, config: &mut RwConfig);
}

impl<T: OverrideConfig> OverrideConfig for &T {
    fn r#override(&self, config: &mut RwConfig) {
        T::r#override(self, config)
    }
}

/// For non-user-facing components where the CLI arguments do not override the config file.
#[derive(Clone, Copy)]
pub struct NoOverride;

impl OverrideConfig for NoOverride {
    fn r#override(&self, _config: &mut RwConfig) {}
}

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Educe, Clone, Serialize, Deserialize, Default, ConfigDoc)]
#[educe(Debug)]
pub struct RwConfig {
    #[serde(default)]
    #[config_doc(nested)]
    pub server: ServerConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta: MetaConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub batch: BatchConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub frontend: FrontendConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub storage: StorageConfig,

    #[serde(default)]
    #[educe(Debug(ignore))]
    #[config_doc(nested)]
    pub system: SystemConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub udf: UdfConfig,

    #[serde(flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

impl RwConfig {
    pub const fn default_connection_pool_size(&self) -> u16 {
        self.server.connection_pool_size
    }

    /// Returns [`StreamingDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn streaming_exchange_connection_pool_size(&self) -> u16 {
        self.streaming
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }

    /// Returns [`BatchDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn batch_exchange_connection_pool_size(&self) -> u16 {
        self.batch
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn default_config_for_docs() -> RwConfig {
        // This is for generating a default template for documentation.
        RwConfig::default()
    }

    #[test]
    fn test_example_up_to_date() {
        let res = toml::to_string_pretty(&default_config_for_docs());
        match res {
            Ok(toml) => println!("{}", toml),
            Err(e) => println!("Failed to serialize config: {:?}", e),
        }
    }

    #[derive(Serialize, Deserialize)]
    struct ConfigItemDoc {
        desc: String,
        default: String,
    }

    fn rw_config_to_markdown() -> String {
        fn set_default_values(
            section: String,
            name: String,
            value: toml::Value,
            configs: &mut BTreeMap<String, BTreeMap<String, ConfigItemDoc>>,
        ) {
            let default_str = match value {
                toml::Value::String(s) => format!("\"{}\"", s),
                v => v.to_string(),
            };

            configs.entry(section).or_default().insert(
                name,
                ConfigItemDoc {
                    desc: "".to_owned(),
                    default: default_str,
                },
            );
        }

        let toml_value: toml::Value = toml::to_string(&default_config_for_docs())
            .unwrap()
            .parse()
            .unwrap();
        let mut configs = BTreeMap::new();

        if let toml::Value::Table(table) = toml_value {
            for (section, value) in table {
                if let toml::Value::Table(section_table) = value {
                    for (name, value) in section_table {
                        set_default_values(section.clone(), name, value, &mut configs);
                    }
                }
            }
        }

        let mut markdown = String::new();
        for (section, section_configs) in configs {
            markdown.push_str(&format!("## `{}`\n\n", section));
            for (name, config) in section_configs {
                markdown.push_str(&format!(
                    "- `{}`: {} (default: {})\n",
                    name, config.desc, config.default
                ));
            }
            markdown.push('\n');
        }

        markdown
    }

    #[test]
    fn test_object_store_configs_backward_compatibility() {
        use super::object_store::*;

        // Test deprecated fields in S3ObjectStoreConfig
        let toml_str = r#"
            retry_unknown_service_error = true
        "#;

        let config: S3ObjectStoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.retry_unknown_service_error, true);

        let toml_str = r#"
            object_store_keepalive_ms = 10000
            object_store_recv_buffer_size = 8192
            object_store_send_buffer_size = 4096
            object_store_nodelay = true
        "#;

        let config: S3ObjectStoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.keepalive_ms, Some(10000));
        assert_eq!(config.recv_buffer_size, Some(8192));
        assert_eq!(config.send_buffer_size, Some(4096));
        assert_eq!(config.nodelay, Some(true));

        // Test deprecated fields in S3ObjectStoreDeveloperConfig
        let toml_str = r#"
            object_store_retry_unknown_service_error = false
            object_store_retryable_service_error_codes = ["SlowDown", "TooManyRequests"]
        "#;

        let config: S3ObjectStoreDeveloperConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.retry_unknown_service_error, false);
        assert_eq!(
            config.retryable_service_error_codes,
            vec!["SlowDown", "TooManyRequests"]
        );

        // Test deprecated fields in ObjectStoreConfig
        let toml_str = r#"
            object_store_set_atomic_write_dir = true
        "#;

        let config: ObjectStoreConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.set_atomic_write_dir, true);
    }

    #[test]
    fn test_meta_configs_backward_compatibility() {
        use super::meta::*;

        // Test deprecated fields in MetaConfig
        let toml_str = r#"
            table_write_throughput_threshold = 16777216
            min_table_split_write_throughput = 4194304
            periodic_split_compact_group_interval_sec = 10
        "#;

        let config: MetaConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.table_high_write_throughput_threshold, 16777216);
        assert_eq!(config.table_low_write_throughput_threshold, 4194304);
        assert_eq!(
            config.periodic_scheduling_compaction_group_split_interval_sec,
            10
        );
    }
}