// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// TODO Add specific configs
//! Storage configuration for storage backends.
//!
//! This module provides configuration types for various storage backends.
//! The configuration types are designed to be used with the `StorageFactory`
//! trait to create storage instances.
//!
//! # Available Configurations
//!
//! - [`StorageConfig`]: Base configuration containing properties for storage backends
//! - [`S3Config`]: Amazon S3 specific configuration
//! - [`GcsConfig`]: Google Cloud Storage specific configuration
//! - [`OssConfig`]: Alibaba Cloud OSS specific configuration
//! - [`AzdlsConfig`]: Azure Data Lake Storage specific configuration

mod azdls;
mod gcs;
mod oss;
mod s3;

use std::collections::HashMap;

pub use azdls::*;
pub use gcs::*;
pub use oss::*;
pub use s3::*;
use serde::{Deserialize, Serialize};

/// Configuration properties for storage backends.
///
/// This struct contains only configuration properties without specifying
/// which storage backend to use. The storage type is determined by the
/// explicit factory selection.
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageConfig {
    /// Configuration properties for the storage backend
    props: HashMap<String, String>,
}

impl StorageConfig {
    /// Create a new empty StorageConfig.
    pub fn new() -> Self {
        Self {
            props: HashMap::new(),
        }
    }

    /// Create a StorageConfig from existing properties.
    ///
    /// # Arguments
    ///
    /// * `props` - Configuration properties for the storage backend
    pub fn from_props(props: HashMap<String, String>) -> Self {
        Self { props }
    }

    /// Get all configuration properties.
    pub fn props(&self) -> &HashMap<String, String> {
        &self.props
    }

    /// Get a specific configuration property by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to look up
    ///
    /// # Returns
    ///
    /// An `Option` containing a reference to the property value if it exists.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.props.get(key)
    }

    /// Add a configuration property.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key
    /// * `value` - The property value
    pub fn with_prop(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.props.insert(key.into(), value.into());
        self
    }

    /// Add multiple configuration properties.
    ///
    /// This is a builder-style method that returns `self` for chaining.
    ///
    /// # Arguments
    ///
    /// * `props` - An iterator of key-value pairs to add
    pub fn with_props(
        mut self,
        props: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        self.props
            .extend(props.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_config_new() {
        let config = StorageConfig::new();

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_from_props() {
        let props = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("bucket".to_string(), "my-bucket".to_string()),
        ]);
        let config = StorageConfig::from_props(props.clone());

        assert_eq!(config.props(), &props);
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_get() {
        let config = StorageConfig::new().with_prop("region", "us-east-1");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("nonexistent"), None);
    }

    #[test]
    fn test_storage_config_with_prop() {
        let config = StorageConfig::new()
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        assert_eq!(config.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(config.get("bucket"), Some(&"my-bucket".to_string()));
    }

    #[test]
    fn test_storage_config_with_props() {
        let additional_props = vec![("key1", "value1"), ("key2", "value2")];
        let config = StorageConfig::new().with_props(additional_props);

        assert_eq!(config.get("key1"), Some(&"value1".to_string()));
        assert_eq!(config.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_storage_config_clone() {
        let config = StorageConfig::new().with_prop("region", "us-east-1");
        let cloned = config.clone();

        assert_eq!(config, cloned);
        assert_eq!(cloned.get("region"), Some(&"us-east-1".to_string()));
    }

    #[test]
    fn test_storage_config_serialization_roundtrip() {
        let config = StorageConfig::new()
            .with_prop("region", "us-east-1")
            .with_prop("bucket", "my-bucket");

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
    }

    #[test]
    fn test_storage_config_clone_independence() {
        let original = StorageConfig::new().with_prop("region", "us-east-1");
        let mut cloned = original.clone();

        // Modify the clone
        cloned = cloned.with_prop("region", "eu-west-1");
        cloned = cloned.with_prop("new_key", "new_value");

        // Original should be unchanged
        assert_eq!(original.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(original.get("new_key"), None);

        // Clone should have the new values
        assert_eq!(cloned.get("region"), Some(&"eu-west-1".to_string()));
        assert_eq!(cloned.get("new_key"), Some(&"new_value".to_string()));
    }

    #[test]
    fn test_storage_config_from_props_empty() {
        let config = StorageConfig::from_props(HashMap::new());

        assert!(config.props().is_empty());
    }

    #[test]
    fn test_storage_config_serialization_empty() {
        let config = StorageConfig::new();

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: StorageConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config, deserialized);
        assert!(deserialized.props().is_empty());
    }
}
