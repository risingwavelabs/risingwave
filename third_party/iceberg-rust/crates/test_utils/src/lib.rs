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

//! This crate contains common utilities for testing.
//!
//! It's not intended for use outside of `iceberg-rust`.

#[cfg(feature = "tests")]
pub use common::*;

#[cfg(feature = "tests")]
mod common {
    use std::sync::Once;

    use iceberg::{Catalog, NamespaceIdent};

    static INIT: Once = Once::new();
    pub fn set_up() {
        INIT.call_once(tracing_subscriber::fmt::init);
    }
    pub fn normalize_test_name(s: impl ToString) -> String {
        s.to_string().replace("::", "__").replace('.', "_")
    }

    // Environment variable names for service endpoints
    pub const ENV_MINIO_ENDPOINT: &str = "ICEBERG_TEST_MINIO_ENDPOINT";
    pub const ENV_REST_CATALOG_ENDPOINT: &str = "ICEBERG_TEST_REST_ENDPOINT";
    pub const ENV_HMS_ENDPOINT: &str = "ICEBERG_TEST_HMS_ENDPOINT";
    pub const ENV_GLUE_ENDPOINT: &str = "ICEBERG_TEST_GLUE_ENDPOINT";
    pub const ENV_GCS_ENDPOINT: &str = "ICEBERG_TEST_GCS_ENDPOINT";

    // Default ports matching dev/docker-compose.yaml
    pub const DEFAULT_MINIO_PORT: u16 = 9000;
    pub const DEFAULT_REST_CATALOG_PORT: u16 = 8181;
    pub const DEFAULT_HMS_PORT: u16 = 9083;
    pub const DEFAULT_GLUE_PORT: u16 = 5001;
    pub const DEFAULT_GCS_PORT: u16 = 4443;

    /// Returns the MinIO S3-compatible endpoint.
    /// Checks ICEBERG_TEST_MINIO_ENDPOINT env var, otherwise returns localhost default.
    pub fn get_minio_endpoint() -> String {
        std::env::var(ENV_MINIO_ENDPOINT)
            .unwrap_or_else(|_| format!("http://localhost:{DEFAULT_MINIO_PORT}"))
    }

    /// Returns the REST catalog endpoint.
    /// Checks ICEBERG_TEST_REST_ENDPOINT env var, otherwise returns localhost default.
    pub fn get_rest_catalog_endpoint() -> String {
        std::env::var(ENV_REST_CATALOG_ENDPOINT)
            .unwrap_or_else(|_| format!("http://localhost:{DEFAULT_REST_CATALOG_PORT}"))
    }

    /// Returns the HMS (Hive Metastore) endpoint.
    /// Checks ICEBERG_TEST_HMS_ENDPOINT env var, otherwise returns localhost default.
    pub fn get_hms_endpoint() -> String {
        std::env::var(ENV_HMS_ENDPOINT).unwrap_or_else(|_| format!("localhost:{DEFAULT_HMS_PORT}"))
    }

    /// Returns the Glue (Moto mock) endpoint.
    /// Checks ICEBERG_TEST_GLUE_ENDPOINT env var, otherwise returns localhost default.
    pub fn get_glue_endpoint() -> String {
        std::env::var(ENV_GLUE_ENDPOINT)
            .unwrap_or_else(|_| format!("http://localhost:{DEFAULT_GLUE_PORT}"))
    }

    /// Returns the GCS (fake-gcs-server) endpoint.
    /// Checks ICEBERG_TEST_GCS_ENDPOINT env var, otherwise returns localhost default.
    pub fn get_gcs_endpoint() -> String {
        std::env::var(ENV_GCS_ENDPOINT)
            .unwrap_or_else(|_| format!("http://localhost:{DEFAULT_GCS_PORT}"))
    }

    /// Helper to clean up a namespace and its tables before a test runs.
    /// This handles the case where previous test runs left data in the persistent database.
    pub async fn cleanup_namespace<C: Catalog>(catalog: &C, ns: &NamespaceIdent) {
        // Try to drop all tables in the namespace first
        if let Ok(tables) = catalog.list_tables(ns).await {
            for table in tables {
                let _ = catalog.drop_table(&table).await;
            }
        }
        // Then try to drop the namespace itself
        let _ = catalog.drop_namespace(ns).await;
    }

    /// Macro to generate a normalized test name with module path prefix.
    /// Takes one or more string parts and joins them with the module path.
    ///
    /// Example:
    /// ```ignore
    /// // Returns something like "rest_catalog_test__test_create_table"
    /// let name = normalize_test_name_with_parts!("test_create_table");
    ///
    /// // Returns something like "rest_catalog_test__apple__ios"
    /// let name = normalize_test_name_with_parts!("apple", "ios");
    /// ```
    #[macro_export]
    macro_rules! normalize_test_name_with_parts {
        ($($part:expr),+) => {
            $crate::normalize_test_name([module_path!(), $($part),+].join("_"))
        };
    }
}
