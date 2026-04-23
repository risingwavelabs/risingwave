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

use std::collections::HashMap;
use std::sync::OnceLock;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg_catalog_rest::REST_CATALOG_PROP_URI;
use iceberg_test_utils::{get_minio_endpoint, get_rest_catalog_endpoint, set_up};

/// Global test fixture that uses environment-based configuration.
/// This assumes Docker containers are started externally (e.g., via `make docker-up`).
pub struct GlobalTestFixture {
    pub catalog_config: HashMap<String, String>,
}

static GLOBAL_FIXTURE: OnceLock<GlobalTestFixture> = OnceLock::new();

impl GlobalTestFixture {
    /// Creates a new GlobalTestFixture from environment variables.
    /// Uses default localhost endpoints if environment variables are not set.
    pub fn from_env() -> Self {
        set_up();

        let rest_endpoint = get_rest_catalog_endpoint();
        let minio_endpoint = get_minio_endpoint();

        let catalog_config = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), rest_endpoint),
            (S3_ENDPOINT.to_string(), minio_endpoint),
            (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ]);

        GlobalTestFixture { catalog_config }
    }
}

/// Returns a reference to the global test fixture.
/// This fixture assumes Docker containers are started externally.
pub fn get_test_fixture() -> &'static GlobalTestFixture {
    GLOBAL_FIXTURE.get_or_init(GlobalTestFixture::from_env)
}
