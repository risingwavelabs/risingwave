// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use opendal::Operator;
use opendal::layers::LoggingLayer;
use opendal::services::Fs;
use risingwave_common::config::ObjectStoreConfig;

use super::{MediaType, OpendalObjectStore, new_operator};
use crate::object::ObjectResult;
use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::opendal_engine::ATOMIC_WRITE_DIR;

impl OpendalObjectStore {
    /// create opendal fs engine.
    pub fn new_fs_engine(
        root: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Create fs backend builder.
        let mut builder = Fs::default().root(&root);
        if config.set_atomic_write_dir {
            let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
            builder = builder.atomic_write_dir(&atomic_write_dir);
        }

        let op = new_operator(
            &config,
            Operator::new(builder)?.layer(LoggingLayer::default()),
        );

        Ok(Self {
            op,
            media_type: MediaType::Fs,
            config,
            metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use prometheus::IntCounter;
    use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

    use super::*;

    #[test]
    fn test_fs_initialization_preserves_external_opendal_metrics() {
        // Iceberg installs OpenDAL metrics independently into this same registry.
        let external =
            IntCounter::new("opendal_operation_bytes", "external OpenDAL metric").unwrap();
        GLOBAL_METRICS_REGISTRY
            .register(Box::new(external.clone()))
            .unwrap();
        external.inc();
        for _ in 0..2 {
            let dir = tempfile::tempdir().unwrap();
            OpendalObjectStore::new_fs_engine(
                dir.path().to_str().unwrap().to_owned(),
                Arc::new(ObjectStoreConfig::default()),
                Arc::new(ObjectStoreMetrics::unused()),
            )
            .unwrap();
        }
        assert_eq!(external.get(), 1);
        GLOBAL_METRICS_REGISTRY
            .unregister(Box::new(external))
            .unwrap();
    }
}
