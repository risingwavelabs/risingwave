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

use std::sync::Arc;

use opendal::Operator;
use opendal::layers::LoggingLayer;
use opendal::services::Hdfs;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::jvm_runtime::JVM;

use super::{MediaType, OpendalObjectStore};
// use crate::object::opendal_engine::ATOMIC_WRITE_DIR;
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal hdfs engine.
    pub fn new_hdfs_engine(
        namenode: String,
        root: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Init the jvm explicitly to avoid duplicate JVM creation by hdfs client
        let _ = JVM
            .get_or_init()
            .inspect_err(|e| tracing::error!("Failed to init JVM: {:?}", e))
            .unwrap();

        // Create hdfs backend builder.
        let mut builder = Hdfs::default();
        // Set the name node for hdfs.
        builder.name_node(&namenode);
        builder.root(&root);
        // todo: reopen the following lines after https://github.com/apache/opendal/issues/4867 is resolved.
        // if config.set_atomic_write_dir {
        //     let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
        //     builder.atomic_write_dir(&atomic_write_dir);
        // }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(Self {
            op,
            media_type: MediaType::Hdfs,
            config,
            metrics,
        })
    }
}
