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
use opendal::services::Webhdfs;
use risingwave_common::config::ObjectStoreConfig;

use super::{MediaType, OpendalObjectStore};
use crate::object::ObjectResult;
use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::opendal_engine::ATOMIC_WRITE_DIR;

impl OpendalObjectStore {
    /// create opendal webhdfs engine.
    pub fn new_webhdfs_engine(
        endpoint: String,
        root: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Create webhdfs backend builder.
        let mut builder = Webhdfs::default();
        // Set the name node for webhdfs.
        builder = builder.endpoint(&endpoint);
        // Set the root for hdfs, all operations will happen under this root.
        // NOTE: the root must be absolute path.
        builder = builder.root(&root);

        let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
        builder = builder.atomic_write_dir(&atomic_write_dir);
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();
        Ok(Self {
            op,
            media_type: MediaType::Webhdfs,
            config,
            metrics,
        })
    }
}
