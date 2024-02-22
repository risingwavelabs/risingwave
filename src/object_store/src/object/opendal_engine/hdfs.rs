// Copyright 2024 RisingWave Labs
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

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Hdfs;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::opendal_engine::ATOMIC_WRITE_DIR;
use crate::object::ObjectResult;

impl OpendalObjectStore {
    /// create opendal hdfs engine.
    pub fn new_hdfs_engine(namenode: String, root: String) -> ObjectResult<Self> {
        // Create hdfs backend builder.
        let mut builder = Hdfs::default();
        // Set the name node for hdfs.
        builder.name_node(&namenode);
        builder.root(&root);
        let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
        builder.atomic_write_dir(&atomic_write_dir);
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Hdfs,
        })
    }
}
