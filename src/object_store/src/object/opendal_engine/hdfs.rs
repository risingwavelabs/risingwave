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

use opendal::services::Hdfs;
use opendal::Operator;

use super::{EngineType, OpendalObjectStore};
use crate::object::ObjectResult;
impl OpendalObjectStore {
    /// create opendal hdfs engine.
    pub fn new_hdfs_engine(namenode: String, root: String) -> ObjectResult<Self> {
        // Create hdfs backend builder.
        let mut builder = Hdfs::default();
        // Set the name node for hdfs.
        builder.name_node(&namenode);
        // Set the root for hdfs, all operations will happen under this root.
        // NOTE: the root must be absolute path.
        builder.root(&root);

        let op: Operator = Operator::create(builder)?.finish();
        Ok(Self {
            op,
            engine_type: EngineType::Hdfs,
        })
    }
}
