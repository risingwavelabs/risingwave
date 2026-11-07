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

use std::path::PathBuf;
use std::sync::LazyLock;

use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::services::Fs;
use risingwave_common::util::batch_spill_config::batch_spill_base_dir;
use tokio::sync::Mutex;

const RW_MANAGED_SPILL_DIR: &str = "rw_batch_spill/";

#[derive(Clone)]
pub enum SpillBackend {
    Disk,
    /// Only for testing purpose
    Memory,
}

/// The root of the spill directories of all executors on this node.
pub struct SpillOp;

impl SpillOp {
    pub(super) fn batch_spill_root() -> PathBuf {
        batch_spill_base_dir().join(RW_MANAGED_SPILL_DIR)
    }

    /// Removes everything under the root, left by a previous run of this node.
    pub async fn clean_spill_directory() -> opendal::Result<()> {
        static LOCK: LazyLock<Mutex<usize>> = LazyLock::new(|| Mutex::new(0));
        let _guard = LOCK.lock().await;

        let root = Self::batch_spill_root();

        let builder = Fs::default().root(&root.to_string_lossy());

        let op: Operator = Operator::new(builder)?.layer(RetryLayer::default());

        op.delete_with("/").recursive(true).await
    }
}
