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

use std::ops::{Deref, DerefMut};

use opendal::layers::RetryLayer;
use opendal::services::Fs;
use opendal::Operator;
use thiserror_ext::AsReport;

use crate::error::Result;

const RW_BATCH_SPILL_DIR_ENV: &str = "RW_BATCH_SPILL_DIR";
pub const DEFAULT_SPILL_PARTITION_NUM: usize = 20;
const DEFAULT_SPILL_DIR: &str = "/tmp/";
const RW_MANAGED_SPILL_DIR: &str = "/rw_batch_spill/";
const DEFAULT_IO_BUFFER_SIZE: usize = 64 * 1024;

/// `SpillOp` is used to manage the spill directory of the spilling executor and it will drop the directory with a RAII style.
pub struct SpillOp {
    pub op: Operator,
}

impl SpillOp {
    pub fn create(path: String) -> Result<SpillOp> {
        assert!(path.ends_with('/'));

        let spill_dir =
            std::env::var(RW_BATCH_SPILL_DIR_ENV).unwrap_or_else(|_| DEFAULT_SPILL_DIR.to_string());
        let root = format!("/{}/{}/{}/", spill_dir, RW_MANAGED_SPILL_DIR, path);

        let mut builder = Fs::default();
        builder.root(&root);

        let op: Operator = Operator::new(builder)?
            .layer(RetryLayer::default())
            .finish();
        Ok(SpillOp { op })
    }

    pub async fn writer_with(&self, name: &str) -> Result<opendal::Writer> {
        Ok(self
            .op
            .writer_with(name)
            .append(true)
            .buffer(DEFAULT_IO_BUFFER_SIZE)
            .await?)
    }

    pub async fn reader_with(&self, name: &str) -> Result<opendal::Reader> {
        Ok(self
            .op
            .reader_with(name)
            .buffer(DEFAULT_IO_BUFFER_SIZE)
            .await?)
    }
}

impl Drop for SpillOp {
    fn drop(&mut self) {
        let op = self.op.clone();
        tokio::task::spawn(async move {
            let result = op.remove_all("/").await;
            if let Err(error) = result {
                error!(
                    error = %error.as_report(),
                    "Failed to remove spill directory"
                );
            }
        });
    }
}

impl DerefMut for SpillOp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.op
    }
}

impl Deref for SpillOp {
    type Target = Operator;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}
