// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;

use crate::object::{BlockLocation, ObjectMetadata, ObjectResult, ObjectStore};

pub struct LocalDiskObjectStore {
    _path_prefix: String,
    compactor_shutdown_sender: parking_lot::Mutex<Option<UnboundedSender<()>>>,
}

impl LocalDiskObjectStore {
    pub fn new(path_prefix: &str) -> LocalDiskObjectStore {
        LocalDiskObjectStore {
            _path_prefix: path_prefix.to_string(),
            compactor_shutdown_sender: parking_lot::Mutex::new(None),
        }
    }

    pub fn set_compactor_shutdown_sender(&self, shutdown_sender: UnboundedSender<()>) {
        *self.compactor_shutdown_sender.lock() = Some(shutdown_sender);
    }
}

impl Drop for LocalDiskObjectStore {
    fn drop(&mut self) {
        if let Some(sender) = self.compactor_shutdown_sender.lock().take() {
            let _ = sender.send(());
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocalDiskObjectStore {
    async fn upload(&self, _path: &str, _obj: Bytes) -> ObjectResult<()> {
        todo!()
    }

    async fn read(&self, _path: &str, _block_loc: Option<BlockLocation>) -> ObjectResult<Bytes> {
        todo!()
    }

    async fn readv(
        &self,
        _path: &str,
        _block_locs: Vec<BlockLocation>,
    ) -> ObjectResult<Vec<Bytes>> {
        todo!()
    }

    async fn metadata(&self, _path: &str) -> ObjectResult<ObjectMetadata> {
        todo!()
    }

    async fn delete(&self, _path: &str) -> ObjectResult<()> {
        todo!()
    }
}
