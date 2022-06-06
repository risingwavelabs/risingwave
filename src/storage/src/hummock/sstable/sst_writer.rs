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

use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{CachePolicy, HummockResult, Sstable};

pub struct SstWriter {
    sstable_store: SstableStoreRef,
}

impl SstWriter {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        Self { sstable_store }
    }

    pub fn write(
        &self,
        sst: Sstable,
        data: Bytes,
        notifier: tokio::sync::oneshot::Sender<HummockResult<Sstable>>,
    ) {
        let sstable_store = self.sstable_store.clone();
        tokio::spawn(async move {
            let result = sstable_store
                .put(sst.clone(), data, CachePolicy::Fill)
                .await;
            if notifier.send(result.map(|_| sst)).is_err() {
                tracing::warn!("receiver has already been deallocated");
            }
        });
    }
}
