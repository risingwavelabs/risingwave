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

use std::ops::DerefMut;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_pb::connector_service::SinkMetadata;

use super::SinkCommittedEpochSubscriber;
use crate::sink::{SinkCommitCoordinator, SinkWriter};

pub type BoxWriter<CM> = Box<dyn SinkWriter<CommitMetadata = CM> + Send + 'static>;
pub type BoxCoordinator = Box<dyn SinkCommitCoordinator + Send + 'static>;

#[async_trait]
impl<CM: 'static + Send> SinkWriter for BoxWriter<CM> {
    type CommitMetadata = CM;

    async fn begin_epoch(&mut self, epoch: u64) -> crate::sink::Result<()> {
        self.deref_mut().begin_epoch(epoch).await
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> crate::sink::Result<()> {
        self.deref_mut().write_batch(chunk).await
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> crate::sink::Result<CM> {
        self.deref_mut().barrier(is_checkpoint).await
    }

    async fn abort(&mut self) -> crate::sink::Result<()> {
        self.deref_mut().abort().await
    }

    async fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) -> crate::sink::Result<()> {
        self.deref_mut().update_vnode_bitmap(vnode_bitmap).await
    }
}

#[async_trait]
impl SinkCommitCoordinator for BoxCoordinator {
    async fn init(
        &mut self,
        subscriber: SinkCommittedEpochSubscriber,
    ) -> crate::sink::Result<Option<u64>> {
        self.deref_mut().init(subscriber).await
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> crate::sink::Result<()> {
        self.deref_mut().commit(epoch, metadata).await
    }
}
