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

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_rpc_client::ConnectorClient;

use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

#[derive(Clone, Debug)]
pub struct RedisConfig;

#[derive(Debug)]
pub struct RedisSink;

impl RedisSink {
    pub fn new(_cfg: RedisConfig, _schema: Schema) -> Result<Self> {
        todo!()
    }
}

#[async_trait]
impl Sink for RedisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = RedisSinkWriter;

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        todo!()
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        todo!()
    }
}

pub struct RedisSinkWriter;

#[async_trait]
impl SinkWriter for RedisSinkWriter {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        todo!();
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        todo!()
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Bitmap) -> Result<()> {
        todo!()
    }
}
