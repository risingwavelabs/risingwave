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

use std::future::Future;
use std::ops::DerefMut;

use async_trait::async_trait;
use futures::FutureExt;
use futures::future::BoxFuture;
use risingwave_pb::connector_service::SinkMetadata;

use super::SinkCommittedEpochSubscriber;
use crate::sink::log_store::{LogStoreReadItem, LogStoreResult, TruncateOffset};
use crate::sink::{LogSinker, SinkCommitCoordinator, SinkLogReader};

pub type BoxCoordinator = Box<dyn SinkCommitCoordinator + Send + 'static>;

pub type BoxLogSinker = Box<
    dyn for<'a> FnOnce(&'a mut dyn DynLogReader) -> BoxFuture<'a, crate::sink::Result<!>>
        + Send
        + 'static,
>;

#[async_trait]
pub trait DynLogReader: Send {
    async fn dyn_start_from(&mut self, start_offset: Option<u64>) -> LogStoreResult<()>;
    async fn dyn_next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)>;

    fn dyn_truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()>;
}

#[async_trait]
impl<R: SinkLogReader> DynLogReader for R {
    async fn dyn_start_from(&mut self, start_offset: Option<u64>) -> LogStoreResult<()> {
        R::start_from(self, start_offset).await
    }

    async fn dyn_next_item(&mut self) -> LogStoreResult<(u64, LogStoreReadItem)> {
        R::next_item(self).await
    }

    fn dyn_truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        R::truncate(self, offset)
    }
}

impl SinkLogReader for &mut dyn DynLogReader {
    fn start_from(
        &mut self,
        start_offset: Option<u64>,
    ) -> impl Future<Output = LogStoreResult<()>> + Send + '_ {
        (*self).dyn_start_from(start_offset)
    }

    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_ {
        (*self).dyn_next_item()
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        (*self).dyn_truncate(offset)
    }
}

pub fn boxed_log_sinker(log_sinker: impl LogSinker) -> BoxLogSinker {
    fn make_future<'a>(
        log_sinker: impl LogSinker,
        log_reader: &'a mut dyn DynLogReader,
    ) -> BoxFuture<'a, crate::sink::Result<!>> {
        log_sinker.consume_log_and_sink(log_reader).boxed()
    }

    // Note: it's magical that the following expression can be cast to the expected return type
    // without any explicit conversion, such as `<expr> as _` or `<expr>.into()`.
    // TODO: may investigate the reason. The currently successful compilation seems volatile to future compatibility.
    Box::new(move |log_reader: &mut dyn DynLogReader| make_future(log_sinker, log_reader))
}

#[async_trait]
impl LogSinker for BoxLogSinker {
    async fn consume_log_and_sink(
        self,
        mut log_reader: impl SinkLogReader,
    ) -> crate::sink::Result<!> {
        (self)(&mut log_reader).await
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
