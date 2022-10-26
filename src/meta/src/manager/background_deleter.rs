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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::SourceId;
use crate::storage::MetaStore;
use crate::stream::{GlobalStreamManagerRef, SourceManagerRef};
use crate::MetaResult;

pub type StreamingJobBackgroundDeleterRef = Arc<StreamingJobBackgroundDeleter>;

#[derive(Debug)]
// TODO(zehua): just use `TableId` instead of `StreamingJobId` after we remove source manager.
pub enum StreamingJobId {
    Table(TableId),
    Sink(TableId),
    Source(SourceId),
}

/// Used to delete actor, fragment, source and so on.
/// When we drop a streaming job in a frontend, meta will drop it from meta store and notify
/// frontends that object is dropped. The other things in compute node or storage related to the
/// object will be drop in `StreamingJobBackgroundDeleter` in the background thread asynchronously.
pub struct StreamingJobBackgroundDeleter(mpsc::UnboundedSender<Vec<StreamingJobId>>);

impl StreamingJobBackgroundDeleter {
    pub async fn new<S: MetaStore>(
        stream_manager: GlobalStreamManagerRef<S>,
        source_manager: SourceManagerRef<S>,
    ) -> MetaResult<(Self, JoinHandle<()>, Sender<()>)> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<StreamingJobId>>();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let join_handle = tokio::spawn(async move {
            loop {
                let streaming_job_ids = tokio::select! {
                    _ = &mut shutdown_rx => {
                        tracing::info!("Table background deleter is stopped");
                        return;
                    }
                    streaming_job_ids = rx.recv() => {
                        streaming_job_ids
                    }
                };

                if let Some(ids) = streaming_job_ids {
                    Self::handle_streaming_job_ids(ids, &stream_manager, &source_manager)
                        .await
                        .ok();
                } else {
                    tracing::info!("Channel is closed");
                    break;
                }
            }
        });

        let background_deleter = Self(tx);

        Ok((background_deleter, join_handle, shutdown_tx))
    }

    pub fn delete(&self, streaming_job_ids: Vec<StreamingJobId>) {
        self.0.send(streaming_job_ids).unwrap()
    }

    async fn handle_streaming_job_ids<S: MetaStore>(
        ids: Vec<StreamingJobId>,
        stream_manager: &GlobalStreamManagerRef<S>,
        source_manager: &SourceManagerRef<S>,
    ) -> MetaResult<()> {
        assert!(!ids.is_empty());

        let (source_ids, table_ids): (Vec<_>, Vec<_>) = ids.iter().partition_map(|id| match id {
            StreamingJobId::Source(id) => either::Either::Left(id),
            StreamingJobId::Table(id) | StreamingJobId::Sink(id) => either::Either::Right(id),
        });

        if !table_ids.is_empty() {
            stream_manager.drop_materialized_views(table_ids).await?;
        }

        if !source_ids.is_empty() {
            source_manager.drop_sources(source_ids).await?;
        }

        Ok(())
    }
}
