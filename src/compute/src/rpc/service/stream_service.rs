// Copyright 2022 RisingWave Labs
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

use std::ops::Bound;

use foyer::Hint;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_sdk::key::prefixed_range_with_vnode;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_storage::error::{StorageError, StorageResult};
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::{
    NewReadSnapshotOptions, PrefetchOptions, ReadOptions, StateStoreIter, StateStoreRead,
};
use risingwave_storage::{StateStore, dispatch_state_store};
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct StreamServiceImpl {
    pub mgr: LocalStreamManager,
    pub env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: LocalStreamManager, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

async fn warm_up_table_cache<S: StateStore>(
    state_store: S,
    table_id: TableId,
    vnode_bitmap: Bitmap,
    read_epoch: HummockReadEpoch,
    concurrency: usize,
) -> StorageResult<u64> {
    let read_snapshot = state_store
        .new_read_snapshot(
            read_epoch,
            NewReadSnapshotOptions {
                table_id,
                table_option: TableOption::default(),
            },
        )
        .await?;

    let key_counts = stream::iter(vnode_bitmap.iter_vnodes())
        .map(|vnode| {
            let read_snapshot = read_snapshot.clone();
            async move {
                let full_range: (Bound<&[u8]>, Bound<&[u8]>) = (Bound::Unbounded, Bound::Unbounded);
                let key_range = prefixed_range_with_vnode::<&[u8]>(full_range, vnode);
                let mut iter = read_snapshot
                    .iter(
                        key_range,
                        ReadOptions {
                            prefetch_options: PrefetchOptions::prefetch_for_large_range_scan(),
                            // A whole-table warm-up should not evict blocks used by normal traffic.
                            // Low-priority entries still fill the cache, but are evicted first.
                            cache_policy: CachePolicy::Fill(Hint::Low),
                            ..Default::default()
                        },
                    )
                    .await?;
                let mut key_count = 0;
                while iter.try_next().await?.is_some() {
                    key_count += 1;
                }
                Ok::<_, StorageError>(key_count)
            }
        })
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(key_counts.into_iter().sum())
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    type StreamingControlStreamStream =
        impl Stream<Item = std::result::Result<StreamingControlStreamResponse, tonic::Status>>;

    async fn streaming_control_stream(
        &self,
        request: Request<Streaming<StreamingControlStreamRequest>>,
    ) -> Result<Response<Self::StreamingControlStreamStream>, Status> {
        let mut stream = request.into_inner().boxed();
        let first_request = stream.try_next().await?;
        let Some(StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::Init(init_request)),
        }) = first_request
        else {
            return Err(Status::invalid_argument(format!(
                "unexpected first request: {:?}",
                first_request
            )));
        };
        let (tx, rx) = unbounded_channel();
        self.mgr.handle_new_control_stream(tx, stream, init_request);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn get_min_uncommitted_object_id(
        &self,
        _request: Request<GetMinUncommittedObjectIdRequest>,
    ) -> Result<Response<GetMinUncommittedObjectIdResponse>, Status> {
        let min_uncommitted_object_id =
            if let Some(hummock) = self.mgr.env.state_store().as_hummock() {
                hummock.min_uncommitted_object_id().await
            } else {
                None
            }
            .unwrap_or_else(|| u64::MAX.into());
        Ok(Response::new(GetMinUncommittedObjectIdResponse {
            min_uncommitted_object_id,
        }))
    }

    async fn warm_up_table_cache(
        &self,
        request: Request<WarmUpTableCacheRequest>,
    ) -> Result<Response<WarmUpTableCacheResponse>, Status> {
        let request = request.into_inner();
        if request.concurrency == 0 {
            return Err(Status::invalid_argument(
                "concurrency must be greater than 0",
            ));
        }
        let table_id = request.table_id;
        let vnode_bitmap = Bitmap::from(
            request
                .vnode_bitmap
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("vnode bitmap is missing"))?,
        );

        let state_store = self.env.state_store();
        let read_epoch = HummockReadEpoch::Committed(request.committed_epoch);

        let key_count = dispatch_state_store!(state_store, state_store, {
            warm_up_table_cache(
                state_store,
                table_id,
                vnode_bitmap,
                read_epoch,
                request.concurrency as usize,
            )
            .await
        })
        .map_err(|error| {
            Status::internal(format!(
                "failed to warm up cache for table {}: {}",
                table_id,
                error.as_report()
            ))
        })?;

        Ok(Response::new(WarmUpTableCacheResponse { key_count }))
    }
}
