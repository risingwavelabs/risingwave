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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::{Either, select};
use futures::pin_mut;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::{Sink, SinkCommitCoordinator, SinkParam, build_sink};
use risingwave_pb::connector_service::SinkMetadata;
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::sleep;
use tonic::Status;
use tracing::{error, warn};

use crate::manager::sink_coordination::handle::SinkWriterCoordinationHandle;

async fn run_future_with_periodic_fn<F: Future>(
    future: F,
    interval: Duration,
    mut f: impl FnMut(),
) -> F::Output {
    pin_mut!(future);
    loop {
        match select(&mut future, pin!(sleep(interval))).await {
            Either::Left((output, _)) => {
                break output;
            }
            Either::Right(_) => f(),
        }
    }
}

struct EpochCommitRequests {
    epoch: u64,
    metadatas: Vec<SinkMetadata>,
    handle_ids: HashSet<usize>,
    committed_bitmap: Option<Bitmap>, // lazy-initialized on first request
}

impl EpochCommitRequests {
    fn new(epoch: u64) -> Self {
        Self {
            epoch,
            metadatas: vec![],
            handle_ids: Default::default(),
            committed_bitmap: None,
        }
    }

    fn add_new_request(
        &mut self,
        handle_id: usize,
        metadata: SinkMetadata,
        vnode_bitmap: Bitmap,
    ) -> anyhow::Result<()> {
        let committed_bitmap = self
            .committed_bitmap
            .get_or_insert_with(|| Bitmap::zeros(vnode_bitmap.len()));
        assert_eq!(committed_bitmap.len(), vnode_bitmap.len());

        self.metadatas.push(metadata);
        assert!(self.handle_ids.insert(handle_id));
        let check_bitmap = (&*committed_bitmap) & &vnode_bitmap;
        if check_bitmap.count_ones() > 0 {
            return Err(anyhow!(
                "duplicate vnode {:?} on epoch {}. request vnode: {:?}, prev vnode: {:?}",
                check_bitmap.iter_ones().collect_vec(),
                self.epoch,
                vnode_bitmap,
                committed_bitmap
            ));
        }
        *committed_bitmap |= &vnode_bitmap;
        Ok(())
    }

    fn can_commit(&self) -> bool {
        self.committed_bitmap.as_ref().is_some_and(|b| b.all())
    }
}

struct CoordinationHandleManager {
    param: SinkParam,
    writer_handles: HashMap<usize, SinkWriterCoordinationHandle>,
    next_handle_id: usize,
    request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
    initial_log_store_rewind_start_epoch: Option<u64>,
}

impl CoordinationHandleManager {
    fn ack_commit(
        &mut self,
        epoch: u64,
        handle_ids: impl IntoIterator<Item = usize>,
    ) -> anyhow::Result<()> {
        for handle_id in handle_ids {
            let handle = self.writer_handles.get_mut(&handle_id).ok_or_else(|| {
                anyhow!(
                    "fail to find handle for {} when ack commit on epoch {}",
                    handle_id,
                    epoch
                )
            })?;
            handle.ack_commit(epoch).map_err(|_| {
                anyhow!(
                    "fail to ack commit on epoch {} for handle {}",
                    epoch,
                    handle_id
                )
            })?;
        }
        Ok(())
    }

    async fn next_commit_request_inner(
        writer_handles: &mut HashMap<usize, SinkWriterCoordinationHandle>,
    ) -> anyhow::Result<(usize, Bitmap, u64, SinkMetadata)> {
        poll_fn(|cx| {
            'outer: loop {
                for (handle_id, handle) in writer_handles.iter_mut() {
                    if let Poll::Ready(result) = handle.poll_next_commit_request(cx) {
                        match result {
                            Ok(Some((epoch, metadata))) => {
                                return Poll::Ready(Ok((
                                    *handle_id,
                                    handle.vnode_bitmap().clone(),
                                    epoch,
                                    metadata,
                                )));
                            }
                            Ok(None) => {
                                let handle_id = *handle_id;
                                writer_handles.remove(&handle_id);
                                continue 'outer;
                            }
                            Err(e) => {
                                return Poll::Ready(Err(e));
                            }
                        }
                    }
                }
                return Poll::Pending;
            }
        })
        .await
    }

    async fn next_commit_request(&mut self) -> anyhow::Result<(usize, Bitmap, u64, SinkMetadata)> {
        loop {
            select! {
                handle = self.request_rx.recv() => {
                    let mut handle = handle.ok_or_else(|| anyhow!("end of writer request stream"))?;
                    if handle.param() != &self.param {
                        warn!(prev_param = ?self.param, new_param = ?handle.param(), "sink param mismatch");
                    }
                    handle.start(self.initial_log_store_rewind_start_epoch)?;
                    let handle_id = self.next_handle_id;
                    self.next_handle_id += 1;
                    self.writer_handles.insert(handle_id, handle);
                }
                result = Self::next_commit_request_inner(&mut self.writer_handles) => {
                    break result;
                }
            }
        }
    }
}

pub struct CoordinatorWorker {
    handle_manager: CoordinationHandleManager,
    pending_epochs: BTreeMap<u64, EpochCommitRequests>,
}

impl CoordinatorWorker {
    pub async fn run(
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
    ) {
        let sink = match build_sink(param.clone()) {
            Ok(sink) => sink,
            Err(e) => {
                error!(
                    error = %e.as_report(),
                    "unable to build sink with param {:?}",
                    param
                );
                return;
            }
        };
        dispatch_sink!(sink, sink, {
            let coordinator = match sink.new_coordinator().await {
                Ok(coordinator) => coordinator,
                Err(e) => {
                    error!(
                        error = %e.as_report(),
                        "unable to build coordinator with param {:?}",
                        param
                    );
                    return;
                }
            };
            Self::execute_coordinator(param, request_rx, coordinator).await
        });
    }

    pub async fn execute_coordinator(
        param: SinkParam,
        request_rx: UnboundedReceiver<SinkWriterCoordinationHandle>,
        coordinator: impl SinkCommitCoordinator,
    ) {
        let mut worker = CoordinatorWorker {
            handle_manager: CoordinationHandleManager {
                param,
                writer_handles: HashMap::new(),
                next_handle_id: 0,
                request_rx,
                initial_log_store_rewind_start_epoch: None,
            },
            pending_epochs: Default::default(),
        };

        if let Err(e) = worker.run_coordination(coordinator).await {
            for handle in worker.handle_manager.writer_handles.into_values() {
                handle.abort(Status::internal(format!(
                    "failed to run coordination: {:?}",
                    e.as_report()
                )))
            }
        }
    }

    async fn run_coordination(
        &mut self,
        mut coordinator: impl SinkCommitCoordinator,
    ) -> anyhow::Result<()> {
        self.handle_manager.initial_log_store_rewind_start_epoch = coordinator.init().await?;
        loop {
            let (handle_id, vnode_bitmap, epoch, metadata) =
                self.handle_manager.next_commit_request().await?;
            self.pending_epochs
                .entry(epoch)
                .or_insert_with(|| EpochCommitRequests::new(epoch))
                .add_new_request(handle_id, metadata, vnode_bitmap)?;
            if self
                .pending_epochs
                .first_key_value()
                .expect("non-empty")
                .1
                .can_commit()
            {
                let (epoch, requests) = self.pending_epochs.pop_first().expect("non-empty");
                // TODO: measure commit time
                let start_time = Instant::now();
                run_future_with_periodic_fn(
                    coordinator.commit(epoch, requests.metadatas),
                    Duration::from_secs(5),
                    || {
                        warn!(
                            elapsed = ?start_time.elapsed(),
                            sink_id = self.handle_manager.param.sink_id.sink_id,
                            "committing"
                        );
                    },
                )
                .await
                .map_err(|e| anyhow!(e))?;
                self.handle_manager.ack_commit(epoch, requests.handle_ids)?;
            }
        }
    }
}
