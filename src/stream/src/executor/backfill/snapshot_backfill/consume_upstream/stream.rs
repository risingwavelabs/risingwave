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

mod upstream_table_ext {
    use std::collections::HashMap;

    use futures::future::{try_join_all, BoxFuture};
    use futures::{TryFutureExt, TryStreamExt};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common_rate_limit::RateLimit;
    use risingwave_storage::table::ChangeLogRow;

    use crate::executor::backfill::snapshot_backfill::consume_upstream::upstream_table_trait::UpstreamTable;
    use crate::executor::backfill::snapshot_backfill::vnode_stream::{
        ChangeLogRowStream, VnodeStream,
    };
    use crate::executor::backfill::utils::create_builder;
    use crate::executor::StreamExecutorResult;

    pub(super) type UpstreamTableSnapshotStream<T: UpstreamTable> =
        VnodeStream<impl ChangeLogRowStream>;
    pub(super) type UpstreamTableSnapshotStreamFuture<'a, T> =
        BoxFuture<'a, StreamExecutorResult<UpstreamTableSnapshotStream<T>>>;
    pub(super) fn create_upstream_table_snapshot_stream<T: UpstreamTable>(
        upstream_table: &T,
        snapshot_epoch: u64,
        rate_limit: RateLimit,
        chunk_size: usize,
        vnode_progresses: HashMap<VirtualNode, (Option<OwnedRow>, usize)>,
    ) -> UpstreamTableSnapshotStreamFuture<'_, T> {
        Box::pin(async move {
            let streams = try_join_all(vnode_progresses.into_iter().map(
                |(vnode, (start_pk, row_count))| {
                    upstream_table
                        .snapshot_stream(vnode, snapshot_epoch, start_pk)
                        .map_ok(move |stream| {
                            (vnode, stream.map_ok(ChangeLogRow::Insert), row_count)
                        })
                },
            ))
            .await?;
            Ok(VnodeStream::new(
                streams,
                upstream_table.pk_in_output_indices(),
                create_builder(rate_limit, chunk_size, upstream_table.output_data_types()),
            ))
        })
    }

    pub(super) type UpstreamTableChangeLogStream<T: UpstreamTable> =
        VnodeStream<impl ChangeLogRowStream>;
    pub(super) type UpstreamTableChangeLogStreamFuture<'a, T> =
        BoxFuture<'a, StreamExecutorResult<UpstreamTableChangeLogStream<T>>>;

    pub(super) fn create_upstream_table_change_log_stream<T: UpstreamTable>(
        upstream_table: &T,
        epoch: u64,
        rate_limit: RateLimit,
        chunk_size: usize,
        vnode_progresses: HashMap<VirtualNode, (Option<OwnedRow>, usize)>,
    ) -> UpstreamTableChangeLogStreamFuture<'_, T> {
        Box::pin(async move {
            let streams = try_join_all(vnode_progresses.into_iter().map(
                |(vnode, (start_pk, row_count))| {
                    upstream_table
                        .change_log_stream(vnode, epoch, start_pk)
                        .map_ok(move |stream| (vnode, stream, row_count))
                },
            ))
            .await?;
            Ok(VnodeStream::new(
                streams,
                upstream_table.pk_in_output_indices(),
                create_builder(rate_limit, chunk_size, upstream_table.output_data_types()),
            ))
        })
    }

    pub(super) type NextEpochFuture<'a> = BoxFuture<'a, StreamExecutorResult<u64>>;
    pub(super) fn next_epoch_future<T: UpstreamTable>(
        upstream_table: &T,
        epoch: u64,
    ) -> NextEpochFuture<'_> {
        Box::pin(async move { upstream_table.next_epoch(epoch).await })
    }
}

use std::collections::{BTreeMap, HashMap};
use std::mem::take;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use risingwave_common::array::StreamChunk;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common_rate_limit::RateLimit;
use upstream_table_ext::*;

use crate::executor::backfill::snapshot_backfill::consume_upstream::upstream_table_trait::UpstreamTable;
use crate::executor::backfill::snapshot_backfill::state::{
    EpochBackfillProgress, VnodeBackfillProgress,
};
use crate::executor::prelude::{Stream, StreamExt};
use crate::executor::{StreamExecutorError, StreamExecutorResult};

enum ConsumeUpstreamStreamState<'a, T: UpstreamTable> {
    CreatingSnapshotStream {
        future: UpstreamTableSnapshotStreamFuture<'a, T>,
        snapshot_epoch: u64,
        pre_finished_vnodes: HashMap<VirtualNode, usize>,
    },
    ConsumingSnapshotStream {
        stream: UpstreamTableSnapshotStream<T>,
        snapshot_epoch: u64,
        pre_finished_vnodes: HashMap<VirtualNode, usize>,
    },
    CreatingChangeLogStream {
        future: UpstreamTableChangeLogStreamFuture<'a, T>,
        prev_epoch_finished_vnodes: Option<(u64, HashMap<VirtualNode, usize>)>,
        epoch: u64,
        pre_finished_vnodes: HashMap<VirtualNode, usize>,
    },
    ConsumingChangeLogStream {
        stream: UpstreamTableChangeLogStream<T>,
        epoch: u64,
        pre_finished_vnodes: HashMap<VirtualNode, usize>,
    },
    ResolvingNextEpoch {
        future: NextEpochFuture<'a>,
        prev_epoch_finished_vnodes: Option<(u64, HashMap<VirtualNode, usize>)>,
    },
    Err,
}

pub(super) struct ConsumeUpstreamStream<'a, T: UpstreamTable> {
    upstream_table: &'a T,
    pending_epoch_vnode_progress:
        BTreeMap<u64, HashMap<VirtualNode, (EpochBackfillProgress, usize)>>,
    state: ConsumeUpstreamStreamState<'a, T>,

    chunk_size: usize,
    rate_limit: RateLimit,
}

impl<'a, T: UpstreamTable> ConsumeUpstreamStream<'a, T> {
    pub(super) fn consume_builder(&mut self) -> Option<StreamChunk> {
        match &mut self.state {
            ConsumeUpstreamStreamState::ConsumingSnapshotStream { stream, .. } => {
                stream.consume_builder()
            }
            ConsumeUpstreamStreamState::ConsumingChangeLogStream { stream, .. } => {
                stream.consume_builder()
            }
            ConsumeUpstreamStreamState::ResolvingNextEpoch { .. }
            | ConsumeUpstreamStreamState::CreatingChangeLogStream { .. }
            | ConsumeUpstreamStreamState::CreatingSnapshotStream { .. } => None,
            ConsumeUpstreamStreamState::Err => {
                unreachable!("should not be accessed on Err")
            }
        }
    }

    pub(super) async fn for_vnode_pk_progress(
        &mut self,
        mut on_vnode_progress: impl FnMut(VirtualNode, u64, usize, Option<OwnedRow>),
    ) -> StreamExecutorResult<()> {
        match &mut self.state {
            ConsumeUpstreamStreamState::CreatingSnapshotStream { .. } => {
                // no update
            }
            ConsumeUpstreamStreamState::ConsumingSnapshotStream {
                stream,
                ref snapshot_epoch,
                ..
            } => {
                stream
                    .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
                        on_vnode_progress(vnode, *snapshot_epoch, row_count, pk_progress)
                    })
                    .await?;
            }
            ConsumeUpstreamStreamState::ConsumingChangeLogStream {
                stream, ref epoch, ..
            } => {
                stream
                    .for_vnode_pk_progress(|vnode, row_count, pk_progress| {
                        on_vnode_progress(vnode, *epoch, row_count, pk_progress)
                    })
                    .await?;
            }
            ConsumeUpstreamStreamState::CreatingChangeLogStream {
                ref prev_epoch_finished_vnodes,
                ..
            }
            | ConsumeUpstreamStreamState::ResolvingNextEpoch {
                ref prev_epoch_finished_vnodes,
                ..
            } => {
                if let Some((prev_epoch, prev_epoch_finished_vnodes)) = prev_epoch_finished_vnodes {
                    for (vnode, row_count) in prev_epoch_finished_vnodes {
                        on_vnode_progress(*vnode, *prev_epoch, *row_count, None);
                    }
                }
            }
            ConsumeUpstreamStreamState::Err => {
                unreachable!("should not be accessed on Err")
            }
        }
        Ok(())
    }
}

impl<T: UpstreamTable> Stream for ConsumeUpstreamStream<'_, T> {
    type Item = StreamExecutorResult<StreamChunk>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result: Result<!, StreamExecutorError> = try {
            loop {
                match &mut self.state {
                    ConsumeUpstreamStreamState::CreatingSnapshotStream {
                        future,
                        snapshot_epoch,
                        pre_finished_vnodes,
                    } => {
                        let stream = ready!(future.as_mut().poll(cx))?;
                        let snapshot_epoch = *snapshot_epoch;
                        let pre_finished_vnodes = take(pre_finished_vnodes);
                        self.state = ConsumeUpstreamStreamState::ConsumingSnapshotStream {
                            stream,
                            snapshot_epoch,
                            pre_finished_vnodes,
                        };
                        continue;
                    }
                    ConsumeUpstreamStreamState::ConsumingSnapshotStream {
                        stream,
                        snapshot_epoch,
                        pre_finished_vnodes,
                    } => match ready!(stream.poll_next_unpin(cx)).transpose()? {
                        None => {
                            let prev_epoch = *snapshot_epoch;
                            let mut prev_epoch_finished_vnodes = take(pre_finished_vnodes);
                            for (vnode, row_count) in stream.take_finished_vnodes() {
                                prev_epoch_finished_vnodes
                                    .try_insert(vnode, row_count)
                                    .expect("non-duplicate");
                            }
                            self.state = ConsumeUpstreamStreamState::ResolvingNextEpoch {
                                future: next_epoch_future(self.upstream_table, prev_epoch),
                                prev_epoch_finished_vnodes: Some((
                                    prev_epoch,
                                    prev_epoch_finished_vnodes,
                                )),
                            };
                            continue;
                        }
                        Some(chunk) => {
                            return Poll::Ready(Some(Ok(chunk)));
                        }
                    },
                    ConsumeUpstreamStreamState::CreatingChangeLogStream {
                        future,
                        epoch,
                        pre_finished_vnodes,
                        ..
                    } => {
                        let stream = ready!(future.as_mut().poll(cx))?;
                        let epoch = *epoch;
                        let pre_finished_vnodes = take(pre_finished_vnodes);
                        self.state = ConsumeUpstreamStreamState::ConsumingChangeLogStream {
                            stream,
                            epoch,
                            pre_finished_vnodes,
                        };
                        continue;
                    }
                    ConsumeUpstreamStreamState::ConsumingChangeLogStream {
                        stream,
                        epoch,
                        pre_finished_vnodes,
                    } => {
                        match ready!(stream.poll_next_unpin(cx)).transpose()? {
                            None => {
                                let prev_epoch = *epoch;
                                let mut prev_epoch_finished_vnodes = take(pre_finished_vnodes);
                                for (vnode, row_count) in stream.take_finished_vnodes() {
                                    prev_epoch_finished_vnodes
                                        .try_insert(vnode, row_count)
                                        .expect("non-duplicate");
                                }
                                self.state = ConsumeUpstreamStreamState::ResolvingNextEpoch {
                                    future: next_epoch_future(self.upstream_table, prev_epoch),
                                    prev_epoch_finished_vnodes: Some((
                                        prev_epoch,
                                        prev_epoch_finished_vnodes,
                                    )),
                                };
                                continue;
                            }
                            Some(chunk) => {
                                return Poll::Ready(Some(Ok(chunk)));
                            }
                        };
                    }
                    ConsumeUpstreamStreamState::ResolvingNextEpoch {
                        future,
                        prev_epoch_finished_vnodes,
                    } => {
                        let epoch = ready!(future.as_mut().poll(cx))?;
                        let prev_epoch_finished_vnodes = take(prev_epoch_finished_vnodes);
                        let mut pre_finished_vnodes = HashMap::new();
                        let mut vnode_progresses = HashMap::new();
                        for prev_epoch_vnode in prev_epoch_finished_vnodes
                            .as_ref()
                            .map(|(_, vnodes)| vnodes.keys())
                            .into_iter()
                            .flatten()
                        {
                            vnode_progresses
                                .try_insert(*prev_epoch_vnode, (None, 0))
                                .expect("non-duplicate");
                        }
                        if let Some((pending_epoch, _)) =
                            self.pending_epoch_vnode_progress.first_key_value()
                        {
                            // TODO: may return error instead to avoid panic
                            assert!(
                                epoch <= *pending_epoch,
                                "pending_epoch {} earlier than next epoch {}",
                                pending_epoch,
                                epoch
                            );
                            if epoch == *pending_epoch {
                                let (_, progress) = self
                                    .pending_epoch_vnode_progress
                                    .pop_first()
                                    .expect("checked Some");
                                for (vnode, (progress, row_count)) in progress {
                                    match progress {
                                        EpochBackfillProgress::Consuming { latest_pk } => {
                                            vnode_progresses
                                                .try_insert(vnode, (Some(latest_pk), row_count))
                                                .expect("non-duplicate");
                                        }
                                        EpochBackfillProgress::Consumed => {
                                            pre_finished_vnodes
                                                .try_insert(vnode, row_count)
                                                .expect("non-duplicate");
                                        }
                                    }
                                }
                            }
                        }
                        self.state = ConsumeUpstreamStreamState::CreatingChangeLogStream {
                            future: create_upstream_table_change_log_stream(
                                self.upstream_table,
                                epoch,
                                self.rate_limit,
                                self.chunk_size,
                                vnode_progresses,
                            ),
                            prev_epoch_finished_vnodes,
                            epoch,
                            pre_finished_vnodes,
                        };
                        continue;
                    }
                    ConsumeUpstreamStreamState::Err => {
                        unreachable!("should not be accessed on Err")
                    }
                }
            }
        };
        self.state = ConsumeUpstreamStreamState::Err;
        Poll::Ready(Some(result.map(|unreachable| unreachable)))
    }
}

impl<'a, T: UpstreamTable> ConsumeUpstreamStream<'a, T> {
    pub(super) fn new<'p>(
        initial_progress: impl Iterator<Item = (VirtualNode, Option<&'p VnodeBackfillProgress>)>,
        upstream_table: &'a T,
        snapshot_epoch: u64,
        chunk_size: usize,
        rate_limit: RateLimit,
    ) -> Self {
        let mut ongoing_snapshot_epoch_vnodes = HashMap::new();
        let mut finished_snapshot_epoch_vnodes = HashMap::new();
        let mut pending_epoch_vnode_progress: BTreeMap<_, HashMap<_, _>> = BTreeMap::new();
        for (vnode, progress) in initial_progress {
            match progress {
                None => {
                    ongoing_snapshot_epoch_vnodes
                        .try_insert(vnode, (None, 0))
                        .expect("non-duplicate");
                }
                Some(progress) => {
                    let epoch = progress.epoch;
                    let row_count = progress.row_count;
                    if epoch == snapshot_epoch {
                        match &progress.progress {
                            EpochBackfillProgress::Consumed => {
                                finished_snapshot_epoch_vnodes
                                    .try_insert(vnode, row_count)
                                    .expect("non-duplicate");
                            }
                            EpochBackfillProgress::Consuming { latest_pk } => {
                                ongoing_snapshot_epoch_vnodes
                                    .try_insert(vnode, (Some(latest_pk.clone()), row_count))
                                    .expect("non-duplicate");
                            }
                        }
                    } else {
                        assert!(
                            epoch > snapshot_epoch,
                            "epoch {} earlier than snapshot_epoch {} on vnode {}",
                            epoch,
                            snapshot_epoch,
                            vnode
                        );
                        pending_epoch_vnode_progress
                            .entry(epoch)
                            .or_default()
                            .try_insert(vnode, (progress.progress.clone(), progress.row_count))
                            .expect("non-duplicate");
                    }
                }
            };
        }
        let (pending_epoch_vnode_progress, state) = {
            if !ongoing_snapshot_epoch_vnodes.is_empty() {
                // some vnode has not finished the snapshot epoch
                (
                    pending_epoch_vnode_progress,
                    ConsumeUpstreamStreamState::CreatingSnapshotStream {
                        future: create_upstream_table_snapshot_stream(
                            upstream_table,
                            snapshot_epoch,
                            rate_limit,
                            chunk_size,
                            ongoing_snapshot_epoch_vnodes,
                        ),
                        snapshot_epoch,
                        pre_finished_vnodes: finished_snapshot_epoch_vnodes,
                    },
                )
            } else if !finished_snapshot_epoch_vnodes.is_empty() {
                // all vnodes have finished the snapshot epoch, but some vnodes has not yet start the first log epoch
                (
                    pending_epoch_vnode_progress,
                    ConsumeUpstreamStreamState::ResolvingNextEpoch {
                        future: next_epoch_future(upstream_table, snapshot_epoch),
                        prev_epoch_finished_vnodes: Some((
                            snapshot_epoch,
                            finished_snapshot_epoch_vnodes,
                        )),
                    },
                )
            } else {
                // all vnodes are in log epoch
                let (first_epoch, first_vnodes) = pending_epoch_vnode_progress
                    .pop_first()
                    .expect("non-empty vnodes");
                let mut ongoing_vnodes = HashMap::new();
                let mut finished_vnodes = HashMap::new();
                for (vnode, (progress, row_count)) in first_vnodes {
                    match progress {
                        EpochBackfillProgress::Consuming { latest_pk } => {
                            ongoing_vnodes
                                .try_insert(vnode, (Some(latest_pk), row_count))
                                .expect("non-duplicate");
                        }
                        EpochBackfillProgress::Consumed => {
                            finished_vnodes
                                .try_insert(vnode, row_count)
                                .expect("non-duplicate");
                        }
                    }
                }
                let state = if ongoing_vnodes.is_empty() {
                    // all vnodes have finished the current epoch
                    ConsumeUpstreamStreamState::ResolvingNextEpoch {
                        future: next_epoch_future(upstream_table, first_epoch),
                        prev_epoch_finished_vnodes: Some((first_epoch, finished_vnodes)),
                    }
                } else {
                    ConsumeUpstreamStreamState::CreatingChangeLogStream {
                        future: create_upstream_table_change_log_stream(
                            upstream_table,
                            first_epoch,
                            rate_limit,
                            chunk_size,
                            ongoing_vnodes,
                        ),
                        prev_epoch_finished_vnodes: None,
                        epoch: first_epoch,
                        pre_finished_vnodes: finished_vnodes,
                    }
                };
                (pending_epoch_vnode_progress, state)
            }
        };
        Self {
            upstream_table,
            pending_epoch_vnode_progress,
            state,
            chunk_size,
            rate_limit,
        }
    }
}
