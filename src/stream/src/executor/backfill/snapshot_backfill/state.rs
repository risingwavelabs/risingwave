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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::replace;
use std::sync::Arc;

use anyhow::anyhow;
use futures::TryFutureExt;
use futures::future::try_join_all;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::row_serde::OrderedRowSerde;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum EpochBackfillProgress {
    Consuming { latest_pk: OwnedRow },
    Consumed,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct VnodeBackfillProgress {
    pub(super) epoch: u64,
    pub(super) row_count: usize,
    pub(super) progress: EpochBackfillProgress,
}

/// `vnode`, `epoch`, `row_count`, `is_finished`
const EXTRA_COLUMN_TYPES: [DataType; 4] = [
    DataType::Int16,
    DataType::Int64,
    DataType::Int64,
    DataType::Boolean,
];

impl VnodeBackfillProgress {
    fn validate_progress_table_schema(
        progress_table_column_types: &[DataType],
        upstream_pk_column_types: &[DataType],
    ) -> StreamExecutorResult<()> {
        if progress_table_column_types.len()
            != EXTRA_COLUMN_TYPES.len() + upstream_pk_column_types.len()
        {
            return Err(anyhow!(
                "progress table columns len not matched with the len derived from upstream table pk. progress table: {:?}, pk: {:?}",
                progress_table_column_types,
                upstream_pk_column_types)
                .into()
            );
        }
        for (expected_type, progress_table_type) in EXTRA_COLUMN_TYPES
            .iter()
            .chain(upstream_pk_column_types.iter())
            .zip_eq_debug(progress_table_column_types.iter())
        {
            if expected_type != progress_table_type {
                return Err(anyhow!(
                    "progress table column not matched with upstream table schema: progress table: {:?}, pk: {:?}",
                    progress_table_column_types,
                    upstream_pk_column_types)
                    .into()
                );
            }
        }
        Ok(())
    }

    pub(super) fn from_row(row: &OwnedRow, pk_serde: &OrderedRowSerde) -> Self {
        assert_eq!(
            row.len(),
            pk_serde.get_data_types().len() + EXTRA_COLUMN_TYPES.len() - 1, /* Pk of the progress state table (i.e. vnode column) not included */
        );
        let epoch = must_match!(&row[0], Some(ScalarImpl::Int64(epoch)) => {
           *epoch as u64
        });
        let row_count = must_match!(&row[1], Some(ScalarImpl::Int64(row_count)) => {
           *row_count as usize
        });
        let is_finished = must_match!(&row[2], Some(ScalarImpl::Bool(is_finished)) => {
           *is_finished
        });
        Self {
            epoch,
            row_count,
            progress: if !is_finished {
                EpochBackfillProgress::Consuming {
                    latest_pk: row.slice(EXTRA_COLUMN_TYPES.len() - 1..).to_owned_row(),
                }
            } else {
                row.slice(EXTRA_COLUMN_TYPES.len() - 1..)
                    .iter()
                    .enumerate()
                    .for_each(|(i, datum)| {
                        if datum.is_some() {
                            if cfg!(debug_assertions) {
                                panic!("get non-empty pk row: {:?}", row);
                            } else {
                                warn!(
                                    i,
                                    row = ?row,
                                    "get non-empty pk row. will be ignore"
                                );
                            }
                        }
                    });
                EpochBackfillProgress::Consumed
            },
        }
    }

    fn build_row<'a>(
        &'a self,
        vnode: VirtualNode,
        consumed_pk_rows: &'a OwnedRow,
    ) -> impl Row + 'a {
        let (is_finished, pk) = match &self.progress {
            EpochBackfillProgress::Consuming { latest_pk } => {
                assert_eq!(latest_pk.len(), consumed_pk_rows.len());
                (false, latest_pk)
            }
            EpochBackfillProgress::Consumed => (true, consumed_pk_rows),
        };
        [
            Some(ScalarImpl::Int16(vnode.to_scalar())),
            Some(ScalarImpl::Int64(self.epoch as _)),
            Some(ScalarImpl::Int64(self.row_count as _)),
            Some(ScalarImpl::Bool(is_finished)),
        ]
        .chain(pk)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum VnodeBackfillState {
    New(VnodeBackfillProgress),
    Update {
        latest: VnodeBackfillProgress,
        committed: VnodeBackfillProgress,
    },
    Committed(VnodeBackfillProgress),
}

impl VnodeBackfillState {
    fn update_inner(&mut self, latest_progress: VnodeBackfillProgress) {
        let temp_place_holder = Self::temp_placeholder();
        let prev_state = replace(self, temp_place_holder);
        *self = match prev_state {
            VnodeBackfillState::New(_) => VnodeBackfillState::New(latest_progress),
            VnodeBackfillState::Update { committed, .. } => VnodeBackfillState::Update {
                latest: latest_progress,
                committed,
            },
            VnodeBackfillState::Committed(committed) => VnodeBackfillState::Update {
                latest: latest_progress,
                committed,
            },
        };
    }

    fn mark_committed(&mut self) {
        *self = VnodeBackfillState::Committed(match replace(self, Self::temp_placeholder()) {
            VnodeBackfillState::New(progress) => progress,
            VnodeBackfillState::Update { latest, .. } => latest,
            VnodeBackfillState::Committed(progress) => progress,
        });
    }

    fn latest_progress(&self) -> &VnodeBackfillProgress {
        match self {
            VnodeBackfillState::New(progress) => progress,
            VnodeBackfillState::Update { latest, .. } => latest,
            VnodeBackfillState::Committed(progress) => progress,
        }
    }

    fn temp_placeholder() -> Self {
        Self::New(VnodeBackfillProgress {
            epoch: 0,
            row_count: 0,
            progress: EpochBackfillProgress::Consumed,
        })
    }
}

use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::StreamExecutorResult;
use crate::executor::prelude::StateTable;

pub(super) struct BackfillState<S: StateStore> {
    vnode_state: HashMap<VirtualNode, VnodeBackfillState>,
    pk_serde: OrderedRowSerde,
    consumed_pk_rows: OwnedRow,
    state_table: StateTable<S>,
}

impl<S: StateStore> BackfillState<S> {
    pub(super) async fn new(
        mut state_table: StateTable<S>,
        init_epoch: EpochPair,
        pk_serde: OrderedRowSerde,
    ) -> StreamExecutorResult<Self> {
        VnodeBackfillProgress::validate_progress_table_schema(
            state_table.get_data_types(),
            pk_serde.get_data_types(),
        )?;
        state_table.init_epoch(init_epoch).await?;
        let mut vnode_state = HashMap::new();
        let committed_progress_row = Self::load_vnode_progress_row(&state_table).await?;
        for (vnode, progress_row) in committed_progress_row {
            let Some(progress_row) = progress_row else {
                continue;
            };
            let progress = VnodeBackfillProgress::from_row(&progress_row, &pk_serde);
            debug!(?vnode, ?progress, "load initial progress");
            assert!(
                vnode_state
                    .insert(vnode, VnodeBackfillState::Committed(progress))
                    .is_none()
            );
        }
        let consumed_pk_rows = OwnedRow::new(vec![None; pk_serde.get_data_types().len()]);
        Ok(Self {
            vnode_state,
            pk_serde,
            consumed_pk_rows,
            state_table,
        })
    }

    async fn load_vnode_progress_row(
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Vec<(VirtualNode, Option<OwnedRow>)>> {
        let rows = try_join_all(state_table.vnodes().iter_vnodes().map(|vnode| {
            state_table
                .get_row([vnode.to_datum()])
                .map_ok(move |progress_row| (vnode, progress_row))
        }))
        .await?;
        Ok(rows)
    }

    fn update_progress(&mut self, vnode: VirtualNode, progress: VnodeBackfillProgress) {
        match self.vnode_state.entry(vnode) {
            Entry::Occupied(entry) => {
                let state = entry.into_mut();
                let prev_progress = state.latest_progress();
                if prev_progress == &progress {
                    // ignore if no update
                    return;
                }
                // sanity check
                {
                    assert!(
                        prev_progress.epoch <= progress.epoch,
                        "progress epoch regress from {} to {}",
                        prev_progress.epoch,
                        progress.epoch
                    );
                    match &prev_progress.progress {
                        EpochBackfillProgress::Consuming { latest_pk: prev_pk } => {
                            if prev_progress.epoch == progress.epoch
                                && let EpochBackfillProgress::Consuming { latest_pk: pk } =
                                    &progress.progress
                            {
                                assert_eq!(pk.len(), self.pk_serde.get_data_types().len());
                                assert!(
                                    prev_progress.row_count <= progress.row_count,
                                    "{} <= {}, vnode: {:?}",
                                    prev_progress.row_count,
                                    progress.row_count,
                                    vnode,
                                );
                                if cfg!(debug_assertions) {
                                    let mut prev_buf = vec![];
                                    self.pk_serde.serialize(prev_pk, &mut prev_buf);
                                    let mut buf = vec![];
                                    self.pk_serde.serialize(pk, &mut buf);
                                    assert!(
                                        buf > prev_buf,
                                        "new pk progress: {:?} not exceed prev pk progress: {:?}",
                                        pk,
                                        prev_pk
                                    );
                                }
                            }
                        }
                        EpochBackfillProgress::Consumed => {
                            assert!(
                                prev_progress.epoch < progress.epoch,
                                "{:?} {:?}",
                                prev_progress,
                                progress
                            );
                        }
                    }
                }
                state.update_inner(progress);
            }
            Entry::Vacant(entry) => {
                entry.insert(VnodeBackfillState::New(progress));
            }
        }
    }

    pub(super) fn update_epoch_progress(
        &mut self,
        vnode: VirtualNode,
        epoch: u64,
        row_count: usize,
        pk: OwnedRow,
    ) {
        self.update_progress(
            vnode,
            VnodeBackfillProgress {
                epoch,
                row_count,
                progress: EpochBackfillProgress::Consuming { latest_pk: pk },
            },
        )
    }

    pub(super) fn finish_epoch(&mut self, vnode: VirtualNode, epoch: u64, row_count: usize) {
        self.update_progress(
            vnode,
            VnodeBackfillProgress {
                epoch,
                row_count,
                progress: EpochBackfillProgress::Consumed,
            },
        );
    }

    pub(super) fn latest_progress(
        &self,
    ) -> impl Iterator<Item = (VirtualNode, Option<&VnodeBackfillProgress>)> {
        self.state_table.vnodes().iter_vnodes().map(|vnode| {
            (
                vnode,
                self.vnode_state
                    .get(&vnode)
                    .map(VnodeBackfillState::latest_progress),
            )
        })
    }

    pub(super) async fn commit(
        &mut self,
        barrier_epoch: EpochPair,
    ) -> StreamExecutorResult<BackfillStatePostCommit<'_, S>> {
        for (vnode, state) in &self.vnode_state {
            match state {
                VnodeBackfillState::New(progress) => {
                    self.state_table
                        .insert(progress.build_row(*vnode, &self.consumed_pk_rows));
                }
                VnodeBackfillState::Update { latest, committed } => {
                    self.state_table.update(
                        committed.build_row(*vnode, &self.consumed_pk_rows),
                        latest.build_row(*vnode, &self.consumed_pk_rows),
                    );
                }
                VnodeBackfillState::Committed(_) => {}
            }
        }
        let post_commit = self.state_table.commit(barrier_epoch).await?;
        self.vnode_state
            .values_mut()
            .for_each(VnodeBackfillState::mark_committed);
        Ok(BackfillStatePostCommit {
            inner: post_commit,
            vnode_state: &mut self.vnode_state,
            pk_serde: &self.pk_serde,
        })
    }
}

#[must_use]
pub(super) struct BackfillStatePostCommit<'a, S: StateStore> {
    inner: StateTablePostCommit<'a, S>,
    vnode_state: &'a mut HashMap<VirtualNode, VnodeBackfillState>,
    pk_serde: &'a OrderedRowSerde,
}

impl<S: StateStore> BackfillStatePostCommit<'_, S> {
    pub(super) async fn post_yield_barrier(
        self,
        new_vnode_bitmap: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<Option<Arc<Bitmap>>> {
        let new_vnode_bitmap = if let Some(((new_vnode_bitmap, prev_vnode_bitmap, state), _)) =
            self.inner.post_yield_barrier(new_vnode_bitmap).await?
        {
            Self::update_vnode_bitmap(&*state, self.vnode_state, self.pk_serde, prev_vnode_bitmap)
                .await?;
            Some(new_vnode_bitmap)
        } else {
            None
        };
        Ok(new_vnode_bitmap)
    }

    async fn update_vnode_bitmap(
        state_table: &StateTable<S>,
        vnode_state: &mut HashMap<VirtualNode, VnodeBackfillState>,
        pk_serde: &OrderedRowSerde,
        prev_vnode_bitmap: Arc<Bitmap>,
    ) -> StreamExecutorResult<()> {
        let committed_progress_rows = BackfillState::load_vnode_progress_row(state_table).await?;
        let mut new_state = HashMap::new();
        for (vnode, progress_row) in committed_progress_rows {
            if let Some(progress_row) = progress_row {
                let progress = VnodeBackfillProgress::from_row(&progress_row, pk_serde);
                assert!(
                    new_state
                        .insert(vnode, VnodeBackfillState::Committed(progress))
                        .is_none()
                );
            }

            if prev_vnode_bitmap.is_set(vnode.to_index()) {
                // if the vnode exist previously, the new state should be the same as the previous one
                assert_eq!(vnode_state.get(&vnode), new_state.get(&vnode));
            }
        }
        *vnode_state = new_state;
        Ok(())
    }
}
