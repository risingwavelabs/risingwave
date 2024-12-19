// Copyright 2024 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::replace;

use risingwave_common::hash::VirtualNode;
use risingwave_common::must_match;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::row_serde::OrderedRowSerde;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum EpochBackfillProgress {
    Consuming { latest_pk: OwnedRow },
    Consumed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct VnodeBackfillProgress {
    pub(super) epoch: u64,
    pub(super) progress: EpochBackfillProgress,
}

/// `vnode`, `epoch`, `is_finished`
const EXTRA_COLUMN_TYPES: [DataType; 3] = [DataType::Int16, DataType::Int64, DataType::Boolean];

impl VnodeBackfillProgress {
    fn from_row(row: &OwnedRow, pk_serde: &OrderedRowSerde) -> (VirtualNode, Self) {
        assert_eq!(
            row.len(),
            pk_serde.get_data_types().len() + EXTRA_COLUMN_TYPES.len()
        );
        let vnode = must_match!(&row[0], Some(ScalarImpl::Int16(vnode)) => {
           VirtualNode::from_scalar(*vnode)
        });
        let epoch = must_match!(&row[1], Some(ScalarImpl::Int64(epoch)) => {
           *epoch as u64
        });
        let is_finished = must_match!(&row[2], Some(ScalarImpl::Bool(is_finished)) => {
           *is_finished
        });
        (
            vnode,
            Self {
                epoch,
                progress: if is_finished {
                    EpochBackfillProgress::Consuming {
                        latest_pk: row.slice(EXTRA_COLUMN_TYPES.len()..).to_owned_row(),
                    }
                } else {
                    row.slice(EXTRA_COLUMN_TYPES.len()..)
                        .iter()
                        .enumerate()
                        .for_each(|(i, datum)| {
                            if datum.is_some() {
                                if cfg!(debug_assertions) {
                                    panic!("get non-empty pk row: {:?}", row);
                                } else {
                                    warn!(
                                        ?vnode,
                                        i,
                                        row = ?row,
                                        "get non-empty pk row. will be ignore"
                                    );
                                }
                            }
                        });
                    EpochBackfillProgress::Consumed
                },
            },
        )
    }
}

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
        let temp_place_holder = Self::temp_place_holder();
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
        *self = VnodeBackfillState::Committed(match replace(self, Self::temp_place_holder()) {
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

    fn temp_place_holder() -> Self {
        Self::New(VnodeBackfillProgress {
            epoch: 0,
            progress: EpochBackfillProgress::Consumed,
        })
    }
}

mod progress_row {
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::{OwnedRow, Row, RowExt};
    use risingwave_common::types::ScalarImpl;

    use crate::executor::backfill::snapshot_backfill::state::{
        EpochBackfillProgress, VnodeBackfillProgress,
    };

    pub(in super::super) type BackfillProgressRow<'a> = impl Row + 'a;

    impl VnodeBackfillProgress {
        pub(super) fn build_row<'a>(
            &'a self,
            vnode: VirtualNode,
            consumed_pk_rows: &'a OwnedRow,
        ) -> BackfillProgressRow<'a> {
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
                Some(ScalarImpl::Bool(is_finished)),
            ]
            .chain(pk)
        }
    }
}

pub(super) use progress_row::*;

pub(super) struct BackfillState {
    vnode_state: HashMap<VirtualNode, VnodeBackfillState>,
    pk_serde: OrderedRowSerde,
    consumed_pk_rows: OwnedRow,
}

impl BackfillState {
    pub(super) fn new<'a>(
        committed_progress: impl IntoIterator<Item = (VirtualNode, &'a OwnedRow)>,
        pk_serde: OrderedRowSerde,
    ) -> Self {
        let mut vnode_state = HashMap::new();
        for (vnode, progress) in committed_progress.into_iter().map(|(vnode, row)| {
            let (row_vnode, progress) = VnodeBackfillProgress::from_row(row, &pk_serde);
            assert_eq!(row_vnode, vnode);
            (vnode, progress)
        }) {
            assert!(vnode_state
                .insert(vnode, VnodeBackfillState::Committed(progress))
                .is_none());
        }
        let consumed_pk_rows = OwnedRow::new(vec![None; pk_serde.get_data_types().len()]);
        Self {
            vnode_state,
            pk_serde,
            consumed_pk_rows,
        }
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
                    match &prev_progress.progress {
                        EpochBackfillProgress::Consuming { latest_pk: prev_pk } => {
                            assert_eq!(prev_progress.epoch, progress.epoch);
                            if let EpochBackfillProgress::Consuming { latest_pk: pk } =
                                &progress.progress
                            {
                                assert_eq!(pk.len(), self.pk_serde.get_data_types().len());
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
                            assert!(prev_progress.epoch < progress.epoch);
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

    pub(super) fn update_epoch_progress(&mut self, vnode: VirtualNode, epoch: u64, pk: OwnedRow) {
        self.update_progress(
            vnode,
            VnodeBackfillProgress {
                epoch,
                progress: EpochBackfillProgress::Consuming { latest_pk: pk },
            },
        )
    }

    pub(super) fn finish_epoch(
        &mut self,
        vnodes: impl IntoIterator<Item = VirtualNode>,
        epoch: u64,
    ) {
        for vnode in vnodes {
            self.update_progress(
                vnode,
                VnodeBackfillProgress {
                    epoch,
                    progress: EpochBackfillProgress::Consumed,
                },
            )
        }
    }

    #[expect(dead_code)]
    pub(super) fn latest_progress(&self, vnode: VirtualNode) -> Option<&VnodeBackfillProgress> {
        self.vnode_state
            .get(&vnode)
            .map(VnodeBackfillState::latest_progress)
    }

    pub(super) fn uncommitted_state(
        &self,
    ) -> impl Iterator<
        Item = (
            VirtualNode,
            Option<BackfillProgressRow<'_>>,
            BackfillProgressRow<'_>,
        ),
    > + '_ {
        self.vnode_state
            .iter()
            .filter_map(|(vnode, state)| match state {
                VnodeBackfillState::New(progress) => Some((
                    *vnode,
                    None,
                    progress.build_row(*vnode, &self.consumed_pk_rows),
                )),
                VnodeBackfillState::Update { latest, committed } => Some((
                    *vnode,
                    Some(committed.build_row(*vnode, &self.consumed_pk_rows)),
                    latest.build_row(*vnode, &self.consumed_pk_rows),
                )),
                VnodeBackfillState::Committed(_) => None,
            })
    }

    pub(super) fn mark_committed(&mut self) {
        self.vnode_state
            .values_mut()
            .for_each(VnodeBackfillState::mark_committed)
    }

    pub(super) fn update_vnode_bitmap(
        &mut self,
        new_vnodes: impl Iterator<Item = (VirtualNode, u64, Option<OwnedRow>)>,
    ) {
        let mut new_state = HashMap::new();
        for (vnode, epoch, pk) in new_vnodes {
            let progress = VnodeBackfillProgress {
                epoch,
                progress: pk
                    .map(|latest_pk| EpochBackfillProgress::Consuming { latest_pk })
                    .unwrap_or(EpochBackfillProgress::Consumed),
            };
            if let Some(prev_progress) = self.vnode_state.get(&vnode) {
                let prev_progress = must_match!(prev_progress, VnodeBackfillState::Committed(prev_progress) => {
                    prev_progress
                });
                assert_eq!(prev_progress, &progress);
            }
            assert!(new_state
                .insert(vnode, VnodeBackfillState::Committed(progress))
                .is_none());
        }
        self.vnode_state = new_state;
    }
}
