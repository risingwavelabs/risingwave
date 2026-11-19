// Copyright 2026 RisingWave Labs
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

use bytes::Bytes;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;

use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;
use crate::executor::prelude::StateStore;

/// The rows of a backfilled arrangement that a lookup can see.
///
/// When the arrangement input of a lookup is the output of an arrangement backfill, the lookup
/// must only see the rows that the backfill has emitted. Otherwise the changes of the stream side
/// join with rows that the backfill emits later, and the join result counts them twice. The
/// backfill emits the snapshot of each vnode in primary key order, and only forwards the upstream
/// changes of the rows up to the current position, so the emitted rows of a vnode are exactly the
/// rows whose primary key is no greater than the largest primary key seen on the arrangement
/// input. After the backfill of a vnode finishes, all changes are forwarded, so the bound keeps
/// covering all rows.
///
/// The bound of each vnode, memcomparable-encoded in the order of the arrangement's primary key, is
/// persisted in a state table keyed by vnode, and committed with the epoch whose arrangement
/// changes it includes.
pub struct ArrangementVisibility<S: StateStore> {
    state_table: StateTable<S>,
    /// The largest primary key seen on the arrangement input of each vnode.
    bounds: HashMap<VirtualNode, Bytes>,
    /// The vnodes whose bound changed since the last commit, with their persisted bound.
    dirty: HashMap<VirtualNode, Option<Bytes>>,
}

impl<S: StateStore> ArrangementVisibility<S> {
    pub fn new(state_table: StateTable<S>) -> Self {
        Self {
            state_table,
            bounds: HashMap::new(),
            dirty: HashMap::new(),
        }
    }

    /// Initializes the state table and loads the persisted bounds.
    pub async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch).await?;
        let vnodes = self.state_table.vnodes().clone();
        for vnode in vnodes.iter_vnodes() {
            if let Some(row) = self.state_table.get_row([vnode.to_datum()]).await? {
                // The bound is the last column, whether or not the row includes the vnode.
                let Some(ScalarImpl::Bytea(bound)) =
                    row.datum_at(row.len() - 1).map(|d| d.into_scalar_impl())
                else {
                    unreachable!("visibility bound should be non-null bytea");
                };
                self.bounds.insert(vnode, Bytes::from(bound));
            }
        }
        Ok(())
    }

    /// Records a primary key seen on the arrangement input.
    pub fn observe(&mut self, vnode: VirtualNode, pk: &[u8]) {
        let old = self.bounds.get(&vnode);
        if old.is_some_and(|old| old.as_ref() >= pk) {
            return;
        }
        let old = self.bounds.insert(vnode, Bytes::copy_from_slice(pk));
        self.dirty.entry(vnode).or_insert(old);
    }

    /// Whether the row with the primary key has been emitted by the backfill.
    pub fn is_visible(&self, vnode: VirtualNode, pk: &[u8]) -> bool {
        self.bounds
            .get(&vnode)
            .is_some_and(|bound| pk <= bound.as_ref())
    }

    fn row(vnode: VirtualNode, bound: &Bytes) -> OwnedRow {
        OwnedRow::new(vec![
            vnode.to_datum(),
            Some(ScalarImpl::Bytea(bound.to_vec().into_boxed_slice())),
        ])
    }

    /// Persists the changed bounds and commits the epoch.
    pub async fn commit(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        for (vnode, old) in self.dirty.drain() {
            let new = Self::row(vnode, &self.bounds[&vnode]);
            match old {
                Some(old) => self.state_table.update(Self::row(vnode, &old), new),
                None => self.state_table.insert(new),
            }
        }
        self.state_table
            .commit_assert_no_update_vnode_bitmap(epoch)
            .await
    }
}
