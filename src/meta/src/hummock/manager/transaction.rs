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

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::build_version_delta_after_version;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::HummockVersionId;

use crate::model::{InMemValTransaction, MetadataModelResult, Transactional, ValTransaction};

pub(super) struct HummockVersionTransaction<'a> {
    orig_version: &'a mut HummockVersion,
    orig_deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,

    pre_applied_version: Option<(HummockVersion, Vec<HummockVersionDelta>)>,
    deterministic_mode: bool,
}

impl<'a> HummockVersionTransaction<'a> {
    pub(super) fn new(
        version: &'a mut HummockVersion,
        deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
    ) -> Self {
        Self {
            orig_version: version,
            orig_deltas: deltas,
            pre_applied_version: None,
            deterministic_mode: false,
        }
    }

    pub(super) fn new_with_deterministic_mode(
        version: &'a mut HummockVersion,
        deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
        deterministic_mode: bool,
    ) -> Self {
        Self {
            orig_version: version,
            orig_deltas: deltas,
            pre_applied_version: None,
            deterministic_mode,
        }
    }

    pub(super) fn version(&self) -> &HummockVersion {
        if let Some((version, _)) = &self.pre_applied_version {
            version
        } else {
            self.orig_version
        }
    }

    pub(super) fn new_delta<'b>(&'b mut self) -> SingleDeltaTransaction<'a, 'b> {
        let delta = build_version_delta_after_version(self.version());
        SingleDeltaTransaction {
            version_txn: self,
            delta: Some(delta),
        }
    }

    fn pre_apply(&mut self, delta: HummockVersionDelta) {
        let (version, deltas) = self
            .pre_applied_version
            .get_or_insert_with(|| (self.orig_version.clone(), Vec::with_capacity(1)));
        version.apply_version_delta(&delta);
        deltas.push(delta);
    }
}

impl<'a> InMemValTransaction for HummockVersionTransaction<'a> {
    fn commit(self) {
        if let Some((version, deltas)) = self.pre_applied_version {
            *self.orig_version = version;
            for delta in deltas {
                assert!(self.orig_deltas.insert(delta.id, delta).is_none());
            }
        }
    }
}

impl<'a, TXN> ValTransaction<TXN> for HummockVersionTransaction<'a>
where
    HummockVersionDelta: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if self.deterministic_mode {
            return Ok(());
        }
        for delta in self
            .pre_applied_version
            .iter()
            .flat_map(|(_, deltas)| deltas.iter())
        {
            delta.upsert_in_transaction(txn).await?;
        }
        Ok(())
    }
}

pub(super) struct SingleDeltaTransaction<'a, 'b> {
    version_txn: &'b mut HummockVersionTransaction<'a>,
    delta: Option<HummockVersionDelta>,
}

impl<'a, 'b> SingleDeltaTransaction<'a, 'b> {
    pub(super) fn version(&self) -> &HummockVersion {
        self.version_txn.version()
    }

    pub(super) fn pre_apply(mut self) {
        self.version_txn.pre_apply(self.delta.take().unwrap());
    }
}

impl<'a, 'b> Deref for SingleDeltaTransaction<'a, 'b> {
    type Target = HummockVersionDelta;

    fn deref(&self) -> &Self::Target {
        self.delta.as_ref().expect("should exist")
    }
}

impl<'a, 'b> DerefMut for SingleDeltaTransaction<'a, 'b> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.delta.as_mut().expect("should exist")
    }
}

impl<'a, 'b> Drop for SingleDeltaTransaction<'a, 'b> {
    fn drop(&mut self) {
        if let Some(delta) = self.delta.take() {
            self.version_txn.pre_apply(delta);
        }
    }
}
