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

use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId, SstIdRange};
use risingwave_pb::meta::heartbeat_request::extra_info::Info;
use risingwave_rpc_client::{ExtraInfoSource, HummockMetaClient};
use sync_point::sync_point;
use tokio::sync::Notify;

use crate::hummock::{HummockError, HummockResult};

pub type SstableIdManagerRef = Arc<SstableIdManager>;

/// 1. Caches SST ids fetched from meta.
/// 2. Maintains GC watermark SST id.
///
/// During full GC, SST in object store with id >= watermark SST id will be excluded from orphan SST
/// candidate and thus won't be deleted.
pub struct SstableIdManager {
    // Lock order: `notifier` before `available_sst_ids`.
    notifier: Mutex<Option<Arc<Notify>>>,
    available_sst_ids: Mutex<SstIdRange>,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sst_id_tracker: SstIdTracker,
}

impl SstableIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            notifier: Default::default(),
            available_sst_ids: Mutex::new(SstIdRange::new(
                HummockSstableId::MIN,
                HummockSstableId::MIN,
            )),
            remote_fetch_number,
            hummock_meta_client,
            sst_id_tracker: SstIdTracker::new(),
        }
    }

    /// Returns a new SST id.
    /// The id is guaranteed to be monotonic increasing.
    pub async fn get_new_sst_id(self: &Arc<Self>) -> HummockResult<HummockSstableId> {
        self.map_next_sst_id(|available_sst_ids| available_sst_ids.get_next_sst_id())
            .await
    }

    /// Executes `f` with next SST id.
    /// May fetch new SST ids via RPC.
    async fn map_next_sst_id<F>(self: &Arc<Self>, f: F) -> HummockResult<HummockSstableId>
    where
        F: Fn(&mut SstIdRange) -> Option<HummockSstableId>,
    {
        loop {
            // 1. Try to get
            if let Some(new_id) = f(self.available_sst_ids.lock().deref_mut()) {
                return Ok(new_id);
            }
            // 2. Otherwise either fetch new ids, or wait for previous fetch if any.
            let (notify, to_fetch) = {
                let mut guard = self.notifier.lock();
                if let Some(new_id) = f(self.available_sst_ids.lock().deref_mut()) {
                    return Ok(new_id);
                }
                match guard.deref() {
                    None => {
                        let notify = Arc::new(Notify::new());
                        *guard = Some(notify.clone());
                        (notify, true)
                    }
                    Some(notify) => (notify.clone(), false),
                }
            };
            if !to_fetch {
                // Wait for previous fetch
                sync_point!("MAP_NEXT_SST_ID.AS_FOLLOWER");
                notify.notified().await;
                notify.notify_one();
                continue;
            }
            // Fetch new ids.
            sync_point!("MAP_NEXT_SST_ID.AS_LEADER");
            sync_point!("MAP_NEXT_SST_ID.BEFORE_FETCH");
            let this = self.clone();
            tokio::spawn(async move {
                let new_sst_ids = match this
                    .hummock_meta_client
                    .get_new_sst_ids(this.remote_fetch_number)
                    .await
                    .map_err(HummockError::meta_error)
                {
                    Ok(new_sst_ids) => new_sst_ids,
                    Err(err) => {
                        this.notifier.lock().take().unwrap().notify_one();
                        return Err(err);
                    }
                };
                sync_point!("MAP_NEXT_SST_ID.AFTER_FETCH");
                sync_point!("MAP_NEXT_SST_ID.BEFORE_FILL_CACHE");
                // Update local cache.
                let result = {
                    let mut guard = this.available_sst_ids.lock();
                    let available_sst_ids = guard.deref_mut();
                    if new_sst_ids.start_id < available_sst_ids.end_id {
                        Err(HummockError::meta_error(format!(
                            "SST id moves backwards. new {} < old {}",
                            new_sst_ids.start_id, available_sst_ids.end_id
                        )))
                    } else {
                        *available_sst_ids = new_sst_ids;
                        Ok(())
                    }
                };
                this.notifier.lock().take().unwrap().notify_one();
                result
            })
            .await
            .unwrap()?;
        }
    }

    /// Adds a new watermark SST id using the next unused SST id.
    /// Returns a tracker id. It's used to remove the watermark later.
    /// - Uses given 'epoch' as tracker id if provided.
    /// - Uses a generated tracker id otherwise.
    pub async fn add_watermark_sst_id(
        self: &Arc<Self>,
        epoch: Option<HummockEpoch>,
    ) -> HummockResult<TrackerId> {
        let tracker_id = match epoch {
            None => self.sst_id_tracker.get_next_auto_tracker_id(),
            Some(epoch) => TrackerId::Epoch(epoch),
        };
        let next_sst_id = self
            .map_next_sst_id(|available_sst_ids| available_sst_ids.peek_next_sst_id())
            .await?;
        self.sst_id_tracker.add_tracker(tracker_id, next_sst_id);
        Ok(tracker_id)
    }

    pub fn remove_watermark_sst_id(&self, tracker_id: TrackerId) {
        self.sst_id_tracker.remove_tracker(tracker_id);
    }

    /// Returns GC watermark. It equals
    /// - min(effective watermarks), if number of effective watermarks > 0.
    /// - `HummockSstableId::MAX`, if no effective watermark.
    pub fn global_watermark_sst_id(&self) -> HummockSstableId {
        self.sst_id_tracker
            .tracking_sst_ids()
            .into_iter()
            .min()
            .unwrap_or(HummockSstableId::MAX)
    }
}

#[async_trait::async_trait]
impl ExtraInfoSource for SstableIdManager {
    async fn get_extra_info(&self) -> Option<Info> {
        Some(Info::HummockGcWatermark(self.global_watermark_sst_id()))
    }
}

type AutoTrackerId = u64;

#[derive(Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub enum TrackerId {
    Auto(AutoTrackerId),
    Epoch(HummockEpoch),
}

/// `SstIdTracker` tracks a min(SST id) for various caller, identified by a `TrackerId`.
pub struct SstIdTracker {
    auto_id: AtomicU64,
    inner: parking_lot::RwLock<SstIdTrackerInner>,
}

impl SstIdTracker {
    fn new() -> Self {
        Self {
            auto_id: Default::default(),
            inner: parking_lot::RwLock::new(SstIdTrackerInner::new()),
        }
    }

    /// Adds a tracker to track `sst_id`. If a tracker with `tracker_id` already exists, it will
    /// track the smallest `sst_id` ever given.
    fn add_tracker(&self, tracker_id: TrackerId, sst_id: HummockSstableId) {
        self.inner.write().add_tracker(tracker_id, sst_id);
    }

    /// Removes given `tracker_id`.
    fn remove_tracker(&self, tracker_id: TrackerId) {
        self.inner.write().remove_tracker(tracker_id);
    }

    fn get_next_auto_tracker_id(&self) -> TrackerId {
        TrackerId::Auto(self.auto_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn tracking_sst_ids(&self) -> Vec<HummockSstableId> {
        self.inner.read().tracking_sst_ids()
    }
}

struct SstIdTrackerInner {
    tracking_sst_ids: HashMap<TrackerId, HummockSstableId>,
}

impl SstIdTrackerInner {
    fn new() -> Self {
        Self {
            tracking_sst_ids: Default::default(),
        }
    }

    fn add_tracker(&mut self, tracker_id: TrackerId, sst_id: HummockSstableId) {
        match self.tracking_sst_ids.entry(tracker_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() = cmp::min(*o.get_mut(), sst_id);
            }
            Entry::Vacant(v) => {
                v.insert(sst_id);
            }
        }
    }

    fn remove_tracker(&mut self, tracker_id: TrackerId) {
        self.tracking_sst_ids.remove(&tracker_id);
    }

    fn tracking_sst_ids(&self) -> Vec<HummockSstableId> {
        self.tracking_sst_ids.values().cloned().collect_vec()
    }
}

#[cfg(test)]
mod test {

    use risingwave_common::try_match_expand;

    use crate::hummock::sstable::sstable_id_manager::AutoTrackerId;
    use crate::hummock::{SstIdTracker, TrackerId};

    #[tokio::test]
    async fn test_sst_id_tracker_basic() {
        let sst_id_tacker = SstIdTracker::new();
        assert!(sst_id_tacker.tracking_sst_ids().is_empty());
        let auto_id =
            try_match_expand!(sst_id_tacker.get_next_auto_tracker_id(), TrackerId::Auto).unwrap();
        assert_eq!(auto_id, AutoTrackerId::MIN + 1);

        let auto_id_1 = sst_id_tacker.get_next_auto_tracker_id();
        let auto_id_2 = sst_id_tacker.get_next_auto_tracker_id();
        let auto_id_3 = sst_id_tacker.get_next_auto_tracker_id();

        sst_id_tacker.add_tracker(auto_id_1, 10);
        assert_eq!(
            sst_id_tacker.tracking_sst_ids().into_iter().min().unwrap(),
            10
        );

        // OK to move SST id backwards.
        sst_id_tacker.add_tracker(auto_id_1, 9);
        sst_id_tacker.add_tracker(auto_id_2, 9);

        // OK to add same id to the same tracker
        sst_id_tacker.add_tracker(auto_id_1, 10);
        // OK to add same id to another tracker
        sst_id_tacker.add_tracker(auto_id_2, 10);

        sst_id_tacker.add_tracker(auto_id_3, 20);
        sst_id_tacker.add_tracker(auto_id_2, 30);
        // Tracker 1 and 2 both hold id 9.
        assert_eq!(
            sst_id_tacker.tracking_sst_ids().into_iter().min().unwrap(),
            9
        );

        sst_id_tacker.remove_tracker(auto_id_1);
        // Tracker 2 still holds 9.
        assert_eq!(
            sst_id_tacker.tracking_sst_ids().into_iter().min().unwrap(),
            9
        );

        sst_id_tacker.remove_tracker(auto_id_2);
        assert_eq!(
            sst_id_tacker.tracking_sst_ids().into_iter().min().unwrap(),
            20
        );

        sst_id_tacker.remove_tracker(auto_id_3);
        assert!(sst_id_tacker.tracking_sst_ids().is_empty());
    }
}
