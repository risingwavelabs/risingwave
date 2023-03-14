// Copyright 2023 RisingWave Labs
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

use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId, SstObjectIdRange};
use risingwave_pb::meta::heartbeat_request::extra_info::Info;
use risingwave_rpc_client::{ExtraInfoSource, HummockMetaClient};
use sync_point::sync_point;
use tokio::sync::oneshot;

use crate::hummock::{HummockError, HummockResult};

pub type SstableObjectIdManagerRef = Arc<SstableObjectIdManager>;

/// 1. Caches SST object ids fetched from meta.
/// 2. Maintains GC watermark SST object id.
///
/// During full GC, SST in object store with object id >= watermark SST object id will be excluded
/// from orphan SST object candidate and thus won't be deleted.
pub struct SstableObjectIdManager {
    // Lock order: `wait_queue` before `available_sst_object_ids`.
    wait_queue: Mutex<Option<Vec<oneshot::Sender<bool>>>>,
    available_sst_object_ids: Mutex<SstObjectIdRange>,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    object_id_tracker: SstObjectIdTracker,
}

impl SstableObjectIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            wait_queue: Default::default(),
            available_sst_object_ids: Mutex::new(SstObjectIdRange::new(
                HummockSstableId::MIN,
                HummockSstableId::MIN,
            )),
            remote_fetch_number,
            hummock_meta_client,
            object_id_tracker: SstObjectIdTracker::new(),
        }
    }

    /// Returns a new SST id.
    /// The id is guaranteed to be monotonic increasing.
    pub async fn get_new_sst_object_id(self: &Arc<Self>) -> HummockResult<HummockSstableId> {
        self.map_next_sst_object_id(|available_sst_object_ids| {
            available_sst_object_ids.get_next_sst_object_id()
        })
        .await
    }

    /// Executes `f` with next SST id.
    /// May fetch new SST ids via RPC.
    async fn map_next_sst_object_id<F>(self: &Arc<Self>, f: F) -> HummockResult<HummockSstableId>
    where
        F: Fn(&mut SstObjectIdRange) -> Option<HummockSstableId>,
    {
        loop {
            // 1. Try to get
            if let Some(new_id) = f(self.available_sst_object_ids.lock().deref_mut()) {
                return Ok(new_id);
            }
            // 2. Otherwise either fetch new ids, or wait for previous fetch if any.
            let waiter = {
                let mut guard = self.wait_queue.lock();
                if let Some(new_id) = f(self.available_sst_object_ids.lock().deref_mut()) {
                    return Ok(new_id);
                }
                let wait_queue = guard.deref_mut();
                if let Some(wait_queue) = wait_queue {
                    let (tx, rx) = oneshot::channel();
                    wait_queue.push(tx);
                    Some(rx)
                } else {
                    *wait_queue = Some(vec![]);
                    None
                }
            };
            if let Some(waiter) = waiter {
                // Wait for previous fetch
                sync_point!("MAP_NEXT_SST_OBJECT_ID.AS_FOLLOWER");
                let _ = waiter.await;
                continue;
            }
            // Fetch new ids.
            sync_point!("MAP_NEXT_SST_OBJECT_ID.AS_LEADER");
            sync_point!("MAP_NEXT_SST_OBJECT_ID.BEFORE_FETCH");
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
                        this.notify_waiters(false);
                        return Err(err);
                    }
                };
                sync_point!("MAP_NEXT_SST_OBJECT_ID.AFTER_FETCH");
                sync_point!("MAP_NEXT_SST_OBJECT_ID.BEFORE_FILL_CACHE");
                // Update local cache.
                let result = {
                    let mut guard = this.available_sst_object_ids.lock();
                    let available_sst_object_ids = guard.deref_mut();
                    if new_sst_ids.start_id < available_sst_object_ids.end_id {
                        Err(HummockError::meta_error(format!(
                            "SST id moves backwards. new {} < old {}",
                            new_sst_ids.start_id, available_sst_object_ids.end_id
                        )))
                    } else {
                        *available_sst_object_ids = new_sst_ids;
                        Ok(())
                    }
                };
                this.notify_waiters(result.is_ok());
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
    pub async fn add_watermark_object_id(
        self: &Arc<Self>,
        epoch: Option<HummockEpoch>,
    ) -> HummockResult<TrackerId> {
        let tracker_id = match epoch {
            None => self.object_id_tracker.get_next_auto_tracker_id(),
            Some(epoch) => TrackerId::Epoch(epoch),
        };
        let next_sst_object_id = self
            .map_next_sst_object_id(|available_sst_object_ids| {
                available_sst_object_ids.peek_next_sst_object_id()
            })
            .await?;
        self.object_id_tracker
            .add_tracker(tracker_id, next_sst_object_id);
        Ok(tracker_id)
    }

    pub fn remove_watermark_object_id(&self, tracker_id: TrackerId) {
        self.object_id_tracker.remove_tracker(tracker_id);
    }

    /// Returns GC watermark. It equals
    /// - min(effective watermarks), if number of effective watermarks > 0.
    /// - `HummockSstableId::MAX`, if no effective watermark.
    pub fn global_watermark_object_id(&self) -> HummockSstableId {
        self.object_id_tracker
            .tracking_object_ids()
            .into_iter()
            .min()
            .unwrap_or(HummockSstableId::MAX)
    }

    fn notify_waiters(&self, success: bool) {
        let mut guard = self.wait_queue.lock();
        let wait_queue = guard.deref_mut().take().unwrap();
        for notify in wait_queue {
            let _ = notify.send(success);
        }
    }
}

#[async_trait::async_trait]
impl ExtraInfoSource for SstableObjectIdManager {
    async fn get_extra_info(&self) -> Option<Info> {
        Some(Info::HummockGcWatermark(self.global_watermark_object_id()))
    }
}

type AutoTrackerId = u64;

#[derive(Eq, Hash, PartialEq, Copy, Clone, Debug)]
pub enum TrackerId {
    Auto(AutoTrackerId),
    Epoch(HummockEpoch),
}

/// `SstObjectIdTracker` tracks a min(SST object id) for various caller, identified by a
/// `TrackerId`.
pub struct SstObjectIdTracker {
    auto_id: AtomicU64,
    inner: parking_lot::RwLock<SstObjectIdTrackerInner>,
}

impl SstObjectIdTracker {
    fn new() -> Self {
        Self {
            auto_id: Default::default(),
            inner: parking_lot::RwLock::new(SstObjectIdTrackerInner::new()),
        }
    }

    /// Adds a tracker to track `object_id`. If a tracker with `tracker_id` already exists, it will
    /// track the smallest `object_id` ever given.
    fn add_tracker(&self, tracker_id: TrackerId, object_id: HummockSstableId) {
        self.inner.write().add_tracker(tracker_id, object_id);
    }

    /// Removes given `tracker_id`.
    fn remove_tracker(&self, tracker_id: TrackerId) {
        self.inner.write().remove_tracker(tracker_id);
    }

    fn get_next_auto_tracker_id(&self) -> TrackerId {
        TrackerId::Auto(self.auto_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn tracking_object_ids(&self) -> Vec<HummockSstableId> {
        self.inner.read().tracking_object_ids()
    }
}

struct SstObjectIdTrackerInner {
    tracking_object_ids: HashMap<TrackerId, HummockSstableId>,
}

impl SstObjectIdTrackerInner {
    fn new() -> Self {
        Self {
            tracking_object_ids: Default::default(),
        }
    }

    fn add_tracker(&mut self, tracker_id: TrackerId, object_id: HummockSstableId) {
        match self.tracking_object_ids.entry(tracker_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() = cmp::min(*o.get_mut(), object_id);
            }
            Entry::Vacant(v) => {
                v.insert(object_id);
            }
        }
    }

    fn remove_tracker(&mut self, tracker_id: TrackerId) {
        match &tracker_id {
            TrackerId::Auto(_) => {
                self.tracking_object_ids.remove(&tracker_id);
            }
            TrackerId::Epoch(max_epoch) => self.tracking_object_ids.retain(|id, _| match id {
                TrackerId::Auto(_) => true,
                TrackerId::Epoch(epoch) => *epoch > *max_epoch,
            }),
        }
    }

    fn tracking_object_ids(&self) -> Vec<HummockSstableId> {
        self.tracking_object_ids.values().cloned().collect_vec()
    }
}

#[cfg(test)]
mod test {

    use risingwave_common::try_match_expand;

    use crate::hummock::sstable::sstable_object_id_manager::AutoTrackerId;
    use crate::hummock::{SstObjectIdTracker, TrackerId};

    #[tokio::test]
    async fn test_object_id_tracker_basic() {
        let object_id_tacker = SstObjectIdTracker::new();
        assert!(object_id_tacker.tracking_object_ids().is_empty());
        let auto_id =
            try_match_expand!(object_id_tacker.get_next_auto_tracker_id(), TrackerId::Auto)
                .unwrap();
        assert_eq!(auto_id, AutoTrackerId::MIN + 1);

        let auto_id_1 = object_id_tacker.get_next_auto_tracker_id();
        let auto_id_2 = object_id_tacker.get_next_auto_tracker_id();
        let auto_id_3 = object_id_tacker.get_next_auto_tracker_id();

        object_id_tacker.add_tracker(auto_id_1, 10);
        assert_eq!(
            object_id_tacker
                .tracking_object_ids()
                .into_iter()
                .min()
                .unwrap(),
            10
        );

        // OK to move SST id backwards.
        object_id_tacker.add_tracker(auto_id_1, 9);
        object_id_tacker.add_tracker(auto_id_2, 9);

        // OK to add same id to the same tracker
        object_id_tacker.add_tracker(auto_id_1, 10);
        // OK to add same id to another tracker
        object_id_tacker.add_tracker(auto_id_2, 10);

        object_id_tacker.add_tracker(auto_id_3, 20);
        object_id_tacker.add_tracker(auto_id_2, 30);
        // Tracker 1 and 2 both hold id 9.
        assert_eq!(
            object_id_tacker
                .tracking_object_ids()
                .into_iter()
                .min()
                .unwrap(),
            9
        );

        object_id_tacker.remove_tracker(auto_id_1);
        // Tracker 2 still holds 9.
        assert_eq!(
            object_id_tacker
                .tracking_object_ids()
                .into_iter()
                .min()
                .unwrap(),
            9
        );

        object_id_tacker.remove_tracker(auto_id_2);
        assert_eq!(
            object_id_tacker
                .tracking_object_ids()
                .into_iter()
                .min()
                .unwrap(),
            20
        );

        object_id_tacker.remove_tracker(auto_id_3);
        assert!(object_id_tacker.tracking_object_ids().is_empty());
    }
}
