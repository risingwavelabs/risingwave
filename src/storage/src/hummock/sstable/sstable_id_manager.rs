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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_hummock_sdk::{HummockEpoch, HummockSstableId, SstIdRange};
use risingwave_pb::meta::heartbeat_request::extra_info::Info;
use risingwave_rpc_client::{ExtraInfoSource, HummockMetaClient};
use tokio::sync::{Notify, RwLock};

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
    pub async fn get_new_sst_id(&self) -> HummockResult<HummockSstableId> {
        self.map_next_sst_id(|available_sst_ids| available_sst_ids.get_next_sst_id())
            .await
    }

    /// Executes `f` with next SST id.
    /// May fetch new SST ids via RPC.
    async fn map_next_sst_id<F>(&self, f: F) -> HummockResult<HummockSstableId>
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
                notify.notified().await;
                notify.notify_one();
                continue;
            }
            // Fetch new ids.
            let new_sst_ids = match self
                .hummock_meta_client
                .get_new_sst_ids(self.remote_fetch_number)
                .await
                .map_err(HummockError::meta_error)
            {
                Ok(new_sst_ids) => new_sst_ids,
                Err(err) => {
                    self.notifier.lock().take().unwrap().notify_one();
                    return Err(err);
                }
            };
            let err = {
                let mut guard = self.available_sst_ids.lock();
                let available_sst_ids = guard.deref_mut();
                let mut err = None;
                if new_sst_ids.start_id < available_sst_ids.end_id {
                    err = Some(Err(HummockError::meta_error(format!(
                        "SST id moves backwards. new {} < old {}",
                        new_sst_ids.start_id, available_sst_ids.end_id
                    ))));
                } else {
                    *available_sst_ids = new_sst_ids;
                }
                err
            };
            self.notifier.lock().take().unwrap().notify_one();
            if let Some(err) = err {
                return err;
            }
        }
    }

    /// Adds a new watermark SST id using the next unused SST id.
    /// Returns a tracker id. It's used to remove the watermark later.
    /// - Uses given 'epoch' as tracker id if provided.
    /// - Uses a generated tracker id otherwise.
    pub async fn add_watermark_sst_id(
        &self,
        epoch: Option<HummockEpoch>,
    ) -> HummockResult<TrackerId> {
        let tracker_id = match epoch {
            None => self.sst_id_tracker.get_next_auto_tracker_id(),
            Some(epoch) => TrackerId::Epoch(epoch),
        };
        self.sst_id_tracker
            .add_tracker(tracker_id, async {
                self.map_next_sst_id(|available_sst_ids| available_sst_ids.peek_next_sst_id())
                    .await
            })
            .await?;
        Ok(tracker_id)
    }

    pub async fn remove_watermark_sst_id(&self, tracker_id: TrackerId) {
        self.sst_id_tracker.remove_tracker(tracker_id).await;
    }

    /// Returns GC watermark. It equals
    /// - min(effective watermarks), if number of effective watermarks > 0.
    /// - `HummockSstableId::MAX`, if no effective watermark.
    pub async fn global_watermark_sst_id(&self) -> HummockSstableId {
        self.sst_id_tracker
            .tracking_sst_ids()
            .await
            .into_iter()
            .min()
            .unwrap_or(HummockSstableId::MAX)
    }
}

#[async_trait::async_trait]
impl ExtraInfoSource for SstableIdManager {
    async fn get_extra_info(&self) -> Option<Info> {
        Some(Info::HummockGcWatermark(
            self.global_watermark_sst_id().await,
        ))
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
    inner: RwLock<SstIdTrackerInner>,
}

impl SstIdTracker {
    fn new() -> Self {
        Self {
            auto_id: Default::default(),
            inner: RwLock::new(SstIdTrackerInner::new()),
        }
    }

    /// Adds a tracker to track SST id given by `get_sst_id`.
    ///
    /// Consecutive `get_sst_id` must return non-decreasing SST ids.
    async fn add_tracker<F>(&self, tracker_id: TrackerId, get_sst_id: F) -> HummockResult<()>
    where
        F: Future<Output = HummockResult<HummockSstableId>>,
    {
        let mut guard = self.inner.write().await;
        let sst_id = get_sst_id.await?;
        guard.add_tracker(tracker_id, sst_id)
    }

    /// Removes given `tracker_id`.
    async fn remove_tracker(&self, tracker_id: TrackerId) {
        self.inner.write().await.remove_tracker(tracker_id);
    }

    fn get_next_auto_tracker_id(&self) -> TrackerId {
        TrackerId::Auto(self.auto_id.fetch_add(1, Ordering::Relaxed) + 1)
    }

    async fn tracking_sst_ids(&self) -> Vec<HummockSstableId> {
        self.inner.read().await.tracking_sst_ids()
    }
}

struct SstIdTrackerInner {
    tracking_sst_ids: HashMap<TrackerId, HummockSstableId>,
    /// Used for sanity check
    max_seen_sst_id: HummockSstableId,
}

impl SstIdTrackerInner {
    fn new() -> Self {
        Self {
            tracking_sst_ids: Default::default(),
            max_seen_sst_id: HummockSstableId::MIN,
        }
    }

    /// Consecutive `add_tracker` must be given non-decreasing `sst_id`,
    /// so that min(`tracking_sst_ids`) never moves backwards.
    fn add_tracker(
        &mut self,
        tracker_id: TrackerId,
        sst_id: HummockSstableId,
    ) -> HummockResult<()> {
        if sst_id < self.max_seen_sst_id {
            return Err(HummockError::sst_id_tracker_error(
                "Tracker's SST id moves backwards",
            ));
        }
        self.max_seen_sst_id = sst_id;
        match self.tracking_sst_ids.entry(tracker_id) {
            Entry::Occupied(o) => {
                if sst_id < *o.get() {
                    return Err(HummockError::sst_id_tracker_error(format!(
                        "Tracker's SST id moves backwards in {:#?}",
                        o.key()
                    )));
                    // Just track the minimum one.
                }
            }
            Entry::Vacant(v) => {
                v.insert(sst_id);
            }
        }

        Ok(())
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
        assert!(sst_id_tacker.tracking_sst_ids().await.is_empty());
        let auto_id =
            try_match_expand!(sst_id_tacker.get_next_auto_tracker_id(), TrackerId::Auto).unwrap();
        assert_eq!(auto_id, AutoTrackerId::MIN + 1);

        let auto_id_1 = sst_id_tacker.get_next_auto_tracker_id();
        let auto_id_2 = sst_id_tacker.get_next_auto_tracker_id();
        let auto_id_3 = sst_id_tacker.get_next_auto_tracker_id();

        sst_id_tacker
            .add_tracker(auto_id_1, async { Ok(10) })
            .await
            .unwrap();
        assert_eq!(
            sst_id_tacker
                .tracking_sst_ids()
                .await
                .into_iter()
                .min()
                .unwrap(),
            10
        );

        // Fails due to SST id moving backwards.
        sst_id_tacker
            .add_tracker(auto_id_1, async { Ok(9) })
            .await
            .unwrap_err();
        sst_id_tacker
            .add_tracker(auto_id_2, async { Ok(9) })
            .await
            .unwrap_err();

        // OK to add same id to the same tracker
        sst_id_tacker
            .add_tracker(auto_id_1, async { Ok(10) })
            .await
            .unwrap();
        // OK to add same id to another tracker
        sst_id_tacker
            .add_tracker(auto_id_2, async { Ok(10) })
            .await
            .unwrap();

        sst_id_tacker
            .add_tracker(auto_id_3, async { Ok(20) })
            .await
            .unwrap();
        sst_id_tacker
            .add_tracker(auto_id_2, async { Ok(30) })
            .await
            .unwrap();
        // Tracker 1 and 2 both hold id 10.
        assert_eq!(
            sst_id_tacker
                .tracking_sst_ids()
                .await
                .into_iter()
                .min()
                .unwrap(),
            10
        );

        sst_id_tacker.remove_tracker(auto_id_1).await;
        // Tracker 2 still holds 10.
        assert_eq!(
            sst_id_tacker
                .tracking_sst_ids()
                .await
                .into_iter()
                .min()
                .unwrap(),
            10
        );

        sst_id_tacker.remove_tracker(auto_id_2).await;
        assert_eq!(
            sst_id_tacker
                .tracking_sst_ids()
                .await
                .into_iter()
                .min()
                .unwrap(),
            20
        );

        sst_id_tacker.remove_tracker(auto_id_3).await;
        assert!(sst_id_tacker.tracking_sst_ids().await.is_empty());
    }
}
