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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{BoxFuture, FutureExt};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Wait for signal {0} timeout")]
    WaitTimeout(&'static str),
}

pub type SyncPoint = &'static str;
type Action = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

static SYNC_FACILITY: spin::Once<SyncFacility> = spin::Once::new();

struct SyncFacility {
    /// `Notify` for each sync point.
    notifies: spin::Mutex<HashMap<SyncPoint, Arc<tokio::sync::Notify>>>,
    /// Actions for each sync point.
    actions: spin::Mutex<HashMap<SyncPoint, Action>>,
}

impl SyncFacility {
    fn new() -> Self {
        Self {
            notifies: Default::default(),
            actions: Default::default(),
        }
    }

    fn get() -> &'static Self {
        SYNC_FACILITY.get().expect("sync point not enabled")
    }

    async fn wait(
        &self,
        sync_point: SyncPoint,
        timeout: Duration,
        relay: bool,
    ) -> Result<(), Error> {
        let entry = self.notifies.lock().entry(sync_point).or_default().clone();
        match tokio::time::timeout(timeout, entry.notified()).await {
            Ok(_) if relay => entry.notify_one(),
            Ok(_) => {}
            Err(_) => return Err(Error::WaitTimeout(sync_point)),
        }
        Ok(())
    }

    fn emit(&self, sync_point: SyncPoint) {
        self.notifies
            .lock()
            .entry(sync_point)
            .or_default()
            .notify_one();
    }

    fn hook(&self, sync_point: SyncPoint, action: Action) {
        self.actions.lock().insert(sync_point, action);
    }

    fn reset(&self) {
        self.actions.lock().clear();
        self.notifies.lock().clear();
    }

    async fn on(&self, sync_point: SyncPoint) {
        let action = self.actions.lock().get(sync_point).map(|action| action());
        if let Some(action) = action {
            action.await;
        }
        self.emit(sync_point);
    }
}

/// Enable or reset the global sync facility.
pub fn reset() {
    SYNC_FACILITY.call_once(SyncFacility::new).reset();
}

/// Mark a sync point.
pub async fn on(sync_point: SyncPoint) {
    if let Some(sync_facility) = SYNC_FACILITY.get() {
        sync_facility.on(sync_point).await;
    }
}

/// Hook a sync point with action.
///
/// The action will be executed before reaching the sync point.
pub fn hook<F, Fut>(sync_point: SyncPoint, action: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let action = Arc::new(move || action().boxed());
    SyncFacility::get().hook(sync_point, action);
}

/// Wait for a sync point to be reached with timeout.
///
/// If the sync point is reached before this call, it will consume this event and return
/// immediately.
pub async fn wait_timeout(sync_point: SyncPoint, dur: Duration) -> Result<(), Error> {
    SyncFacility::get().wait(sync_point, dur, false).await
}
