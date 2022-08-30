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

use futures::future::BoxFuture;
use futures::FutureExt;

use crate::util::sync_point::Error;

pub type SyncPoint = &'static str;
pub type Signal = &'static str;
type Action = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

lazy_static::lazy_static! {
    static ref SYNC_FACILITY: SyncFacility = SyncFacility::new();
}

struct SyncFacility {
    /// `Notify` for each `Signal`.
    signals: parking_lot::Mutex<HashMap<Signal, Arc<tokio::sync::Notify>>>,
    /// `SyncPointInfo` for active `SyncPoint`.
    sync_points: parking_lot::Mutex<HashMap<SyncPoint, Action>>,
}

impl SyncFacility {
    fn new() -> Self {
        Self {
            signals: Default::default(),
            sync_points: Default::default(),
        }
    }

    async fn wait(
        &self,
        signal: Signal,
        timeout: Duration,
        relay_signal: bool,
    ) -> Result<(), Error> {
        let entry = self.signals.lock().entry(signal).or_default().clone();
        match tokio::time::timeout(timeout, entry.notified()).await {
            Ok(_) if relay_signal => entry.notify_one(),
            Ok(_) => {}
            Err(_) => return Err(Error::WaitForSignalTimeout(signal)),
        }
        Ok(())
    }

    fn emit_signal(&self, signal: Signal) {
        if let Some(notify) = self.signals.lock().get_mut(signal) {
            notify.notify_one();
        }
    }

    fn set_action(&self, sync_point: SyncPoint, action: Action) {
        self.sync_points.lock().insert(sync_point, action);
    }

    fn reset(&self) {
        self.sync_points.lock().clear();
        self.signals.lock().clear();
    }

    async fn on(&self, sync_point: SyncPoint) {
        let action = self
            .sync_points
            .lock()
            .get(sync_point)
            .map(|action| action());
        if let Some(action) = action {
            action.await;
        }
        self.emit_signal(sync_point);
    }
}

/// Activate the sync point forever.
pub fn hook<F, Fut>(sync_point: SyncPoint, action: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let action = Arc::new(move || action().boxed());
    SYNC_FACILITY.set_action(sync_point, action);
}

/// The sync point is triggered
pub async fn on(sync_point: SyncPoint) {
    SYNC_FACILITY.on(sync_point).await;
}

pub async fn wait(signal: Signal, timeout: Duration) -> Result<(), Error> {
    SYNC_FACILITY.wait(signal, timeout, false).await
}

pub fn reset() {
    SYNC_FACILITY.reset();
}
