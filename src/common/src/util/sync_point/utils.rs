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
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;

use crate::util::sync_point::Error;

pub type SyncPoint = &'static str;
pub type Signal = &'static str;
type Action = Arc<dyn Fn() -> BoxFuture<'static, Result<(), Error>> + Send + Sync>;

lazy_static::lazy_static! {
    static ref SYNC_FACILITY: SyncFacility = SyncFacility::new();
}

/// A `SyncPoint` is activated by attaching a `SyncPointInfo` to it.
struct SyncPointInfo {
    /// `Action`s to be executed when `SyncPoint` is triggered.
    action: Action,
    /// The `SyncPoint` is deactivated after triggered `execute_times`.
    execute_times: u64,
}

struct SyncFacility {
    /// `Notify` for each `Signal`.
    signals: parking_lot::Mutex<HashMap<Signal, Arc<tokio::sync::Notify>>>,
    /// `SyncPointInfo` for active `SyncPoint`.
    sync_points: parking_lot::Mutex<HashMap<SyncPoint, SyncPointInfo>>,
}

impl SyncFacility {
    fn new() -> Self {
        Self {
            signals: Default::default(),
            sync_points: Default::default(),
        }
    }

    async fn wait_for_signal(
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

    fn set_action(&self, sync_point: SyncPoint, action: Action, execute_times: u64) {
        if execute_times == 0 {
            return;
        }
        self.sync_points.lock().insert(
            sync_point,
            SyncPointInfo {
                action,
                execute_times,
            },
        );
    }

    fn reset_action(&self, sync_point: SyncPoint) {
        self.sync_points.lock().remove(sync_point);
    }

    async fn on(&self, sync_point: SyncPoint) {
        let action = {
            let mut guard = self.sync_points.lock();
            match guard.entry(sync_point) {
                Entry::Occupied(mut o) => {
                    if o.get().execute_times == 1 {
                        // Deactivate the sync point and execute its actions for the last time.
                        (o.remove().action)()
                    } else {
                        o.get_mut().execute_times -= 1;
                        (o.get().action)()
                    }
                }
                Entry::Vacant(_) => return,
            }
        };
        action.await.unwrap();
    }
}

/// Activate the sync point forever.
pub fn activate<F, Fut>(sync_point: SyncPoint, action: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + Sync + 'static,
{
    activate_n(sync_point, u64::MAX, action);
}

/// Activate the sync point once.
pub fn activate_once<F, Fut>(sync_point: SyncPoint, action: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + Sync + 'static,
{
    activate_n(sync_point, 1, action);
}

/// Activate the sync point and reset after executed `n` times.
pub fn activate_n<F, Fut>(sync_point: SyncPoint, n: u64, action: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Error>> + Send + Sync + 'static,
{
    let action = Arc::new(move || action().boxed());
    SYNC_FACILITY.set_action(sync_point, action, n);
}

/// Deactivate the sync point.
pub fn deactivate(sync_point: SyncPoint) {
    SYNC_FACILITY.reset_action(sync_point);
}

/// The sync point is triggered
pub async fn on(sync_point: SyncPoint) {
    SYNC_FACILITY.on(sync_point).await;
}

pub async fn wait_for_signal(signal: Signal, timeout: Duration) -> Result<(), Error> {
    SYNC_FACILITY.wait_for_signal(signal, timeout, false).await
}

pub async fn emit_signal(signal: Signal) -> Result<(), Error> {
    SYNC_FACILITY.emit_signal(signal);
    Ok(())
}
