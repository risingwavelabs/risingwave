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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

/// `IdleManager` keeps track of latest activity and report whether the meta service has been
/// idle for long time.
pub struct IdleManager {
    config_max_idle_ms: u64, // Idle manager will not work if set to 0
    instant_base: Instant,   // An arbitrary base, used to convert new instants into u64 secs.
    last_active_offset_ms: AtomicU64,
}

pub type IdleManagerRef = Arc<IdleManager>;

impl IdleManager {
    pub fn disabled() -> Self {
        Self::new(0)
    }

    pub fn new(config_max_idle_ms: u64) -> Self {
        IdleManager {
            config_max_idle_ms,
            instant_base: Instant::now(),
            last_active_offset_ms: AtomicU64::new(0),
        }
    }

    pub fn get_config_max_idle(&self) -> Duration {
        Duration::from_millis(self.config_max_idle_ms)
    }

    fn offset_ms_now(&self) -> u64 {
        let now = Instant::now();
        if now <= self.instant_base {
            return 0;
        }
        ((now - self.instant_base).as_secs_f64() * 1000.0) as u64
    }

    pub fn record_activity(&self) {
        self.last_active_offset_ms
            .store(self.offset_ms_now(), Ordering::Release);
    }

    pub fn is_exceeding_max_idle(&self) -> bool {
        if self.config_max_idle_ms == 0 {
            return false;
        }
        let new_offset_ms = self.offset_ms_now();
        let last_offset_ms = self.last_active_offset_ms.load(Ordering::Acquire);
        if new_offset_ms < last_offset_ms {
            // Should never happen normally, but in some arch it may happen.
            // In this case, let's do nothing..
            return false;
        }
        (new_offset_ms - last_offset_ms) > self.config_max_idle_ms
    }

    /// Idle checker send signal when the meta does not receive requests for long time.
    pub async fn start_idle_checker(
        idle_manager: IdleManagerRef,
        check_interval: Duration,
        idle_send: tokio::sync::oneshot::Sender<()>,
    ) -> (JoinHandle<()>, Sender<()>) {
        let dur = idle_manager.get_config_max_idle();
        if !dur.is_zero() {
            tracing::warn!("--dangerous-max-idle-secs is set. The meta server will be automatically stopped after idle for {:?}.", dur)
        }

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    _ = min_interval.tick() => {},
                    _ = &mut shutdown_rx => {
                        tracing::info!("Idle checker is stopped");
                        return;
                    }
                }
                if idle_manager.is_exceeding_max_idle() {
                    break;
                }
            }
            tracing::warn!(
                "Idle checker found the server is already idle for {:?}",
                idle_manager.get_config_max_idle()
            );
            tracing::warn!("Idle checker is shutting down the server");
            let _ = idle_send.send(());
        });
        (join_handle, shutdown_tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_idle_manager() {
        let im = IdleManager::new(400);
        assert!(!im.is_exceeding_max_idle());
        im.record_activity();
        assert!(!im.is_exceeding_max_idle());

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert!(im.is_exceeding_max_idle());
        im.record_activity();
        assert!(!im.is_exceeding_max_idle());

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        assert!(im.is_exceeding_max_idle());
        im.record_activity();
        assert!(!im.is_exceeding_max_idle());

        let im = IdleManager::disabled();
        assert!(!im.is_exceeding_max_idle());
        im.record_activity();
        assert!(!im.is_exceeding_max_idle());
    }
}
