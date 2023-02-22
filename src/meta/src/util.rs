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

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use delay_timer::prelude::*;
use futures::future::BoxFuture;
use tokio::task::JoinHandle;

pub struct GlobalEventManager {
    timer: DelayTimer,
    task_id: AtomicU64,
    shutdown_callbacks: Vec<BoxFuture<'static, ()>>,
    handles: Vec<JoinHandle<()>>,
}

impl Default for GlobalEventManager {
    fn default() -> Self {
        let timer = DelayTimerBuilder::default()
            .tokio_runtime_by_default()
            .build();
        Self {
            timer,
            task_id: AtomicU64::new(0),
            shutdown_callbacks: vec![],
            handles: vec![],
        }
    }
}

impl GlobalEventManager {
    pub fn register_interval_task<F, U>(&self, interval_sec: u64, f: F)
    where
        F: Fn() -> U + 'static + Send,
        U: Future + 'static + Send,
    {
        let mut task_builder = TaskBuilder::default();
        let task_id = self.task_id.fetch_add(1, Ordering::SeqCst);
        let task = task_builder
            .set_task_id(task_id)
            .set_frequency_once_by_seconds(interval_sec)
            .spawn_async_routine(f)
            .unwrap();
        self.timer.add_task(task).unwrap();
    }

    pub fn register_shutdown_task<F: Future<Output = ()> + Send + 'static>(
        &mut self,
        f: F,
        handle: JoinHandle<()>,
    ) {
        self.shutdown_callbacks.push(Box::pin(f));
        self.handles.push(handle);
    }

    pub fn register_shutdown_callback<F: Future<Output = ()> + Send + 'static>(&mut self, f: F) {
        self.shutdown_callbacks.push(Box::pin(f));
    }

    pub async fn shutdown(self) {
        let _ = self.timer.stop_delay_timer();
        for callback in self.shutdown_callbacks {
            callback.await;
        }

        for handle in self.handles {
            // The barrier manager can't be shutdown gracefully if it's under recovering, try to
            // abort it using timeout.
            match tokio::time::timeout(Duration::from_secs(1), handle).await {
                Ok(Err(err)) => {
                    tracing::warn!("Failed to join shutdown: {:?}", err);
                }
                Err(e) => {
                    tracing::warn!("Join shutdown timeout: {:?}", e);
                }
                _ => {}
            }
        }
    }
}
