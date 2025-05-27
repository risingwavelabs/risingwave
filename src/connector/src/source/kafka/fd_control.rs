// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use tokio::sync::{OnceCell, Semaphore, SemaphorePermit};

static KAFKA_FD_CONTROL: OnceCell<Arc<Semaphore>> = OnceCell::const_new();

pub fn init_kafka_fd_control(initial_count: usize) {
    let semaphore = Arc::new(Semaphore::new(initial_count));
    match KAFKA_FD_CONTROL.set(semaphore) {
        Ok(()) => {
            tracing::info!("Kafka FD quota initialized with {} permits", initial_count);
        }
        Err(e) => {
            tracing::warn!("get fd quota failed, maybe already initialized: {:?}", e);
        }
    }
}

pub async fn try_reserve_fds(permits: u32) -> Option<SemaphorePermit<'static>> {
    let x = KAFKA_FD_CONTROL.get().unwrap();
    x.try_acquire_many(permits).ok()
}
