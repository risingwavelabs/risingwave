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

use tokio::sync::{OnceCell, Semaphore, SemaphorePermit, TryAcquireError};

static KAFKA_FD_CONTROL: OnceCell<Arc<Semaphore>> = OnceCell::const_new();

#[derive(thiserror::Error, Debug)]
pub enum FdControlError {
    #[error("allocation error: expect {0}, got {1} available.")]
    AllocationError(usize, usize),
    #[error("semaphore is not available")]
    NotAvailable,
}

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

fn try_reserve_fds(permits: u32) -> Result<SemaphorePermit<'static>, FdControlError> {
    if let Some(semaphore) = KAFKA_FD_CONTROL.get() {
        match semaphore.try_acquire_many(permits) {
            Ok(permit) => {
                tracing::debug!(
                    "successfully reserved {} fd permits, available {}",
                    permits,
                    semaphore.available_permits()
                );
                Ok(permit)
            }
            Err(TryAcquireError::NoPermits) => Err(FdControlError::AllocationError(
                permits as usize,
                semaphore.available_permits(),
            )),
            Err(TryAcquireError::Closed) => Err(FdControlError::NotAvailable),
        }
    } else {
        Err(FdControlError::NotAvailable)
    }
}

pub fn try_reserve_fds_source(
    broker_num: usize,
) -> Result<SemaphorePermit<'static>, FdControlError> {
    try_reserve_fds((3 * broker_num + 2) as u32)
}
