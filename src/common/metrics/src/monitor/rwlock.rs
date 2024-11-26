// Copyright 2024 RisingWave Labs
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

use prometheus::HistogramVec;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct MonitoredRwLock<T> {
    // labels: [lock_name, lock_type]
    metrics: HistogramVec,
    inner: RwLock<T>,
    lock_name: &'static str,
}

impl<T> MonitoredRwLock<T> {
    pub fn new(metrics: HistogramVec, val: T, lock_name: &'static str) -> Self {
        Self {
            metrics,
            inner: RwLock::new(val),
            lock_name,
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        let _timer = self
            .metrics
            .with_label_values(&[self.lock_name, "read"])
            .start_timer();
        self.inner.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        let _timer = self
            .metrics
            .with_label_values(&[self.lock_name, "write"])
            .start_timer();
        self.inner.write().await
    }
}
