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

use std::ops::{Deref, DerefMut};
use std::time::Instant;

use prometheus::{Histogram, HistogramVec};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const UNNAMED_PROCESS: &str = "unnamed";

pub struct MonitoredRwLock<T> {
    // labels: [lock_name, lock_type]
    metrics: HistogramVec,
    // labels: [method, lock_name]
    process_metrics: HistogramVec,
    inner: RwLock<T>,
    lock_name: &'static str,
}

impl<T> MonitoredRwLock<T> {
    pub fn new(
        metrics: HistogramVec,
        process_metrics: HistogramVec,
        val: T,
        lock_name: &'static str,
    ) -> Self {
        Self {
            metrics,
            process_metrics,
            inner: RwLock::new(val),
            lock_name,
        }
    }

    pub async fn read(&self) -> MonitoredRwLockGuard<RwLockReadGuard<'_, T>> {
        self.read_with_process_name(UNNAMED_PROCESS).await
    }

    pub async fn write(&self) -> MonitoredRwLockGuard<RwLockWriteGuard<'_, T>> {
        self.write_with_process_name(UNNAMED_PROCESS).await
    }

    pub async fn read_with_process_name(
        &self,
        func_name: &'static str,
    ) -> MonitoredRwLockGuard<RwLockReadGuard<'_, T>> {
        let _timer = self
            .metrics
            .with_label_values(&[self.lock_name, "read"])
            .start_timer();
        let guard = self.inner.read().await;
        MonitoredRwLockGuard::new(guard, self.process_metric(func_name))
    }

    pub async fn write_with_process_name(
        &self,
        func_name: &'static str,
    ) -> MonitoredRwLockGuard<RwLockWriteGuard<'_, T>> {
        let _timer = self
            .metrics
            .with_label_values(&[self.lock_name, "write"])
            .start_timer();
        let guard = self.inner.write().await;
        MonitoredRwLockGuard::new(guard, self.process_metric(func_name))
    }

    fn process_metric(&self, func_name: &'static str) -> Histogram {
        self.process_metrics
            .with_label_values(&[func_name, self.lock_name])
    }
}

pub struct MonitoredRwLockGuard<G> {
    guard: G,
    process_metric: Histogram,
    process_start: Instant,
}

impl<G> MonitoredRwLockGuard<G> {
    fn new(guard: G, process_metric: Histogram) -> Self {
        Self {
            guard,
            process_metric,
            process_start: Instant::now(),
        }
    }
}

impl<G> Deref for MonitoredRwLockGuard<G>
where
    G: Deref,
{
    type Target = G::Target;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<G> DerefMut for MonitoredRwLockGuard<G>
where
    G: DerefMut,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<G> Drop for MonitoredRwLockGuard<G> {
    fn drop(&mut self) {
        self.process_metric
            .observe(self.process_start.elapsed().as_secs_f64());
    }
}
