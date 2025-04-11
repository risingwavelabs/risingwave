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

use std::ops::Deref;
use std::sync::Arc;

use prometheus::core::Atomic;
use risingwave_common_metrics::TrAdderAtomic;

use super::MonitoredGlobalAlloc;
use crate::metrics::{LabelGuardedIntGauge, TrAdderGauge};

pub trait MemCounter: Send + Sync + 'static {
    fn add(&self, bytes: i64);
    fn get_bytes_used(&self) -> i64;
}

impl MemCounter for TrAdderGauge {
    fn add(&self, bytes: i64) {
        self.add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

impl MemCounter for TrAdderAtomic {
    fn add(&self, bytes: i64) {
        self.inc_by(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

impl MemCounter for LabelGuardedIntGauge {
    fn add(&self, bytes: i64) {
        self.deref().add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

struct MemoryContextInner {
    counter: Box<dyn MemCounter>,
    parent: Option<MemoryContext>,
    mem_limit: u64,
}

#[derive(Clone)]
pub struct MemoryContext {
    /// Add None op mem context, so that we don't need to return [`Option`] in
    /// `BatchTaskContext`. This helps with later `Allocator` implementation.
    inner: Option<Arc<MemoryContextInner>>,
}

impl MemoryContext {
    pub fn new(parent: Option<MemoryContext>, counter: impl MemCounter) -> Self {
        let mem_limit = parent.as_ref().map_or_else(|| u64::MAX, |p| p.mem_limit());
        Self::new_with_mem_limit(parent, counter, mem_limit)
    }

    pub fn new_with_mem_limit(
        parent: Option<MemoryContext>,
        counter: impl MemCounter,
        mem_limit: u64,
    ) -> Self {
        let c = Box::new(counter);
        Self {
            inner: Some(Arc::new(MemoryContextInner {
                counter: c,
                parent,
                mem_limit,
            })),
        }
    }

    /// Creates a noop memory context.
    pub fn none() -> Self {
        Self { inner: None }
    }

    pub fn root(counter: impl MemCounter, mem_limit: u64) -> Self {
        Self::new_with_mem_limit(None, counter, mem_limit)
    }

    pub fn for_spill_test() -> Self {
        Self::new_with_mem_limit(None, TrAdderAtomic::new(0), 0)
    }

    /// Add `bytes` memory usage. Pass negative value to decrease memory usage.
    /// Returns `false` if the memory usage exceeds the limit.
    pub fn add(&self, bytes: i64) -> bool {
        if let Some(inner) = &self.inner {
            if (inner.counter.get_bytes_used() + bytes) as u64 > inner.mem_limit {
                return false;
            }
            if let Some(parent) = &inner.parent {
                if parent.add(bytes) {
                    inner.counter.add(bytes);
                } else {
                    return false;
                }
            } else {
                inner.counter.add(bytes);
            }
        }
        true
    }

    pub fn get_bytes_used(&self) -> i64 {
        if let Some(inner) = &self.inner {
            inner.counter.get_bytes_used()
        } else {
            0
        }
    }

    pub fn mem_limit(&self) -> u64 {
        if let Some(inner) = &self.inner {
            inner.mem_limit
        } else {
            u64::MAX
        }
    }

    /// Check if the memory usage exceeds the limit.
    /// Returns `false` if the memory usage exceeds the limit.
    pub fn check_memory_usage(&self) -> bool {
        if let Some(inner) = &self.inner {
            if inner.counter.get_bytes_used() as u64 > inner.mem_limit {
                return false;
            }
            if let Some(parent) = &inner.parent {
                return parent.check_memory_usage();
            }
        }

        true
    }

    /// Creates a new global allocator that reports memory usage to this context.
    pub fn global_allocator(&self) -> MonitoredGlobalAlloc {
        MonitoredGlobalAlloc::with_memory_context(self.clone())
    }
}

impl Drop for MemoryContextInner {
    fn drop(&mut self) {
        if let Some(p) = &self.parent {
            p.add(-self.counter.get_bytes_used());
        }
    }
}
