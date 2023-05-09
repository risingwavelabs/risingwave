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

use std::sync::Arc;

use prometheus::IntGauge;

use crate::metrics::TrAdderGauge;

struct MemoryContextInner {
    counter: MemCounter,
    parent: Option<MemoryContext>,
}

#[derive(Default, Clone)]
pub struct MemoryContext {
    /// Add None op mem context, so that we don't need to return [`Option`] in
    /// [`BatchTaskContext`]. This helps with later `Allocator` implementation.
    inner: Option<Arc<MemoryContextInner>>,
}

pub enum MemCounter {
    /// Used when the add/sub operation don't have much conflicts.
    Atomic(IntGauge),
    /// Used when the add/sub operation may cause a lot of conflicts.
    TrAdder(TrAdderGauge),
}

impl From<IntGauge> for MemCounter {
    fn from(value: IntGauge) -> Self {
        MemCounter::Atomic(value)
    }
}

impl From<TrAdderGauge> for MemCounter {
    fn from(value: TrAdderGauge) -> Self {
        MemCounter::TrAdder(value)
    }
}

impl MemoryContext {
    pub fn new<C: Into<MemCounter>>(parent: Option<MemoryContext>, counter: C) -> Self {
        Self {
            inner: Some(Arc::new(MemoryContextInner {
                counter: counter.into(),
                parent,
            })),
        }
    }

    /// Add `bytes` memory usage. Pass negative value to decrease memory usage.
    pub fn add(&self, bytes: i64) {
        if let Some(inner) = &self.inner {
            match &inner.counter {
                MemCounter::TrAdder(c) => c.add(bytes),
                MemCounter::Atomic(c) => c.add(bytes),
            }

            if let Some(parent) = &inner.parent {
                parent.add(bytes);
            }
        }
    }

    pub fn get_bytes_used(&self) -> i64 {
        if let Some(inner) = &self.inner {
            match &inner.counter {
                MemCounter::TrAdder(c) => c.get(),
                MemCounter::Atomic(c) => c.get(),
            }
        } else {
            0
        }
    }
}

impl Drop for MemoryContext {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            if let Some(p) = &inner.parent {
                p.add(-self.get_bytes_used())
            }
        }
    }
}
