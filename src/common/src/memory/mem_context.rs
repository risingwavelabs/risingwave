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

pub type MemoryContextRef = Arc<MemoryContext>;

pub struct MemoryContext {
    counter: MemCounter,
    parent: Option<MemoryContextRef>,
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
    pub fn new<C: Into<MemCounter>>(parent: Option<MemoryContextRef>, counter: C) -> Self {
        Self {
            counter: counter.into(),
            parent,
        }
    }

    /// Add `bytes` memory usage. Pass negative value to decrease memory usage.
    pub fn add(&self, bytes: i64) {
        match &self.counter {
            MemCounter::TrAdder(c) => c.add(bytes),
            MemCounter::Atomic(c) => c.add(bytes),
        }

        if let Some(parent) = &self.parent {
            parent.add(bytes);
        }
    }

    pub fn get_bytes_used(&self) -> i64 {
        match &self.counter {
            MemCounter::TrAdder(c) => c.get(),
            MemCounter::Atomic(c) => c.get(),
        }
    }

    pub fn for_test() -> Self {
        Self {
            counter: MemCounter::Atomic(IntGauge::new("test", "test").unwrap()),
            parent: None,
        }
    }
}

impl Drop for MemoryContext {
    fn drop(&mut self) {
        if let Some(p) = &self.parent {
            p.add(-self.get_bytes_used())
        }
    }
}
