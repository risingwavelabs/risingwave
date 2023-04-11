use std::sync::Arc;

use prometheus::IntGauge;

use crate::metrics::TrAdderGauage;

pub type MemoryContextRef = Arc<MemoryContext>;

pub struct MemoryContext {
    counter: MemCounter,
    parent: Option<MemoryContextRef>,
}

pub enum MemCounter {
    /// Used when the add/sub operation don't have much conflicts.
    Atomic(IntGauge),
    /// Used when the add/sub operation may cause a lot of conflicts.
    TrAdder(TrAdderGauage),
}

impl From<IntGauge> for MemCounter {
    fn from(value: IntGauge) -> Self {
        MemCounter::Atomic(value)
    }
}

impl From<TrAdderGauage> for MemCounter {
    fn from(value: TrAdderGauage) -> Self {
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
