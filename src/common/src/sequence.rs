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

use std::sync::atomic::{AtomicU64, Ordering};

pub type Sequence = u64;
pub type AtomicSequence = AtomicU64;

pub static SEQUENCE_GLOBAL: AtomicSequence = AtomicSequence::new(0);

/// An globally unique and approximate ascending sequence generator with local optimization.
///
/// [`Sequencer`] can be used to generate globally unique sequence (id) in an approximate order. [`Sequencer`] allow
/// the generated sequence to be disordered in a certain window. The larger the allowed disordered sequence window is,
/// the better multithreading performance of the generator will be.
///
/// The window is controlled with two arguments, `step` and `lag`. `step` controls the size of the batch of the
/// sequences to allocate by the local sequence generator. `lag` controls the maximum lag between the local generator
/// and the global generator to avoid skew.
pub struct Sequencer {
    local: Sequence,
    target: Sequence,

    step: Sequence,
    lag: Sequence,
}

impl Sequencer {
    pub const DEFAULT_LAG: Sequence = Self::DEFAULT_STEP * 32;
    pub const DEFAULT_STEP: Sequence = 64;

    /// Create a new local sequence generator.
    pub const fn new(step: Sequence, lag: Sequence) -> Self {
        Self {
            local: 0,
            target: 0,
            step,
            lag,
        }
    }

    /// Get the global sequence to allocate.
    pub fn global(&self) -> Sequence {
        SEQUENCE_GLOBAL.load(Ordering::Relaxed)
    }

    /// Get the local sequence to allocate.
    pub fn local(&self) -> Sequence {
        self.local
    }

    /// Allocate a new sequence. The allocated sequences from the same [`Sequencer`] are strictly ascending, the
    /// allocated sequences from different [`Sequencer`]s are approximate ascending.
    pub fn alloc(&mut self) -> Sequence {
        self.try_alloc();
        let res = self.local;
        self.local += 1;
        res
    }

    #[inline(always)]
    fn try_alloc(&mut self) {
        if self.local == self.target
            || self.local + self.lag < SEQUENCE_GLOBAL.load(Ordering::Relaxed)
        {
            self.alloc_inner()
        }
    }

    #[inline(always)]
    fn alloc_inner(&mut self) {
        self.local = SEQUENCE_GLOBAL.fetch_add(self.step, Ordering::Relaxed);
        self.target = self.local + self.step;
    }
}
