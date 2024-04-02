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

pub struct Sequencer {
    local: Sequence,
    target: Sequence,

    step: Sequence,
    lag: Sequence,
}

impl Sequencer {
    pub const DEFAULT_LAG: Sequence = Self::DEFAULT_STEP * 32;
    pub const DEFAULT_STEP: Sequence = 64;

    pub const fn new(step: Sequence, lag: Sequence) -> Self {
        Self {
            local: 0,
            target: 0,
            step,
            lag,
        }
    }

    pub fn global(&self) -> Sequence {
        SEQUENCE_GLOBAL.load(Ordering::Relaxed)
    }

    pub fn local(&self) -> Sequence {
        self.local
    }

    pub fn inc(&mut self) -> Sequence {
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
            self.alloc()
        }
    }

    #[inline(always)]
    fn alloc(&mut self) {
        self.local = SEQUENCE_GLOBAL.fetch_add(self.step, Ordering::Relaxed);
        self.target = self.local + self.step;
    }
}
