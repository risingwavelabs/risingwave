// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::fmt;

use quanta::Clock;

lazy_static::lazy_static! {
    static ref EPOCH_CLOCK: Clock = Clock::new();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// `INVALID_EPOCH` defines the invalid epoch value.
pub const INVALID_EPOCH: u64 = 0;

impl Epoch {
    pub fn now() -> Self {
        Self(Self::physical_now())
    }

    /// `physical_now` returns the current physical epoch using current time in milliseconds.
    fn physical_now() -> u64 {
        EPOCH_CLOCK.now().as_u64() / 1_000_000
    }
}

impl From<u64> for Epoch {
    fn from(epoch: u64) -> Self {
        Self(epoch)
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_epoch() {
        let mut prev_epoch = Epoch::now();
        for _ in 0..1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            let epoch = Epoch::now();
            assert!(prev_epoch < epoch);
            prev_epoch = epoch;
        }
    }
}
