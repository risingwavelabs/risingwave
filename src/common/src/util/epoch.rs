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
use std::time::{Duration, SystemTime};

lazy_static::lazy_static! {
    /// `UNIX_SINGULARITY_DATE_EPOCH` represents the singularity date of the UNIX epoch: 2021-04-01T00:00:00Z.
    pub static ref UNIX_SINGULARITY_DATE_EPOCH: SystemTime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_617_235_200);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Epoch(pub u64);

/// `INVALID_EPOCH` defines the invalid epoch value.
pub const INVALID_EPOCH: u64 = 0;

impl Epoch {
    pub fn now() -> Self {
        Self(Self::physical_now())
    }

    // TODO: use a monotonic library to replace SystemTime.
    pub fn physical_now() -> u64 {
        UNIX_SINGULARITY_DATE_EPOCH
            .elapsed()
            .expect("system clock set earlier than singularity date!")
            .as_millis() as u64
    }

    /// Returns the epoch in real system time.
    pub fn as_system_time(&self) -> SystemTime {
        *UNIX_SINGULARITY_DATE_EPOCH + Duration::from_millis(self.0)
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
    use chrono::{Local, TimeZone, Utc};

    use super::*;

    #[test]
    fn test_singularity_system_time() {
        let utc = Utc.ymd(2021, 4, 1).and_hms(0, 0, 0);
        let singularity_dt = Local.from_utc_datetime(&utc.naive_utc());
        let singularity_st = SystemTime::from(singularity_dt);
        assert_eq!(singularity_st, *UNIX_SINGULARITY_DATE_EPOCH);
    }
}
