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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TIMESTAMP_SHIFT_BITS: u8 = 22;
const WORKER_ID_SHIFT_BITS: u8 = 12;
const SEQUENCE_UPPER_BOUND: u16 = 1 << 12;
const WORKER_ID_UPPER_BOUND: u32 = 1 << 10;

/// `IdGenerator` generates unique ids using snowflake algorithm as following format:
///
/// | timestamp | worker id | sequence |
/// |-----------|-----------|----------|
/// |  41 bits  | 10 bits   | 12 bits  |
#[derive(Debug)]
pub struct IdGenerator {
    /// Specific epoch using for generating ids.
    epoch: SystemTime,

    /// Last timestamp part of id.
    last_duration_ms: i64,
    /// Current worker id.
    worker_id: u32,
    /// Last sequence part of id.
    sequence: u16,
}

pub type Id = i64;

impl IdGenerator {
    pub fn new(worker_id: u32) -> Self {
        Self::with_epoch(worker_id, UNIX_EPOCH)
    }

    pub fn with_epoch(worker_id: u32, epoch: SystemTime) -> Self {
        assert!(worker_id < WORKER_ID_UPPER_BOUND);
        Self {
            epoch,
            last_duration_ms: epoch.elapsed().unwrap().as_millis() as i64,
            worker_id,
            sequence: 0,
        }
    }

    fn id(&self) -> Id {
        self.last_duration_ms << TIMESTAMP_SHIFT_BITS
            | (self.worker_id << WORKER_ID_SHIFT_BITS) as i64
            | self.sequence as i64
    }

    fn try_update_duration(&mut self) {
        let current_duration = self.epoch.elapsed().unwrap();
        let current_duration_ms = current_duration.as_millis() as i64;
        if current_duration_ms < self.last_duration_ms {
            tracing::warn!(
                "Clock moved backwards: last_duration={}, current_duration={}",
                self.last_duration_ms,
                current_duration_ms
            );
        }

        if current_duration_ms > self.last_duration_ms {
            self.last_duration_ms = current_duration_ms;
            self.sequence = 0;
        } else if self.sequence == SEQUENCE_UPPER_BOUND {
            // If the sequence reaches the upper bound, spin loop here and wait for next
            // millisecond. Here we do not consider time goes backwards, it can also be covered
            // here.
            tracing::warn!("Sequence for row-id reached upper bound, spin loop.");
            std::thread::sleep(
                Duration::from_millis(current_duration.subsec_millis() as u64 + 1)
                    - Duration::from_nanos(current_duration.subsec_nanos() as u64),
            );
        }
    }

    pub fn next_id_batch(&mut self, length: usize) -> Vec<Id> {
        self.try_update_duration();
        let mut ret = Vec::with_capacity(length);
        while ret.len() < length {
            if self.sequence < SEQUENCE_UPPER_BOUND {
                ret.push(self.id());
                self.sequence += 1;
            } else {
                self.try_update_duration();
            }
        }

        ret
    }

    pub fn next_id(&mut self) -> Id {
        self.try_update_duration();
        if self.sequence < SEQUENCE_UPPER_BOUND {
            let id = self.id();
            self.sequence += 1;
            id
        } else {
            self.next_id()
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_generator() {
        let mut generator = IdGenerator::new(0);
        let mut last_id = generator.next_id();
        for _ in 0..100000 {
            let id = generator.next_id();
            assert!(id > last_id);
            last_id = id;
        }
        std::thread::sleep(Duration::from_millis(10));
        let id = generator.next_id();
        assert!(id > last_id);
        assert_ne!(id >> TIMESTAMP_SHIFT_BITS, last_id >> TIMESTAMP_SHIFT_BITS);
        assert_eq!(id & (SEQUENCE_UPPER_BOUND as i64 - 1), 0);

        let mut generator = IdGenerator::new(1);
        let ids = generator.next_id_batch((SEQUENCE_UPPER_BOUND + 10) as usize);
        let mut expected = (0..SEQUENCE_UPPER_BOUND).collect_vec();
        expected.extend(0..10);
        assert_eq!(
            ids.into_iter()
                .map(|id| (id as u16) & (SEQUENCE_UPPER_BOUND - 1))
                .collect_vec(),
            expected
        );
    }
}
