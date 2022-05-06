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

use std::time::Duration;

use quanta::Clock;

const TIMESTAMP_SHIFT_BITS: u8 = 22;
const WORKER_ID_SHIFT_BITS: u8 = 12;
const SEQUENCE_UPPER_BOUND: u16 = 1 << 12;
const WORKER_ID_UPPER_BOUND: u32 = 1 << 10;
const NANOS_PER_MILLI: i64 = 1_000_000;

/// `RowIdGenerator` generates unique row ids using snowflake algorithm as following format:
///
/// | timestamp | worker id | sequence |
/// |-----------|-----------|----------|
/// |  41 bits  | 10 bits   | 12 bits  |
#[derive(Debug)]
pub struct RowIdGenerator {
    /// Clock used to get current timestamp.
    clock: Clock,

    /// Last timestamp part of row id.
    last_duration_ms: i64,
    /// Current worker id.
    worker_id: u32,
    /// Last sequence part of row id.
    sequence: u16,
}

pub type RowId = i64;

impl RowIdGenerator {
    pub fn new(worker_id: u32) -> Self {
        assert!(worker_id < WORKER_ID_UPPER_BOUND);
        let clock = Clock::new();
        let last_duration_ms = clock.now().as_u64() as i64 / NANOS_PER_MILLI;
        Self {
            clock,
            last_duration_ms,
            worker_id,
            sequence: 0,
        }
    }

    pub fn next(&mut self) -> RowId {
        let current_duration = self.clock.now().as_u64();
        let current_duration_ms = current_duration as i64 / NANOS_PER_MILLI;
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
        }

        if self.sequence < SEQUENCE_UPPER_BOUND {
            let row_id = self.last_duration_ms << TIMESTAMP_SHIFT_BITS
                | (self.worker_id << WORKER_ID_SHIFT_BITS) as i64
                | self.sequence as i64;
            self.sequence += 1;

            row_id
        } else {
            // If the sequence reaches the upper bound, spin loop here and wait for next
            // millisecond. Here we do not consider time goes backwards, it can also be covered
            // here.
            tracing::warn!("Sequence for row-id reached upper bound, spin loop.");
            std::thread::sleep(
                Duration::from_millis(current_duration_ms as u64 + 1)
                    - Duration::from_nanos(current_duration as u64),
            );
            self.next()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator() {
        let mut generator = RowIdGenerator::new(0);
        let mut last_row_id = generator.next();
        for _ in 0..100000 {
            let row_id = generator.next();
            assert!(row_id > last_row_id);
            last_row_id = row_id;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
        let row_id = generator.next();
        assert!(row_id > last_row_id);
        assert_ne!(
            row_id >> TIMESTAMP_SHIFT_BITS,
            last_row_id >> TIMESTAMP_SHIFT_BITS
        );
        assert_eq!(row_id & (SEQUENCE_UPPER_BOUND as i64 - 1), 0);
    }
}
