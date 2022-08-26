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
const VNODE_ID_SHIFT_BITS: u8 = 12;
const SEQUENCE_UPPER_BOUND: u16 = 1 << 12;
const VNODE_ID_UPPER_BOUND: u32 = 1 << 10;

/// `RowIdGenerator` generates unique row ids using snowflake algorithm as following format:
///
/// | timestamp | vnode id | sequence |
/// |-----------|-----------|----------|
/// |  41 bits  | 10 bits   | 12 bits  |
#[derive(Debug)]
pub struct RowIdGenerator {
    /// Specific epoch using for generating row ids.
    epoch: SystemTime,

    /// Last timestamp part of row id.
    last_duration_ms: i64,
    /// Current vnode id.
    pub vnode_id: u32,
    /// Last sequence part of row id.
    sequence: u16,
}

pub type RowId = i64;

impl RowIdGenerator {
    pub fn new(vnode_id: u32) -> Self {
        assert!(vnode_id < VNODE_ID_UPPER_BOUND);
        Self::with_epoch(vnode_id, UNIX_EPOCH)
    }

    pub fn with_epoch(vnode_id: u32, epoch: SystemTime) -> Self {
        Self {
            epoch,
            last_duration_ms: epoch.elapsed().unwrap().as_millis() as i64,
            vnode_id,
            sequence: 0,
        }
    }

    fn row_id(&self) -> RowId {
        self.last_duration_ms << TIMESTAMP_SHIFT_BITS
            | (self.vnode_id << VNODE_ID_SHIFT_BITS) as i64
            | self.sequence as i64
    }

    async fn try_update_duration(&mut self) {
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
            tokio::time::sleep(
                Duration::from_millis(current_duration.subsec_millis() as u64 + 1)
                    - Duration::from_nanos(current_duration.subsec_nanos() as u64),
            )
            .await;
        }
    }

    /// Generate a sequence `RowId`s.
    ///
    /// This may block for a while if too many IDs are generated in one millisecond.
    pub async fn next_batch(&mut self, length: usize) -> Vec<RowId> {
        self.try_update_duration().await;
        let mut ret = Vec::with_capacity(length);
        while ret.len() < length {
            if self.sequence < SEQUENCE_UPPER_BOUND {
                ret.push(self.row_id());
                self.sequence += 1;
            } else {
                self.try_update_duration().await;
            }
        }
        ret
    }

    /// Generate a new `RowId`.
    ///
    /// This may block for a while if too many IDs are generated in one millisecond.
    #[allow(clippy::should_implement_trait)]
    pub async fn next(&mut self) -> RowId {
        loop {
            self.try_update_duration().await;
            if self.sequence < SEQUENCE_UPPER_BOUND {
                let row_id = self.row_id();
                self.sequence += 1;
                return row_id;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[tokio::test]
    async fn test_generator() {
        let mut generator = RowIdGenerator::new(0);
        let mut last_row_id = generator.next().await;
        for _ in 0..100000 {
            let row_id = generator.next().await;
            assert!(row_id > last_row_id);
            last_row_id = row_id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        let row_id = generator.next().await;
        assert!(row_id > last_row_id);
        assert_ne!(
            row_id >> TIMESTAMP_SHIFT_BITS,
            last_row_id >> TIMESTAMP_SHIFT_BITS
        );
        assert_eq!(row_id & (SEQUENCE_UPPER_BOUND as i64 - 1), 0);

        let mut generator = RowIdGenerator::new(1);
        let row_ids = generator
            .next_batch((SEQUENCE_UPPER_BOUND + 10) as usize)
            .await;
        let mut expected = (0..SEQUENCE_UPPER_BOUND).collect_vec();
        expected.extend(0..10);
        assert_eq!(
            row_ids
                .into_iter()
                .map(|id| (id as u16) & (SEQUENCE_UPPER_BOUND - 1))
                .collect_vec(),
            expected
        );
    }
}
