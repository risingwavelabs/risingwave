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

use std::cmp::Ordering;
use std::time::SystemTime;

use static_assertions::const_assert;

use super::epoch::UNIX_RISINGWAVE_DATE_EPOCH;
use crate::hash::VirtualNode;

const TIMESTAMP_SHIFT_BITS: u8 = 22;
const VNODE_ID_SHIFT_BITS: u8 = 12;
const SEQUENCE_UPPER_BOUND: u16 = 1 << 12;
const VNODE_ID_UPPER_BOUND: u32 = 1 << 10;

const_assert!(VNODE_ID_UPPER_BOUND >= VirtualNode::COUNT as u32);

/// `RowIdGenerator` generates unique row ids using snowflake algorithm as following format:
///
/// | timestamp | vnode id | sequence |
/// |-----------|----------|----------|
/// |  41 bits  | 10 bits  | 12 bits  |
#[derive(Debug)]
pub struct RowIdGenerator {
    /// Specific base timestamp using for generating row ids.
    base: SystemTime,

    /// Last timestamp part of row id, based on `base`.
    last_timestamp_ms: i64,

    /// Virtual nodes used by this generator.
    pub vnodes: Vec<VirtualNode>,

    /// Current index of `vnodes`.
    vnodes_index: u16,

    /// Last sequence part of row id.
    sequence: u16,
}

pub type RowId = i64;

#[inline]
pub fn extract_vnode_id_from_row_id(id: RowId) -> VirtualNode {
    let vnode_id = ((id >> VNODE_ID_SHIFT_BITS) & (VNODE_ID_UPPER_BOUND as i64 - 1)) as u32;
    assert!(vnode_id < VNODE_ID_UPPER_BOUND);
    VirtualNode::from_index(vnode_id as usize)
}

impl RowIdGenerator {
    /// Create a new `RowIdGenerator` with given virtual nodes.
    pub fn new(vnodes: impl IntoIterator<Item = VirtualNode>) -> Self {
        let base = *UNIX_RISINGWAVE_DATE_EPOCH;
        Self {
            base,
            last_timestamp_ms: base.elapsed().unwrap().as_millis() as i64,
            vnodes: vnodes.into_iter().collect(),
            vnodes_index: 0,
            sequence: 0,
        }
    }

    /// Update the timestamp, so that the millisecond part of row id is **always** increased.
    ///
    /// This method will immediately return if the timestamp is increased or there's remaining
    /// sequence for the current millisecond. Otherwise, it will spin loop until the timestamp is
    /// increased.
    fn try_update_timestamp(&mut self) {
        let get_current_timestamp_ms = || self.base.elapsed().unwrap().as_millis() as i64;

        let current_timestamp_ms = get_current_timestamp_ms();
        let to_update = match current_timestamp_ms.cmp(&self.last_timestamp_ms) {
            Ordering::Less => {
                tracing::warn!(
                    "Clock moved backwards: last={}, current={}",
                    self.last_timestamp_ms,
                    current_timestamp_ms,
                );
                true
            }
            Ordering::Equal => self.sequence == SEQUENCE_UPPER_BOUND,
            Ordering::Greater => true,
        };

        if to_update {
            // If the timestamp is not increased, spin loop here and wait for next millisecond. The
            // case for time going backwards and sequence reaches the upper bound are both covered.
            let mut current_timestamp_ms = current_timestamp_ms;
            loop {
                if current_timestamp_ms > self.last_timestamp_ms {
                    break;
                }
                current_timestamp_ms = get_current_timestamp_ms();
                std::hint::spin_loop();
            }

            // Reset states. We do not reset the `vnode_index` to make all vnodes are evenly used.
            self.last_timestamp_ms = current_timestamp_ms;
            self.sequence = 0;
        }
    }

    /// Generate a new `RowId`. Returns `None` if the sequence reaches the upper bound of current
    /// timestamp, and `try_update_timestamp` should be called to update the timestamp and reset the
    /// sequence. After that, the next call of this method always returns `Some`.
    fn next_row_id_in_current_timestamp(&mut self) -> Option<RowId> {
        if self.sequence >= SEQUENCE_UPPER_BOUND {
            return None;
        }

        let vnode = self.vnodes[self.vnodes_index as usize].to_index();
        let sequence = self.sequence;

        self.vnodes_index = (self.vnodes_index + 1) % self.vnodes.len() as u16;
        if self.vnodes_index == 0 {
            self.sequence += 1;
        }

        Some(
            self.last_timestamp_ms << TIMESTAMP_SHIFT_BITS
                | (vnode << VNODE_ID_SHIFT_BITS) as i64
                | sequence as i64,
        )
    }

    /// Returns an infinite iterator that generates `RowId`s.
    fn gen_iter(&mut self) -> impl Iterator<Item = RowId> + '_ {
        std::iter::from_fn(move || {
            if let Some(next) = self.next_row_id_in_current_timestamp() {
                Some(next)
            } else {
                self.try_update_timestamp();
                Some(
                    self.next_row_id_in_current_timestamp()
                        .expect("timestamp should be updated"),
                )
            }
        })
    }

    /// Generate a sequence of `RowId`s. Compared to `next`, this method is more efficient as it
    /// only checks the timestamp once before generating the first `RowId`, instead of doing that
    /// every `RowId`.
    ///
    /// This may block for a while if too many IDs are generated in one millisecond.
    pub fn next_batch(&mut self, length: usize) -> Vec<RowId> {
        self.try_update_timestamp();

        let mut ret = Vec::with_capacity(length);
        ret.extend(self.gen_iter().take(length));
        assert_eq!(ret.len(), length);
        ret
    }

    /// Generate a new `RowId`.
    ///
    /// This may block for a while if too many IDs are generated in one millisecond.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> RowId {
        self.try_update_timestamp();

        self.gen_iter().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_generator() {
        let mut generator = RowIdGenerator::new([VirtualNode::from_index(0)]);

        let mut last_row_id = generator.next();
        for _ in 0..100000 {
            let row_id = generator.next();
            assert!(row_id > last_row_id);
            last_row_id = row_id;
        }
        std::thread::sleep(Duration::from_millis(10));
        let row_id = generator.next();
        assert!(row_id > last_row_id);
        assert_ne!(
            row_id >> TIMESTAMP_SHIFT_BITS,
            last_row_id >> TIMESTAMP_SHIFT_BITS
        );
        assert_eq!(row_id & (SEQUENCE_UPPER_BOUND as i64 - 1), 0);

        let mut generator = RowIdGenerator::new([VirtualNode::from_index(1)]);
        let row_ids = generator.next_batch((SEQUENCE_UPPER_BOUND + 10) as usize);
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

    #[test]
    fn test_generator_multiple_vnodes() {
        let mut generator = RowIdGenerator::new((0..10).map(VirtualNode::from_index));

        let row_ids = generator.next_batch((SEQUENCE_UPPER_BOUND as usize) * 10 + 1);
        let timestamps = row_ids
            .into_iter()
            .map(|r| r >> TIMESTAMP_SHIFT_BITS)
            .collect_vec();

        let (last_timestamp, first_timestamps) = timestamps.split_last().unwrap();
        let first_timestamp = first_timestamps.iter().unique().exactly_one().unwrap();

        assert!(last_timestamp > first_timestamp);
    }
}
