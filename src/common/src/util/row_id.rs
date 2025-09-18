// Copyright 2025 RisingWave Labs
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
use std::collections::HashMap;
use std::time::SystemTime;

use super::epoch::UNIX_RISINGWAVE_DATE_EPOCH;
use crate::hash::VirtualNode;

/// The number of bits occupied by the vnode part and the sequence part of a row id.
const TIMESTAMP_SHIFT_BITS: u32 = 22;

/// The number of bits occupied by the vnode part of a row id in the previous version.
const COMPAT_VNODE_BITS: u32 = 10;

/// `RowIdGenerator` generates unique row ids using snowflake algorithm as following format:
///
/// | timestamp | vnode & sequence |
/// |-----------|------------------|
/// |  41 bits  |     22 bits      |
///
/// The vnode part can occupy 10..=15 bits, which is determined by the vnode count. Thus,
/// the sequence part will occupy 7..=12 bits. See [`bit_for_vnode`] for more details.
#[derive(Debug)]
pub struct RowIdGenerator {
    /// Specific base timestamp using for generating row ids.
    base: SystemTime,

    /// Last timestamp part of row id, based on `base`.
    last_timestamp_ms: i64,

    /// The number of bits used for vnode.
    vnode_bit: u32,

    /// Virtual nodes used by this generator.
    vnodes: Vec<VirtualNode>,

    /// Current index of `vnodes`.
    vnodes_index: u16,

    /// Last sequence part of row id.
    sequence: u16,
}

pub type RowId = i64;

/// The number of bits occupied by the vnode part of a row id.
///
/// In previous versions, this was fixed to 10 bits even if the vnode count was fixed to 256.
/// For backward compatibility, we still use 10 bits for vnode count less than or equal to 1024.
/// For larger vnode counts, we use the smallest power of 2 that fits the vnode count.
fn bit_for_vnode(vnode_count: usize) -> u32 {
    debug_assert!(
        vnode_count <= VirtualNode::MAX_COUNT,
        "invalid vnode count {vnode_count}"
    );

    if vnode_count <= 1 << COMPAT_VNODE_BITS {
        COMPAT_VNODE_BITS
    } else {
        vnode_count.next_power_of_two().ilog2()
    }
}

/// Compute vnode from the given row id.
///
/// # `vnode_count`
///
/// The given `vnode_count` determines the valid range of the returned vnode. It does not have to
/// be the same as the vnode count used when the row id was generated with [`RowIdGenerator`].
///
/// However, only if they are the same, the vnode retrieved here is guaranteed to be the same as
/// when it was generated. Otherwise, the vnode can be different and skewed, but the row ids
/// generated under the same vnode will still yield the same result.
///
/// This is okay because we rely on the reversibility only if the serial type (row id) is generated
/// and persisted in the same fragment, where the vnode count is the same. In other cases, the
/// serial type is more like a normal integer type, and the algorithm to hash or compute vnode from
/// it does not matter.
#[inline]
pub fn compute_vnode_from_row_id(id: RowId, vnode_count: usize) -> VirtualNode {
    let vnode_bit = bit_for_vnode(vnode_count);
    let sequence_bit = TIMESTAMP_SHIFT_BITS - vnode_bit;

    let vnode_part = ((id >> sequence_bit) & ((1 << vnode_bit) - 1)) as usize;

    // If the given `vnode_count` is the same as the one used when the row id was generated, this
    // is no-op. Otherwise, we clamp the vnode to fit in the given vnode count.
    VirtualNode::from_index(vnode_part % vnode_count)
}

impl RowIdGenerator {
    /// Create a new `RowIdGenerator` with given virtual nodes and vnode count.
    pub fn new(vnodes: impl IntoIterator<Item = VirtualNode>, vnode_count: usize) -> Self {
        let base = *UNIX_RISINGWAVE_DATE_EPOCH;
        let vnode_bit = bit_for_vnode(vnode_count);

        Self {
            base,
            last_timestamp_ms: base.elapsed().unwrap().as_millis() as i64,
            vnode_bit,
            vnodes: vnodes.into_iter().collect(),
            vnodes_index: 0,
            sequence: 0,
        }
    }

    /// The upper bound of the sequence part, exclusive.
    fn sequence_upper_bound(&self) -> u16 {
        1 << (TIMESTAMP_SHIFT_BITS - self.vnode_bit)
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
            Ordering::Equal => {
                // Update the timestamp if the sequence reaches the upper bound.
                self.sequence == self.sequence_upper_bound()
            }
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

                #[cfg(madsim)]
                tokio::time::advance(std::time::Duration::from_micros(10));
                #[cfg(not(madsim))]
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
        if self.sequence >= self.sequence_upper_bound() {
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
                | (vnode << (TIMESTAMP_SHIFT_BITS - self.vnode_bit)) as i64
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

/// `ChangelogRowIdGenerator` generates unique changelog row ids using snowflake algorithm.
/// Unlike `RowIdGenerator`, it maintains a separate sequence for each vnode and generates
/// row ids based on the input vnode.
#[derive(Debug)]
pub struct ChangelogRowIdGenerator {
    /// Specific base timestamp using for generating row ids.
    base: SystemTime,

    /// Last timestamp part of row id, based on `base`.
    last_timestamp_ms: i64,

    /// The number of bits used for vnode.
    vnode_bit: u32,

    /// Sequence for each vnode. Key is vnode index, value is sequence.
    vnodes_sequence: HashMap<VirtualNode, u16>,
}

impl ChangelogRowIdGenerator {
    /// Create a new `ChangelogRowIdGenerator` with given vnode count.
    pub fn new(vnodes: impl IntoIterator<Item = VirtualNode>, vnode_count: usize) -> Self {
        let base = *UNIX_RISINGWAVE_DATE_EPOCH;
        let vnode_bit = bit_for_vnode(vnode_count);

        let vnodes_sequence = vnodes
            .into_iter()
            .map(|vnode| (vnode, 0))
            .collect::<HashMap<_, _>>();
        Self {
            base,
            last_timestamp_ms: base.elapsed().unwrap().as_millis() as i64,
            vnode_bit,
            vnodes_sequence,
        }
    }

    /// The upper bound of the sequence part for changelog, exclusive.
    fn sequence_upper_bound(&self) -> u16 {
        1 << (TIMESTAMP_SHIFT_BITS - self.vnode_bit)
    }

    fn try_update_timestamp(&mut self) {
        let get_current_timestamp_ms = || self.base.elapsed().unwrap().as_millis() as i64;

        let current_timestamp_ms = get_current_timestamp_ms();
        let sequence_upper_bound = self.sequence_upper_bound();

        // Check if any vnode has reached the upper bound
        let any_vnode_at_limit = self
            .vnodes_sequence
            .values()
            .any(|&seq| seq >= sequence_upper_bound);

        let to_update = match current_timestamp_ms.cmp(&self.last_timestamp_ms) {
            Ordering::Less => {
                tracing::warn!(
                    "Clock moved backwards: last={}, current={}",
                    self.last_timestamp_ms,
                    current_timestamp_ms,
                );
                true
            }
            Ordering::Equal => {
                // Update the timestamp if any vnode sequence reaches the upper bound.
                any_vnode_at_limit
            }
            Ordering::Greater => true,
        };

        if to_update {
            // If the timestamp is not increased, spin loop here and wait for next millisecond.
            let mut current_timestamp_ms = current_timestamp_ms;
            loop {
                if current_timestamp_ms > self.last_timestamp_ms {
                    break;
                }
                current_timestamp_ms = get_current_timestamp_ms();

                #[cfg(madsim)]
                tokio::time::advance(std::time::Duration::from_micros(10));
                #[cfg(not(madsim))]
                std::hint::spin_loop();
            }

            // Reset states: reset all vnode sequences to 0.
            self.last_timestamp_ms = current_timestamp_ms;
            self.vnodes_sequence.iter_mut().for_each(|(_, seq)| *seq = 0);
        }
    }

    fn next_changelog_row_id_in_current_timestamp(&mut self, vnode: &VirtualNode) -> Option<RowId> {
        let current_sequence = *self
            .vnodes_sequence
            .get(vnode)
            .expect(&format!("vnode {:?} not found in generator", vnode));

        if current_sequence >= self.sequence_upper_bound() {
            return None;
        }

        let sequence = current_sequence;
        self.vnodes_sequence.insert(*vnode, current_sequence + 1);

        Some(
            self.last_timestamp_ms << TIMESTAMP_SHIFT_BITS
                | (vnode.to_index() << (TIMESTAMP_SHIFT_BITS - self.vnode_bit)) as i64
                | sequence as i64,
        )
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self, vnode: &VirtualNode) -> RowId {
        self.try_update_timestamp();

        if let Some(row_id) = self.next_changelog_row_id_in_current_timestamp(vnode) {
            row_id
        } else {
            self.try_update_timestamp();
            self.next_changelog_row_id_in_current_timestamp(vnode)
                .expect("timestamp should be updated")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use itertools::Itertools;

    use super::*;

    #[allow(clippy::unused_async)] // `madsim::time::advance` requires to be in async context
    async fn test_generator_with_vnode_count(vnode_count: usize) {
        let mut generator = RowIdGenerator::new([VirtualNode::from_index(0)], vnode_count);
        let sequence_upper_bound = generator.sequence_upper_bound();

        let mut last_row_id = generator.next();
        for _ in 0..100000 {
            let row_id = generator.next();
            assert!(row_id > last_row_id);
            last_row_id = row_id;
        }

        let dur = Duration::from_millis(10);
        #[cfg(madsim)]
        tokio::time::advance(dur);
        #[cfg(not(madsim))]
        std::thread::sleep(dur);

        let row_id = generator.next();
        assert!(row_id > last_row_id);
        assert_ne!(
            row_id >> TIMESTAMP_SHIFT_BITS,
            last_row_id >> TIMESTAMP_SHIFT_BITS
        );
        assert_eq!(row_id & (sequence_upper_bound as i64 - 1), 0);

        let mut generator = RowIdGenerator::new([VirtualNode::from_index(1)], vnode_count);
        let row_ids = generator.next_batch((sequence_upper_bound + 10) as usize);
        let mut expected = (0..sequence_upper_bound).collect_vec();
        expected.extend(0..10);
        assert_eq!(
            row_ids
                .into_iter()
                .map(|id| (id as u16) & (sequence_upper_bound - 1))
                .collect_vec(),
            expected
        );
    }

    #[allow(clippy::unused_async)] // `madsim::time::advance` requires to be in async context
    async fn test_generator_multiple_vnodes_with_vnode_count(vnode_count: usize) {
        assert!(vnode_count >= 20);

        let vnodes = || {
            (0..10)
                .chain((vnode_count - 10)..vnode_count)
                .map(VirtualNode::from_index)
        };
        let vnode_of = |row_id: RowId| compute_vnode_from_row_id(row_id, vnode_count);

        let mut generator = RowIdGenerator::new(vnodes(), vnode_count);
        let sequence_upper_bound = generator.sequence_upper_bound();

        let row_ids = generator.next_batch((sequence_upper_bound as usize) * 20 + 1);

        // Check timestamps.
        let timestamps = row_ids
            .iter()
            .map(|&r| r >> TIMESTAMP_SHIFT_BITS)
            .collect_vec();

        let (last_timestamp, first_timestamps) = timestamps.split_last().unwrap();
        let first_timestamp = first_timestamps.iter().unique().exactly_one().unwrap();

        // Check vnodes.
        let expected_vnodes = vnodes().cycle();
        let actual_vnodes = row_ids.iter().map(|&r| vnode_of(r));

        #[expect(clippy::disallowed_methods)] // `expected_vnodes` is an endless cycle iterator
        for (expected, actual) in expected_vnodes.zip(actual_vnodes) {
            assert_eq!(expected, actual);
        }

        assert!(last_timestamp > first_timestamp);
    }

    macro_rules! test {
        ($vnode_count:expr, $name:ident, $name_mul:ident) => {
            #[tokio::test]
            async fn $name() {
                test_generator_with_vnode_count($vnode_count).await;
            }

            #[tokio::test]
            async fn $name_mul() {
                test_generator_multiple_vnodes_with_vnode_count($vnode_count).await;
            }
        };
    }

    test!(64, test_64, test_64_mul); // less than default value
    test!(114, test_114, test_114_mul); // not a power of 2, less than default value
    test!(256, test_256, test_256_mul); // default value, backward compatibility
    test!(1 << COMPAT_VNODE_BITS, test_1024, test_1024_mul); // max value with 10 bits
    test!(2048, test_2048, test_2048_mul); // more than 10 bits
    test!(2333, test_2333, test_2333_mul); // not a power of 2, larger than default value
    test!(VirtualNode::MAX_COUNT, test_max, test_max_mul); // max supported
}
