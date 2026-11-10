// Copyright 2026 RisingWave Labs
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

//! Private K=16 block-key index used only by cached SST metadata.
//!
//! Each entry is [shared `TableKey` length: u32 LE, suffix length: u32 LE, suffix, epoch: 8B].
//! Every sixteenth entry has shared=0. Table IDs stay in the block descriptors and epochs
//! remain uncompressed, preserving `FullKey` order without reconstructing keys during search.

use std::cmp::Ordering;
use std::ops::{Bound, Range};

use bytes::Buf;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::{EPOCH_LEN, FullKey, TABLE_PREFIX_LEN, TableKey, UserKey};

use super::CachedBlockMeta;

const KEY_RESTART_INTERVAL: usize = 16;

/// Construction-only accumulator shared by native, Foyer and writer metadata paths.
pub(super) struct CachedBlocksBuilder {
    block_metas: Vec<CachedBlockMeta>,
    data: Vec<u8>,
    restarts: Vec<usize>,
}

impl CachedBlocksBuilder {
    pub(super) fn new(block_count: usize) -> Self {
        Self {
            block_metas: Vec::with_capacity(block_count),
            // Two u32 headers and an uncompressed epoch are present in every entry.
            data: Vec::with_capacity(block_count * (8 + EPOCH_LEN)),
            restarts: Vec::with_capacity(block_count.div_ceil(KEY_RESTART_INTERVAL)),
        }
    }

    // Callers own the previous full key so native decoding can borrow it from the input.
    pub(super) fn push(&mut self, block: CachedBlockMeta, key: &[u8], previous: &[u8]) {
        let table_key = FullKey::decode(key).user_key.table_key.0;
        let shared = if self.block_metas.len().is_multiple_of(KEY_RESTART_INTERVAL) {
            self.restarts.push(self.data.len());
            0
        } else {
            common_prefix(FullKey::decode(previous).user_key.table_key.0, table_key)
        };
        let suffix = &table_key[shared..];
        self.data.extend_from_slice(&(shared as u32).to_le_bytes());
        self.data
            .extend_from_slice(&(suffix.len() as u32).to_le_bytes());
        self.data.extend_from_slice(suffix);
        self.data.extend_from_slice(&key[key.len() - EPOCH_LEN..]);
        self.block_metas.push(block);
    }

    #[inline]
    pub(super) fn finish(self) -> CachedBlocks {
        CachedBlocks {
            block_metas: self.block_metas,
            keys: PrefixKeys {
                data: self.data.into_boxed_slice(),
                restarts: self.restarts.into_boxed_slice(),
            },
        }
    }
}

pub(super) struct CachedBlocks {
    pub(super) block_metas: Vec<CachedBlockMeta>,
    pub(super) keys: PrefixKeys,
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let end = a.len().min(b.len());
    let mut len = 0;
    while len + 32 <= end && a[len..len + 32] == b[len..len + 32] {
        len += 32;
    }
    while len < end && a[len] == b[len] {
        len += 1;
    }
    len
}

/// Cache-only K16 encoding: shared `TableKey` length, suffix length, suffix, and an
/// uncompressed epoch. Table IDs live in `CachedBlockMeta`; neither can enter a shared prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PrefixKeys {
    data: Box<[u8]>,
    restarts: Box<[usize]>,
}

impl PrefixKeys {
    fn entries(&self, offset: usize) -> impl Iterator<Item = KeyEntry<'_>> {
        let mut data = &self.data[offset..];
        std::iter::from_fn(move || (!data.is_empty()).then(|| KeyEntry::decode(&mut data)))
    }

    fn restart(&self, group: usize) -> KeyEntry<'_> {
        self.entries(self.restarts[group]).next().unwrap()
    }
}

/// One borrowed entry of the in-memory prefix encoding.
struct KeyEntry<'a> {
    shared: usize,
    suffix: &'a [u8],
    epoch: &'a [u8; EPOCH_LEN],
}

impl<'a> KeyEntry<'a> {
    fn decode(data: &mut &'a [u8]) -> Self {
        let shared = data.get_u32_le() as usize;
        let len = data.get_u32_le() as usize;
        let (suffix, rest) = data.split_at(len);
        let (epoch, rest) = rest.split_at(EPOCH_LEN);
        *data = rest;
        Self {
            shared,
            suffix,
            epoch: epoch.try_into().unwrap(),
        }
    }

    fn epoch(&self) -> EpochWithGap {
        EpochWithGap::from_u64(u64::from_be_bytes(*self.epoch))
    }

    /// Apply a `TableKey` delta to a full-key buffer. This also replaces its table ID and epoch.
    fn restore(&self, table_id: TableId, key: &mut Vec<u8>) {
        key.resize(TABLE_PREFIX_LEN + self.shared, 0);
        key[..TABLE_PREFIX_LEN].copy_from_slice(&table_id.as_raw_id().to_be_bytes());
        key.extend_from_slice(self.suffix);
        key.extend_from_slice(self.epoch);
    }

    fn restart_key(&self, table_id: TableId) -> FullKey<&'a [u8]> {
        debug_assert_eq!(self.shared, 0);
        FullKey {
            user_key: UserKey::new(table_id, TableKey(self.suffix)),
            epoch_with_gap: self.epoch(),
        }
    }
}

/// Compares successive `TableKey` deltas against one fixed target, without restoring keys.
struct TableKeyComparator<'a> {
    target: &'a [u8],
    matched: usize,
    order: Ordering,
}

impl<'a> TableKeyComparator<'a> {
    fn new(target: &'a [u8]) -> Self {
        Self {
            target,
            matched: 0,
            order: Ordering::Equal,
        }
    }

    fn compare(&mut self, entry: &KeyEntry<'_>) -> Ordering {
        if entry.shared <= self.matched {
            let target = &self.target[entry.shared..];
            let matched = common_prefix(entry.suffix, target);
            self.matched = entry.shared + matched;
            self.order = entry.suffix.get(matched).cmp(&target.get(matched));
        }
        // Otherwise the shared prefix retains the previous first difference, including
        // a target that ended there. A previous key that ended there cannot share more bytes.
        self.order
    }
}

impl PrefixKeys {
    pub(super) fn estimated_heap_size(&self) -> usize {
        self.data.len() + std::mem::size_of_val(&*self.restarts)
    }

    /// Full-key lengths for legacy size accounting; no keys are reconstructed.
    pub(super) fn full_key_lengths(&self) -> impl Iterator<Item = usize> + '_ {
        self.entries(0)
            .map(|entry| TABLE_PREFIX_LEN + entry.shared + entry.suffix.len() + EPOCH_LEN)
    }

    /// Sequential reconstruction for legacy serialization, using caller-owned scratch.
    pub(super) fn cursor(&self) -> KeyCursor<'_> {
        KeyCursor { data: &self.data }
    }

    /// Restores one key into reader-owned scratch, without retaining decoded keys in the cache.
    pub(super) fn restore_key<'a>(
        &self,
        index: usize,
        table_id: TableId,
        key: &'a mut Vec<u8>,
    ) -> &'a [u8] {
        let group = index / KEY_RESTART_INTERVAL;
        let first = group * KEY_RESTART_INTERVAL;
        for entry in self.entries(self.restarts[group]).take(index - first + 1) {
            entry.restore(table_id, key);
        }
        key
    }

    /// Reads the fixed vnode prefix, if the block separator retains it.
    pub(super) fn vnode(&self, index: usize) -> Option<VirtualNode> {
        let group = index / KEY_RESTART_INTERVAL;
        let first = group * KEY_RESTART_INTERVAL;
        let mut prefix = [0; VirtualNode::SIZE];
        let mut key_len = 0;
        for entry in self.entries(self.restarts[group]).take(index - first + 1) {
            key_len = entry.shared + entry.suffix.len();
            let start = entry.shared.min(prefix.len());
            let end = key_len.min(prefix.len());
            prefix[start..end].copy_from_slice(&entry.suffix[..end - start]);
        }
        (key_len >= prefix.len()).then(|| VirtualNode::from_be_bytes(prefix))
    }

    /// Compares a block boundary without allocating or restoring its full key.
    pub(super) fn compare_key(
        &self,
        index: usize,
        table_id: TableId,
        target: FullKey<&[u8]>,
    ) -> Ordering {
        let table_order = table_id.cmp(&target.user_key.table_id);
        if !table_order.is_eq() {
            return table_order;
        }
        let group = index / KEY_RESTART_INTERVAL;
        let first = group * KEY_RESTART_INTERVAL;
        let mut comparator = TableKeyComparator::new(target.user_key.table_key.0);
        let mut order = Ordering::Equal;
        for entry in self.entries(self.restarts[group]).take(index - first + 1) {
            order = comparator
                .compare(&entry)
                .then_with(|| target.epoch_with_gap.cmp(&entry.epoch()));
        }
        order
    }

    /// Returns the absolute index after the keys within `bound` in the requested range.
    pub(super) fn partition_point(
        &self,
        blocks: &[CachedBlockMeta],
        range: Range<usize>,
        bound: Bound<FullKey<&[u8]>>,
    ) -> usize {
        assert!(range.start <= range.end && range.end <= blocks.len());
        if range.is_empty() {
            return range.start;
        }
        let (target, inclusive) = match bound {
            Bound::Included(target) => (target, true),
            Bound::Excluded(target) => (target, false),
            Bound::Unbounded => return range.end,
        };
        let before_bound = |order: Ordering| order.is_lt() || (inclusive && order.is_eq());
        let first_group = range.start / KEY_RESTART_INTERVAL;
        let end_group = (range.end - 1) / KEY_RESTART_INTERVAL + 1;
        if !before_bound(self.compare_key(range.start, blocks[range.start].table_id, target)) {
            return range.start;
        }
        let mut left = first_group + 1;
        let mut right = end_group;
        while left < right {
            let group = left + (right - left) / 2;
            let first = group * KEY_RESTART_INTERVAL;
            let restart = self.restart(group).restart_key(blocks[first].table_id);
            if before_bound(restart.cmp(&target)) {
                left = group + 1;
            } else {
                right = group;
            }
        }
        let group = left - 1;
        let first = group * KEY_RESTART_INTERVAL;
        let end = range.end.min(first + KEY_RESTART_INTERVAL);
        let known_true = first.max(range.start);
        if known_true + 1 == end {
            return end;
        }
        let mut comparator = TableKeyComparator::new(target.user_key.table_key.0);
        for (offset, entry) in self
            .entries(self.restarts[group])
            .take(end - first)
            .enumerate()
        {
            let index = first + offset;
            // Always advance TableKey comparison, including entries before range.start and
            // entries from another table. Prefixes contain only TableKey bytes.
            let user_order = comparator.compare(&entry);
            if index <= known_true {
                continue;
            }
            let order = blocks[index]
                .table_id
                .cmp(&target.user_key.table_id)
                .then(user_order)
                .then_with(|| target.epoch_with_gap.cmp(&entry.epoch()));
            if !before_bound(order) {
                return index;
            }
        }
        end
    }
}

/// A borrowed position in the encoded keys. Only serialization needs full sequential keys.
pub(super) struct KeyCursor<'a> {
    data: &'a [u8],
}

impl KeyCursor<'_> {
    pub(super) fn restore_next(&mut self, table_id: TableId, key: &mut Vec<u8>) {
        KeyEntry::decode(&mut self.data).restore(table_id, key);
    }
}
