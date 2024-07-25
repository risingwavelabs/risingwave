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

use std::cmp::Ordering;
use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::safe_epoch_read_table_watermarks_impl;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::table_stats::{add_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::table_watermark::{
    ReadTableWatermark, TableWatermarks, WatermarkDirection,
};

use crate::hummock::iterator::{Forward, HummockIterator, ValueMeta};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

pub struct SkipWatermarkIterator<I> {
    inner: I,
    state: SkipWatermarkState,
    /// The stats of skipped key-value pairs for each table.
    skipped_entry_table_stats: TableStatsMap,
    /// The id of table currently undergoing processing.
    last_table_id: Option<u32>,
    /// The stats of table currently undergoing processing.
    last_table_stats: TableStats,
}

impl<I: HummockIterator<Direction = Forward>> SkipWatermarkIterator<I> {
    pub fn new(inner: I, watermarks: BTreeMap<TableId, ReadTableWatermark>) -> Self {
        Self {
            inner,
            state: SkipWatermarkState::new(watermarks),
            skipped_entry_table_stats: TableStatsMap::default(),
            last_table_id: None,
            last_table_stats: TableStats::default(),
        }
    }

    pub fn from_safe_epoch_watermarks(
        inner: I,
        safe_epoch_watermarks: &BTreeMap<u32, TableWatermarks>,
    ) -> Self {
        Self {
            inner,
            state: SkipWatermarkState::from_safe_epoch_watermarks(safe_epoch_watermarks),
            skipped_entry_table_stats: TableStatsMap::default(),
            last_table_id: None,
            last_table_stats: TableStats::default(),
        }
    }

    fn reset_watermark(&mut self) {
        self.state.reset_watermark();
    }

    fn reset_skipped_entry_table_stats(&mut self) {
        self.skipped_entry_table_stats = TableStatsMap::default();
        self.last_table_id = None;
        self.last_table_stats = TableStats::default();
    }

    /// Advance the key until iterator invalid or the current key will not be filtered by the latest watermark.
    /// Calling this method should ensure that the first remaining watermark has been advanced to the current key.
    ///
    /// Return a flag indicating whether should later advance the watermark.
    async fn advance_key_and_watermark(&mut self) -> HummockResult<()> {
        // advance key and watermark in an interleave manner until nothing
        // changed after the method is called.
        while self.inner.is_valid() {
            if !self.state.should_delete(&self.inner.key()) {
                break;
            }

            if self.last_table_id.map_or(true, |last_table_id| {
                last_table_id != self.inner.key().user_key.table_id.table_id
            }) {
                self.add_last_table_stats();
                self.last_table_id = Some(self.inner.key().user_key.table_id.table_id);
            }
            self.last_table_stats.total_key_count -= 1;
            self.last_table_stats.total_key_size -= self.inner.key().encoded_len() as i64;
            self.last_table_stats.total_value_size -= self.inner.value().encoded_len() as i64;

            self.inner.next().await?;
        }
        self.add_last_table_stats();
        Ok(())
    }

    fn add_last_table_stats(&mut self) {
        let Some(last_table_id) = self.last_table_id.take() else {
            return;
        };
        let delta = std::mem::take(&mut self.last_table_stats);
        let e = self
            .skipped_entry_table_stats
            .entry(last_table_id)
            .or_default();
        e.total_key_count += delta.total_key_count;
        e.total_key_size += delta.total_key_size;
        e.total_value_size += delta.total_value_size;
    }
}

impl<I: HummockIterator<Direction = Forward>> HummockIterator for SkipWatermarkIterator<I> {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.inner.next().await?;
        // Check whether there is any remaining watermark and return early to
        // avoid calling the async `advance_key_and_watermark`, since in benchmark
        // performance downgrade is observed without this early return.
        if self.state.has_watermark() {
            self.advance_key_and_watermark().await?;
        }
        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.inner.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.reset_watermark();
        self.reset_skipped_entry_table_stats();
        self.inner.rewind().await?;
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        self.reset_watermark();
        self.reset_skipped_entry_table_stats();
        self.inner.seek(key).await?;
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        add_table_stats_map(
            &mut stats.skipped_by_watermark_table_stats,
            &self.skipped_entry_table_stats,
        );
        self.inner.collect_local_statistic(stats)
    }

    fn value_meta(&self) -> ValueMeta {
        self.inner.value_meta()
    }
}
pub struct SkipWatermarkState {
    watermarks: BTreeMap<TableId, ReadTableWatermark>,
    remain_watermarks: VecDeque<(TableId, VirtualNode, WatermarkDirection, Bytes)>,
}

impl SkipWatermarkState {
    pub fn new(watermarks: BTreeMap<TableId, ReadTableWatermark>) -> Self {
        Self {
            remain_watermarks: VecDeque::new(),
            watermarks,
        }
    }

    pub fn from_safe_epoch_watermarks(
        safe_epoch_watermarks: &BTreeMap<u32, TableWatermarks>,
    ) -> Self {
        let watermarks = safe_epoch_read_table_watermarks_impl(safe_epoch_watermarks);
        Self::new(watermarks)
    }

    #[inline(always)]
    pub fn has_watermark(&self) -> bool {
        !self.remain_watermarks.is_empty()
    }

    pub fn should_delete(&mut self, key: &FullKey<&[u8]>) -> bool {
        if let Some((table_id, vnode, direction, watermark)) = self.remain_watermarks.front() {
            let key_table_id = key.user_key.table_id;
            let (key_vnode, inner_key) = key.user_key.table_key.split_vnode();
            match (&key_table_id, &key_vnode).cmp(&(table_id, vnode)) {
                Ordering::Less => {
                    return false;
                }
                Ordering::Equal => {
                    return direction.filter_by_watermark(inner_key, watermark);
                }
                Ordering::Greater => {
                    // The current key has advanced over the watermark.
                    // We may advance the watermark before advancing the key.
                    return self.advance_watermark(key);
                }
            }
        }
        false
    }

    pub fn reset_watermark(&mut self) {
        self.remain_watermarks = self
            .watermarks
            .iter()
            .flat_map(|(table_id, read_watermarks)| {
                read_watermarks
                    .vnode_watermarks
                    .iter()
                    .map(|(vnode, watermarks)| {
                        (
                            *table_id,
                            *vnode,
                            read_watermarks.direction,
                            watermarks.clone(),
                        )
                    })
            })
            .collect();
    }

    /// Advance watermark until no watermark remains or the first watermark can possibly
    /// filter out the current or future key.
    ///
    /// Return a flag indicating whether the current key will be filtered by the current watermark.
    fn advance_watermark(&mut self, key: &FullKey<&[u8]>) -> bool {
        let key_table_id = key.user_key.table_id;
        let (key_vnode, inner_key) = key.user_key.table_key.split_vnode();
        while let Some((table_id, vnode, direction, watermark)) = self.remain_watermarks.front() {
            match (table_id, vnode).cmp(&(&key_table_id, &key_vnode)) {
                Ordering::Less => {
                    self.remain_watermarks.pop_front();
                    continue;
                }
                Ordering::Equal => {
                    match direction {
                        WatermarkDirection::Ascending => {
                            match inner_key.cmp(watermark.as_ref()) {
                                Ordering::Less => {
                                    // The current key will be filtered by the watermark.
                                    // Return true to further advance the key.
                                    return true;
                                }
                                Ordering::Equal | Ordering::Greater => {
                                    // The current key has passed the watermark.
                                    // Advance the next watermark.
                                    self.remain_watermarks.pop_front();
                                    // Since it is impossible for a (table_id, vnode) tuple to have multiple
                                    // watermark, after the pop_front, the next (table_id, vnode) must have
                                    // exceeded the current key, and we can directly return and mark that the
                                    // current key is not filtered by the watermark at the front.
                                    #[cfg(debug_assertions)]
                                    {
                                        if let Some((next_table_id, next_vnode, _, _)) =
                                            self.remain_watermarks.front()
                                        {
                                            assert!(
                                                (next_table_id, next_vnode)
                                                    > (&key_table_id, &key_vnode)
                                            );
                                        }
                                    }
                                    return false;
                                }
                            }
                        }
                        WatermarkDirection::Descending => {
                            return match inner_key.cmp(watermark.as_ref()) {
                                // Current key as not reached the watermark. Just return.
                                Ordering::Less | Ordering::Equal => false,
                                // Current key will be filtered by the watermark.
                                // Return true to further advance the key.
                                Ordering::Greater => true,
                            };
                        }
                    }
                }
                Ordering::Greater => {
                    return false;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::iter::{empty, once};

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{gen_key_from_str, FullKey, TableKey, UserKey};
    use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, WatermarkDirection};
    use risingwave_hummock_sdk::EpochWithGap;

    use crate::hummock::iterator::{HummockIterator, SkipWatermarkIterator};
    use crate::hummock::shared_buffer::shared_buffer_batch::{
        SharedBufferBatch, SharedBufferValue,
    };

    const EPOCH: u64 = test_epoch(1);
    const TABLE_ID: TableId = TableId::new(233);

    async fn assert_iter_eq(
        mut first: Option<impl HummockIterator>,
        mut second: impl HummockIterator,
        seek_key: Option<(usize, usize)>,
    ) {
        if let Some((vnode, key_index)) = seek_key {
            let (seek_key, _) = gen_key_value(vnode, key_index);
            let full_key = FullKey {
                user_key: UserKey {
                    table_id: TABLE_ID,
                    table_key: seek_key,
                },
                epoch_with_gap: EpochWithGap::new_from_epoch(EPOCH),
            };
            if let Some(first) = &mut first {
                first.seek(full_key.to_ref()).await.unwrap();
            }
            second.seek(full_key.to_ref()).await.unwrap()
        } else {
            if let Some(first) = &mut first {
                first.rewind().await.unwrap();
            }
            second.rewind().await.unwrap();
        }

        if let Some(first) = &mut first {
            while first.is_valid() {
                assert!(second.is_valid());
                let first_key = first.key();
                let second_key = second.key();
                assert_eq!(first_key, second_key);
                assert_eq!(first.value(), second.value());
                first.next().await.unwrap();
                second.next().await.unwrap();
            }
        }
        assert!(!second.is_valid());
    }

    fn build_batch(
        pairs: impl Iterator<Item = (TableKey<Bytes>, SharedBufferValue<Bytes>)>,
    ) -> Option<SharedBufferBatch> {
        let pairs: Vec<_> = pairs.collect();
        if pairs.is_empty() {
            None
        } else {
            Some(SharedBufferBatch::for_test(pairs, EPOCH, TABLE_ID))
        }
    }

    fn filter_with_watermarks(
        iter: impl Iterator<Item = (TableKey<Bytes>, SharedBufferValue<Bytes>)>,
        table_watermarks: ReadTableWatermark,
    ) -> impl Iterator<Item = (TableKey<Bytes>, SharedBufferValue<Bytes>)> {
        iter.filter(move |(key, _)| {
            if let Some(watermark) = table_watermarks.vnode_watermarks.get(&key.vnode_part()) {
                !table_watermarks
                    .direction
                    .filter_by_watermark(key.key_part(), watermark)
            } else {
                true
            }
        })
    }

    fn gen_inner_key(index: usize) -> String {
        format!("key{:5}", index)
    }

    async fn test_watermark(
        watermarks: impl IntoIterator<Item = (usize, usize)>,
        direction: WatermarkDirection,
    ) {
        let test_index = [(0, 2), (0, 3), (0, 4), (1, 1), (1, 3), (4, 2), (8, 1)];
        let items = test_index
            .iter()
            .map(|(vnode, key_index)| gen_key_value(*vnode, *key_index))
            .collect_vec();

        let read_watermark = ReadTableWatermark {
            direction,
            vnode_watermarks: BTreeMap::from_iter(watermarks.into_iter().map(
                |(vnode, key_index)| {
                    (
                        VirtualNode::from_index(vnode),
                        Bytes::from(gen_inner_key(key_index)),
                    )
                },
            )),
        };

        let gen_iters = || {
            let batch = build_batch(filter_with_watermarks(
                items.clone().into_iter(),
                read_watermark.clone(),
            ));
            let iter = SkipWatermarkIterator::new(
                build_batch(items.clone().into_iter())
                    .unwrap()
                    .into_forward_iter(),
                BTreeMap::from_iter(once((TABLE_ID, read_watermark.clone()))),
            );
            (batch.map(|batch| batch.into_forward_iter()), iter)
        };
        let (first, second) = gen_iters();
        assert_iter_eq(first, second, None).await;
        for (vnode, key_index) in &test_index {
            let (first, second) = gen_iters();
            assert_iter_eq(first, second, Some((*vnode, *key_index))).await;
        }
        let (last_vnode, last_key_index) = test_index.last().unwrap();
        let (first, second) = gen_iters();
        assert_iter_eq(first, second, Some((*last_vnode, last_key_index + 1))).await;
    }

    fn gen_key_value(vnode: usize, index: usize) -> (TableKey<Bytes>, SharedBufferValue<Bytes>) {
        (
            gen_key_from_str(VirtualNode::from_index(vnode), &gen_inner_key(index)),
            SharedBufferValue::Insert(Bytes::copy_from_slice(
                format!("{}-value-{}", vnode, index).as_bytes(),
            )),
        )
    }

    #[tokio::test]
    async fn test_no_watermark() {
        test_watermark(empty(), WatermarkDirection::Ascending).await;
        test_watermark(empty(), WatermarkDirection::Descending).await;
    }

    #[tokio::test]
    async fn test_too_low_watermark() {
        test_watermark(vec![(0, 0)], WatermarkDirection::Ascending).await;
        test_watermark(vec![(0, 10)], WatermarkDirection::Descending).await;
    }

    #[tokio::test]
    async fn test_single_watermark() {
        test_watermark(vec![(0, 3)], WatermarkDirection::Ascending).await;
        test_watermark(vec![(0, 3)], WatermarkDirection::Descending).await;
    }

    #[tokio::test]
    async fn test_watermark_vnode_no_data() {
        test_watermark(vec![(3, 3)], WatermarkDirection::Ascending).await;
        test_watermark(vec![(3, 3)], WatermarkDirection::Descending).await;
    }

    #[tokio::test]
    async fn test_filter_all() {
        test_watermark(
            vec![(0, 5), (1, 4), (2, 0), (4, 3), (8, 2)],
            WatermarkDirection::Ascending,
        )
        .await;
        test_watermark(
            vec![(0, 0), (1, 0), (2, 0), (4, 0), (8, 0)],
            WatermarkDirection::Descending,
        )
        .await;
    }

    #[tokio::test]
    async fn test_advance_multi_vnode() {
        test_watermark(vec![(1, 2), (8, 0)], WatermarkDirection::Ascending).await;
    }
}
