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
use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, WatermarkDirection};

use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

pub struct SkipWatermarkIterator<I> {
    inner: I,
    watermarks: BTreeMap<TableId, ReadTableWatermark>,
    remain_watermarks: VecDeque<(TableId, VirtualNode, WatermarkDirection, Bytes)>,
}

impl<I: HummockIterator<Direction = Forward>> SkipWatermarkIterator<I> {
    pub fn new(inner: I, watermarks: BTreeMap<TableId, ReadTableWatermark>) -> Self {
        Self {
            inner,
            remain_watermarks: VecDeque::new(),
            watermarks,
        }
    }

    fn reset_watermark(&mut self) {
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
    fn advance_watermark(&mut self) -> bool {
        let mut changed = false;
        if self.inner.is_valid() {
            let key = self.inner.key();
            let key_table_id = key.user_key.table_id;
            let key_vnode = key.user_key.table_key.vnode_part();
            let inner_key = key.user_key.table_key.key_part();
            while let Some((table_id, vnode, direction, watermark)) = self.remain_watermarks.front()
            {
                match (table_id, vnode).cmp(&(&key_table_id, &key_vnode)) {
                    Ordering::Less => {
                        self.remain_watermarks.pop_front();
                        changed = true;
                        continue;
                    }
                    Ordering::Equal => {
                        // when watermark is ascending and the inner key has passed the watermark,
                        // the watermark is not likely to take effect to current or future key,
                        // and we can skip the watermark.
                        if *direction == WatermarkDirection::Ascending
                            && inner_key >= watermark.as_ref()
                        {
                            self.remain_watermarks.pop_front();
                            changed = true;
                            continue;
                        } else {
                            break;
                        }
                    }
                    Ordering::Greater => {
                        break;
                    }
                }
            }
        }
        changed
    }

    /// Advance the key until iterator invalid or the current key will not be filtered by the latest watermark.
    /// Calling this method should ensure that the first remaining watermark has been advanced to the current key
    async fn advance_key(&mut self) -> HummockResult<bool> {
        let mut changed = false;
        if let Some((table_id, vnode, direction, watermark)) = self.remain_watermarks.front() {
            while self.inner.is_valid() {
                let skip = {
                    let key = self.inner.key();
                    let key_table_id = key.user_key.table_id;
                    let key_vnode = key.user_key.table_key.vnode_part();
                    let inner_key = key.user_key.table_key.key_part();
                    match (&key_table_id, &key_vnode).cmp(&(table_id, vnode)) {
                        Ordering::Less => false,
                        Ordering::Equal => match direction {
                            WatermarkDirection::Ascending => inner_key < watermark.as_ref(),
                            WatermarkDirection::Descending => inner_key > watermark.as_ref(),
                        },
                        Ordering::Greater => {
                            unreachable!("watermark should be advanced to current key")
                        }
                    }
                };
                if skip {
                    self.inner.next().await?;
                    changed = true;
                } else {
                    break;
                }
            }
        }
        Ok(changed)
    }

    async fn advance_key_and_watermark(&mut self) -> HummockResult<()> {
        // here we assume that before calling this method, it was an operation on key,
        // so we call advance_watermark anyway at first.
        self.advance_watermark();
        loop {
            // advance key and watermark in an interleave manner until nothing
            // changed after the method is called.
            if !self.advance_key().await? {
                break;
            }
            if !self.advance_watermark() {
                break;
            }
        }
        Ok(())
    }
}

impl<I: HummockIterator<Direction = Forward>> HummockIterator for SkipWatermarkIterator<I> {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.inner.next().await?;
        self.advance_key_and_watermark().await?;
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
        self.inner.rewind().await?;
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        self.reset_watermark();
        self.inner.seek(key).await?;
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_basic() {}
}
