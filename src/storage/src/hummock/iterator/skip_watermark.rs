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
use std::collections::VecDeque;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, WatermarkDirection};

use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

pub struct SkipWatermarkIterator<I> {
    inner: I,
    watermarks: Vec<(TableId, ReadTableWatermark)>,
    remain_watermarks: VecDeque<(TableId, ReadTableWatermark)>,
}

impl<I: HummockIterator<Direction = Forward>> SkipWatermarkIterator<I> {
    pub fn new(inner: I, watermarks: Vec<(TableId, ReadTableWatermark)>) -> Self {
        assert!(watermarks.is_sorted_by_key(|(table_id, _)| table_id));
        Self {
            inner,
            remain_watermarks: VecDeque::with_capacity(watermarks.len()),
            watermarks,
        }
    }

    async fn advance_key_and_watermark(&mut self) -> HummockResult<()> {
        loop {
            if !self.inner.is_valid() {
                return Ok(());
            }
            while let Some((watermark_table_id, table_read_watermarks)) =
                self.remain_watermarks.front_mut()
            {
                match (*watermark_table_id).cmp(&self.inner.key().user_key.table_id) {
                    Ordering::Less => {
                        // Current key has advanced the least watermark. See the next watermark
                        self.remain_watermarks.pop_front();
                        continue;
                    }
                    Ordering::Equal => {
                        while let Some((vnode, watermark)) =
                            table_read_watermarks.vnode_watermarks.front_mut()
                        {
                            let key_vnode = self.inner.key().user_key.table_key.vnode_part();
                            match (*vnode).cmp(&key_vnode) {
                                Ordering::Less => {
                                    // Current key has advanced the least watermark. See the next watermark
                                    let _ = table_read_watermarks.vnode_watermarks.pop_front();
                                }
                                Ordering::Equal => {
                                    loop {
                                        if !self.inner.is_valid() {
                                            return Ok(());
                                        }
                                        let skip_key = {
                                            let key = self.inner.key();
                                            let key_vnode = key.user_key.table_key.vnode_part();
                                            if key_vnode > *vnode {
                                                // key has advanced to a new vnode
                                                let _ = table_read_watermarks
                                                    .vnode_watermarks
                                                    .pop_front();
                                                break;
                                            }
                                            let inner_table_key = key.user_key.table_key.key_part();
                                            match table_read_watermarks.direction {
                                                WatermarkDirection::Ascending => {
                                                    inner_table_key < watermark.as_ref()
                                                }
                                                WatermarkDirection::Descending => {
                                                    inner_table_key > watermark.as_ref()
                                                }
                                            }
                                        };
                                        if skip_key {
                                            self.inner.next().await?;
                                            continue;
                                        } else {
                                            return Ok(());
                                        }
                                    }
                                }
                                Ordering::Greater => {
                                    // Current key has not reached the least watermark yet. No need to advance any more.
                                    return Ok(());
                                }
                            }
                        }
                        if table_read_watermarks.vnode_watermarks.is_empty() {
                            self.remain_watermarks.pop_front();
                        }
                    }
                    Ordering::Greater => {
                        // Current key has not reached the latest watermark yet. No need to advance any more.
                        return Ok(());
                    }
                }
            }
        }
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
        self.inner.rewind().await?;
        self.remain_watermarks = self.watermarks.iter().cloned().collect();
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        self.inner.seek(key).await?;
        self.remain_watermarks = self.watermarks.iter().cloned().collect();
        self.advance_key_and_watermark().await?;
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats)
    }
}
