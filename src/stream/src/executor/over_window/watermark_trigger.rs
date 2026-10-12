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

use std::sync::Arc;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_expr::window_function::WatermarkBound;
use risingwave_storage::StateStore;

use crate::common::table::state_table::{StateTable, StateTablePostCommit};
use crate::executor::StreamExecutorResult;
use crate::executor::eowc::BufferTableCache;

/// A partition to check once the watermark of the order key satisfies the bound.
#[derive(Clone, Debug, EstimateSize)]
pub(super) struct TriggerEntry {
    pub bound: WatermarkBound,
    pub partition_key: OwnedRow,
}

/// The triggers of the partitions whose next rows to output may be closed by watermark, indexed by
/// bound, so that a watermark only visits the partitions whose triggers it satisfies.
pub(super) struct WatermarkTriggers<S: StateStore> {
    /// See `StreamEowcOverWindow::infer_watermark_trigger_table` for the schema.
    table: StateTable<S>,
    /// The smallest entries of `table`, keyed by the memcmp-encoded pk.
    cache: BufferTableCache<MemcmpEncoded, TriggerEntry>,
    order_key_data_type: DataType,
    /// The latest watermark of the order key, with its memcmp encoding.
    watermark: Option<(ScalarImpl, MemcmpEncoded)>,
    /// The table row of the last entry popped for the current watermark, after which the cache is
    /// refilled to skip the entries deleted before.
    last_popped: Option<OwnedRow>,
}

impl<S: StateStore> WatermarkTriggers<S> {
    pub fn new(table: StateTable<S>, order_key_data_type: DataType) -> Self {
        Self {
            table,
            cache: BufferTableCache::new(),
            order_key_data_type,
            watermark: None,
            last_popped: None,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.table.init_epoch(epoch).await
    }

    /// The latest watermark of the order key, memcmp-encoded, which is unknown after recovery until
    /// the next watermark arrives.
    pub fn watermark(&self) -> Option<&MemcmpEncoded> {
        self.watermark.as_ref().map(|(_, encoded)| encoded)
    }

    /// Returns whether the watermark advances.
    pub fn advance_watermark(&mut self, watermark: ScalarImpl) -> StreamExecutorResult<bool> {
        let encoded =
            memcmp_encoding::encode_value(Some(watermark.clone()), OrderType::ascending())?;
        if self.watermark().is_some_and(|w| w >= &encoded) {
            return Ok(false);
        }
        self.watermark = Some((watermark, encoded));
        self.last_popped = None;
        Ok(true)
    }

    pub fn register(&mut self, entry: TriggerEntry) {
        let row = self.table_row(&entry);
        self.table.insert(&row);
        self.cache.insert(cache_key(&self.table, &row), entry);
    }

    /// Pop at most `limit` entries satisfied by the watermark, in the order of their bounds, and
    /// delete them from the table.
    pub async fn pop_due(&mut self, limit: usize) -> StreamExecutorResult<Vec<TriggerEntry>> {
        let Some((_, watermark)) = self.watermark.clone() else {
            return Ok(vec![]);
        };
        let mut due = vec![];
        while due.len() < limit {
            if !self.cache.is_synced() {
                self.refill_cache().await?;
            }
            match self
                .cache
                .pop_first_if(|_, entry| entry.bound.is_satisfied_by(&watermark))
            {
                Some((_, entry)) => {
                    let row = self.table_row(&entry);
                    self.table.delete(&row);
                    self.last_popped = Some(row);
                    due.push(entry);
                }
                // The cache runs out, so refill it.
                None if !self.cache.is_synced() => {}
                None => break,
            }
        }
        Ok(due)
    }

    /// Clean the entries below the watermark from the table, after all satisfied ones are popped.
    pub fn clean(&mut self) {
        if let Some((watermark, _)) = &self.watermark {
            self.table.update_watermark(watermark.clone());
        }
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.table.try_flush().await
    }

    pub async fn commit(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<TriggersPostCommit<'_, S>> {
        Ok(TriggersPostCommit {
            post_commit: self.table.commit(epoch).await?,
            cache: &mut self.cache,
        })
    }

    async fn refill_cache(&mut self) -> StreamExecutorResult<()> {
        let table = &self.table;
        let complete = self
            .cache
            .refill(table, self.last_popped.clone(), |row| {
                let entry = TriggerEntry {
                    bound: WatermarkBound {
                        value: memcmp_encoding::encode_value(
                            row.datum_at(0),
                            OrderType::ascending(),
                        )?,
                        exclusive: row
                            .datum_at(1)
                            .expect("trigger_exclusive must be non-NULL")
                            .into_bool(),
                    },
                    partition_key: OwnedRow::new(row.as_inner()[2..].to_vec()),
                };
                Ok((cache_key(table, &row), entry))
            })
            .await?;
        if let Some(count) = complete {
            // The entries before `last_popped` are all deleted, so the cache holds all entries.
            self.cache.set_table_row_count(count);
        }
        Ok(())
    }

    fn table_row(&self, entry: &TriggerEntry) -> OwnedRow {
        let value = memcmp_encoding::decode_value(
            &self.order_key_data_type,
            &entry.bound.value,
            OrderType::ascending(),
        )
        .expect("trigger bound must be encoded from an order value");
        row::once(value)
            .chain(row::once(Some(ScalarImpl::Bool(entry.bound.exclusive))))
            .chain(&entry.partition_key)
            .into_owned_row()
    }
}

fn cache_key<S: StateStore>(table: &StateTable<S>, row: &OwnedRow) -> MemcmpEncoded {
    let mut key = vec![];
    table
        .pk_serde()
        .serialize(row.project(table.pk_indices()), &mut key);
    key.into()
}

pub(super) struct TriggersPostCommit<'a, S: StateStore> {
    post_commit: StateTablePostCommit<'a, S>,
    cache: &'a mut BufferTableCache<MemcmpEncoded, TriggerEntry>,
}

impl<S: StateStore> TriggersPostCommit<'_, S> {
    pub async fn post_yield_barrier(
        self,
        new_vnodes: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<()> {
        if self
            .post_commit
            .post_yield_barrier(new_vnodes)
            .await?
            .is_some()
        {
            // The cached entries may be of the vnodes no longer owned.
            self.cache.clear();
        }
        Ok(())
    }
}
