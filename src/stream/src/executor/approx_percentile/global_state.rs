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

use std::collections::{BTreeMap, Bound};
use std::mem;

use risingwave_common::array::Op;
use risingwave_common::bail;
use risingwave_common::row::Row;
use risingwave_common::types::{Datum, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use crate::executor::StreamExecutorResult;
use crate::executor::prelude::*;

/// The global approx percentile state.
pub struct GlobalApproxPercentileState<S: StateStore> {
    quantile: f64,
    base: f64,
    row_count: i64,
    bucket_state_table: StateTable<S>,
    count_state_table: StateTable<S>,
    cache: BucketTableCache,
    last_output: Option<Datum>,
    output_changed: bool,
}

// Initialization
impl<S: StateStore> GlobalApproxPercentileState<S> {
    pub fn new(
        quantile: f64,
        base: f64,
        bucket_state_table: StateTable<S>,
        count_state_table: StateTable<S>,
    ) -> Self {
        Self {
            quantile,
            base,
            row_count: 0,
            bucket_state_table,
            count_state_table,
            cache: BucketTableCache::new(),
            last_output: None,
            output_changed: false,
        }
    }

    pub async fn init(&mut self, init_epoch: EpochPair) -> StreamExecutorResult<()> {
        // Init state tables.
        self.count_state_table.init_epoch(init_epoch).await?;
        self.bucket_state_table.init_epoch(init_epoch).await?;

        // Refill row_count
        let row_count_state = self.get_row_count_state().await?;
        let row_count = Self::decode_row_count(&row_count_state)?;
        self.row_count = row_count;
        tracing::debug!(?row_count, "recovered row_count");

        // Refill cache
        self.refill_cache().await?;

        // Update the last output downstream
        let last_output = if row_count_state.is_none() {
            None
        } else {
            Some(self.cache.get_output(row_count, self.quantile, self.base))
        };
        tracing::debug!(?last_output, "recovered last_output");
        self.last_output = last_output;
        Ok(())
    }

    async fn refill_cache(&mut self) -> StreamExecutorResult<()> {
        let bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (Bound::Unbounded, Bound::Unbounded);
        #[for_await]
        for keyed_row in self
            .bucket_state_table
            .iter_with_prefix(&[Datum::None; 0], &bounds, PrefetchOptions::default())
            .await?
        {
            let row = keyed_row?.into_owned_row();
            let sign = row.datum_at(0).unwrap().into_int16();
            let bucket_id = row.datum_at(1).unwrap().into_int32();
            let count = row.datum_at(2).unwrap().into_int64();
            match sign {
                -1 => {
                    self.cache.neg_buckets.insert(bucket_id, count as i64);
                }
                0 => {
                    self.cache.zeros = count as i64;
                }
                1 => {
                    self.cache.pos_buckets.insert(bucket_id, count as i64);
                }
                _ => {
                    bail!("Invalid sign: {}", sign);
                }
            }
        }
        Ok(())
    }

    async fn get_row_count_state(&self) -> StreamExecutorResult<Option<OwnedRow>> {
        self.count_state_table.get_row(&[Datum::None; 0]).await
    }

    fn decode_row_count(row_count_state: &Option<OwnedRow>) -> StreamExecutorResult<i64> {
        if let Some(row) = row_count_state.as_ref() {
            let Some(datum) = row.datum_at(0) else {
                bail!("Invalid row count state: {:?}", row)
            };
            Ok(datum.into_int64())
        } else {
            Ok(0)
        }
    }
}

// Update
impl<S: StateStore> GlobalApproxPercentileState<S> {
    pub fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        // Op is ignored here, because we only check the `delta` column inside the row.
        // The sign of the `delta` column will tell us if we need to decrease or increase the
        // count of the bucket.
        for (_op, row) in chunk.rows() {
            debug_assert_eq!(_op, Op::Insert);
            self.apply_row(row)?;
        }
        Ok(())
    }

    pub fn apply_row(&mut self, row: impl Row) -> StreamExecutorResult<()> {
        // Decoding
        let sign_datum = row.datum_at(0);
        let sign = sign_datum.unwrap().into_int16();
        let sign_datum = sign_datum.to_owned_datum();
        let bucket_id_datum = row.datum_at(1);
        let bucket_id = bucket_id_datum.unwrap().into_int32();
        let bucket_id_datum = bucket_id_datum.to_owned_datum();
        let delta_datum = row.datum_at(2);
        let delta: i32 = delta_datum.unwrap().into_int32();

        if delta == 0 {
            return Ok(());
        }

        self.output_changed = true;

        // Updates
        self.row_count = self.row_count.checked_add(delta as i64).unwrap();
        tracing::debug!("updated row_count: {}", self.row_count);

        let (is_new_entry, old_count, new_count) = match sign {
            -1 => {
                let count_entry = self.cache.neg_buckets.get(&bucket_id).copied();
                let old_count = count_entry.unwrap_or(0);
                let new_count = old_count.checked_add(delta as i64).unwrap();
                let is_new_entry = count_entry.is_none();
                if new_count != 0 {
                    self.cache.neg_buckets.insert(bucket_id, new_count);
                } else {
                    self.cache.neg_buckets.remove(&bucket_id);
                }
                (is_new_entry, old_count, new_count)
            }
            0 => {
                let old_count = self.cache.zeros;
                let new_count = old_count.checked_add(delta as i64).unwrap();
                let is_new_entry = old_count == 0;
                if new_count != 0 {
                    self.cache.zeros = new_count;
                }
                (is_new_entry, old_count, new_count)
            }
            1 => {
                let count_entry = self.cache.pos_buckets.get(&bucket_id).copied();
                let old_count = count_entry.unwrap_or(0);
                let new_count = old_count.checked_add(delta as i64).unwrap();
                let is_new_entry = count_entry.is_none();
                if new_count != 0 {
                    self.cache.pos_buckets.insert(bucket_id, new_count);
                } else {
                    self.cache.pos_buckets.remove(&bucket_id);
                }
                (is_new_entry, old_count, new_count)
            }
            _ => bail!("Invalid sign: {}", sign),
        };

        let old_row = &[
            sign_datum.clone(),
            bucket_id_datum.clone(),
            Datum::from(ScalarImpl::Int64(old_count)),
        ];
        if new_count == 0 && !is_new_entry {
            self.bucket_state_table.delete(old_row);
        } else if new_count > 0 {
            let new_row = &[
                sign_datum,
                bucket_id_datum,
                Datum::from(ScalarImpl::Int64(new_count)),
            ];
            if is_new_entry {
                self.bucket_state_table.insert(new_row);
            } else {
                self.bucket_state_table.update(old_row, new_row);
            }
        } else {
            bail!("invalid state, new_count = 0 and is_new_entry is true")
        }

        Ok(())
    }

    pub async fn commit(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // Commit row count state.
        let row_count_datum = Datum::from(ScalarImpl::Int64(self.row_count));
        let row_count_row = &[row_count_datum];
        let last_row_count_state = self.count_state_table.get_row(&[Datum::None; 0]).await?;
        match last_row_count_state {
            None => self.count_state_table.insert(row_count_row),
            Some(last_row_count_state) => self
                .count_state_table
                .update(last_row_count_state, row_count_row),
        }
        self.count_state_table
            .commit_assert_no_update_vnode_bitmap(epoch)
            .await?;
        self.bucket_state_table
            .commit_assert_no_update_vnode_bitmap(epoch)
            .await?;
        Ok(())
    }
}

// Read
impl<S: StateStore> GlobalApproxPercentileState<S> {
    pub fn get_output(&mut self) -> StreamChunk {
        let last_output = mem::take(&mut self.last_output);
        let new_output = if !self.output_changed {
            tracing::debug!("last_output: {:#?}", last_output);
            last_output.clone().flatten()
        } else {
            self.cache
                .get_output(self.row_count, self.quantile, self.base)
        };
        self.last_output = Some(new_output.clone());
        let output_chunk = match last_output {
            None => StreamChunk::from_rows(&[(Op::Insert, &[new_output])], &[DataType::Float64]),
            Some(last_output) if !self.output_changed => StreamChunk::from_rows(
                &[
                    (Op::UpdateDelete, std::slice::from_ref(&last_output)),
                    (Op::UpdateInsert, std::slice::from_ref(&last_output)),
                ],
                &[DataType::Float64],
            ),
            Some(last_output) => StreamChunk::from_rows(
                &[
                    (Op::UpdateDelete, std::slice::from_ref(&last_output)),
                    (Op::UpdateInsert, std::slice::from_ref(&new_output)),
                ],
                &[DataType::Float64],
            ),
        };
        tracing::debug!("get_output: {:#?}", output_chunk,);
        self.output_changed = false;
        output_chunk
    }
}

type Count = i64;
type BucketId = i32;

type BucketMap = BTreeMap<BucketId, Count>;

/// Keeps the entire bucket state table contents in-memory.
struct BucketTableCache {
    neg_buckets: BucketMap,
    zeros: Count, // If Count is 0, it means this bucket has not be inserted into before.
    pos_buckets: BucketMap,
}

impl BucketTableCache {
    pub fn new() -> Self {
        Self {
            neg_buckets: BucketMap::new(),
            zeros: 0,
            pos_buckets: BucketMap::new(),
        }
    }

    pub fn get_output(&self, row_count: i64, quantile: f64, base: f64) -> Datum {
        let quantile_count = ((row_count - 1) as f64 * quantile).floor() as i64;
        let mut acc_count = 0;
        for (bucket_id, count) in self.neg_buckets.iter().rev() {
            acc_count += count;
            if acc_count > quantile_count {
                // approx value = -2 * y^i / (y + 1)
                let approx_percentile = -2.0 * base.powi(*bucket_id) / (base + 1.0);
                let approx_percentile = ScalarImpl::Float64(approx_percentile.into());
                return Datum::from(approx_percentile);
            }
        }
        acc_count += self.zeros;
        if acc_count > quantile_count {
            return Datum::from(ScalarImpl::Float64(0.0.into()));
        }
        for (bucket_id, count) in &self.pos_buckets {
            acc_count += count;
            if acc_count > quantile_count {
                // approx value = 2 * y^i / (y + 1)
                let approx_percentile = 2.0 * base.powi(*bucket_id) / (base + 1.0);
                let approx_percentile = ScalarImpl::Float64(approx_percentile.into());
                return Datum::from(approx_percentile);
            }
        }
        Datum::None
    }
}
