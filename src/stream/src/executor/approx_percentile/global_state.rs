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

use std::collections::Bound;
use std::mem;

use risingwave_common::array::Op;
use risingwave_common::bail;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{Datum, ToOwnedDatum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::executor::prelude::*;
use crate::executor::StreamExecutorResult;

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
        self.count_state_table.init_epoch(init_epoch);
        self.bucket_state_table.init_epoch(init_epoch);

        // Refill row_count
        let row_count_state = self.get_row_count_state().await?;
        let row_count = Self::decode_row_count(&row_count_state)?;

        // Refill cache
        self.refill_cache().await?;

        // Update the last output downstream
        let last_output = if row_count_state.is_none() {
            None
        } else {
            Some(self.cache.get_output(row_count, self.quantile, self.base))
        };
        self.last_output = last_output;
        Ok(())
    }

    async fn refill_cache(&mut self) -> StreamExecutorResult<()> {
        let neg_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (
            Bound::Unbounded,
            Bound::Excluded([Datum::from(ScalarImpl::Int16(0))].to_owned_row()),
        );
        let non_neg_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (
            Bound::Included([Datum::from(ScalarImpl::Int16(0))].to_owned_row()),
            Bound::Unbounded,
        );
        #[for_await]
        for keyed_row in self
            .bucket_state_table
            .rev_iter_with_prefix(&[Datum::None; 0], &neg_bounds, PrefetchOptions::default())
            .await?
            .chain(
                self.bucket_state_table
                    .iter_with_prefix(
                        &[Datum::None; 0],
                        &non_neg_bounds,
                        PrefetchOptions::default(),
                    )
                    .await?,
            )
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
    pub async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        for (_op, row) in chunk.rows() {
            self.apply_row(row).await?;
        }
        Ok(())
    }

    pub async fn apply_row(&mut self, row: impl Row) -> StreamExecutorResult<()> {

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

        let pk = row.project(&[0, 1]);
        let old_row = self.bucket_state_table.get_row(pk).await?;
        let old_bucket_row_count: i64 = if let Some(row) = old_row.as_ref() {
            row.datum_at(2).unwrap().into_int64()
        } else {
            0
        };

        let new_value = old_bucket_row_count.checked_add(delta as i64).unwrap();
        if new_value == 0 {
            self.bucket_state_table.delete(old_row);
        } else {
            let new_value_datum = Datum::from(ScalarImpl::Int64(new_value));
            let new_row = &[sign_datum, bucket_id_datum, new_value_datum];

            if old_row.is_none() {
                self.bucket_state_table.insert(new_row);
            } else {
                self.bucket_state_table.update(old_row, new_row);
            }
        }

        // Update cache
        self.cache.update(sign, bucket_id, new_value);

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
        self.count_state_table.commit(epoch).await?;
        self.bucket_state_table.commit(epoch).await?;
        Ok(())
    }
}

// Read
impl<S: StateStore> GlobalApproxPercentileState<S> {
    pub fn get_output(&mut self) -> StreamChunk {
        let last_output = mem::take(&mut self.last_output);
        let new_output = if !self.output_changed {
            last_output.clone().flatten()
        } else {
            let new_output = self.cache.get_output(self.row_count, self.quantile, self.base);
            self.last_output = Some(new_output.clone());
            new_output
        };
        match last_output {
            None => {
                StreamChunk::from_rows(&[(Op::Insert, &[new_output])], &[DataType::Float64])
            }
            Some(last_output) if !self.output_changed => {
                StreamChunk::from_rows(
                    &[
                        (Op::UpdateDelete, &[last_output.clone()]),
                        (Op::UpdateInsert, &[last_output]),
                    ],
                    &[DataType::Float64],
                )
            },
            Some(last_output) => {
                StreamChunk::from_rows(
                    &[
                        (Op::UpdateDelete, &[last_output.clone()]),
                        (Op::UpdateInsert, &[new_output.clone()]),
                    ],
                    &[DataType::Float64],
                )
            }
        }
    }
}

type Count = i64;
type BucketId = i32;

/// Keeps the entire bucket state table contents in-memory.
struct BucketTableCache {
    neg_buckets: EstimatedBTreeMap<BucketId, Count>,
    zeros: Count,
    pos_buckets: EstimatedBTreeMap<BucketId, Count>,
}

impl BucketTableCache {
    pub fn new() -> Self {
        Self {
            neg_buckets: EstimatedBTreeMap::new(),
            zeros: 0,
            pos_buckets: EstimatedBTreeMap::new(),
        }
    }

    /// TODO(kwannoel): Track if we updated the input, if we didn't, we can return the last output.
    pub fn get_output(&self, row_count: i64, quantile: f64, base: f64) -> Datum {
        let quantile_count = (row_count as f64 * quantile).floor() as i64;
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
        for (bucket_id, count) in self.pos_buckets.iter() {
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

    pub fn update(&mut self, sign: i16, bucket_id: i32, new_value: i64) {
        match sign {
            -1 => {
                if new_value == 0 {
                    self.neg_buckets.remove(&bucket_id);
                } else {
                    self.neg_buckets.insert(bucket_id, new_value);
                }
            }
            0 => {
                self.zeros = new_value;
            }
            1 => {
                if new_value == 0 {
                    self.pos_buckets.remove(&bucket_id);
                } else {
                    self.pos_buckets.insert(bucket_id, new_value);
                }
            }
            _ => {
                panic!("Invalid sign: {}", sign);
            }
        }
    }
}
