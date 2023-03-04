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

use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::{bail, row};
use risingwave_storage::StateStore;

use super::approx_distinct_utils::{
    deserialize_buckets_from_list, serialize_buckets, RegisterBucket, StreamingApproxCountDistinct,
};
use crate::common::table::state_table::StateTable;
use crate::executor::aggregation::table::TableStateImpl;
use crate::executor::StreamExecutorResult;

#[derive(Clone, Debug)]
pub(super) struct AppendOnlyRegisterBucket {
    max: u8,
}

impl RegisterBucket for AppendOnlyRegisterBucket {
    fn new() -> Self {
        Self { max: 0 }
    }

    fn update_bucket(&mut self, index: usize, is_insert: bool) -> StreamExecutorResult<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        if !is_insert {
            bail!("HyperLogLog: Deletion in append-only bucket");
        }

        if index as u8 > self.max {
            self.max = index as u8;
        }

        Ok(())
    }

    fn get_max(&self) -> u8 {
        self.max
    }
}

#[derive(Clone, Debug, Default)]
pub struct AppendOnlyStreamingApproxCountDistinct {
    registers: Vec<AppendOnlyRegisterBucket>,

    initial_count: i64,
}

impl StreamingApproxCountDistinct for AppendOnlyStreamingApproxCountDistinct {
    type Bucket = AppendOnlyRegisterBucket;

    fn with_i64(registers_num: u32, initial_count: i64) -> Self {
        Self {
            registers: vec![AppendOnlyRegisterBucket::new(); registers_num as usize],
            initial_count,
        }
    }

    fn get_initial_count(&self) -> i64 {
        self.initial_count
    }

    fn reset_buckets(&mut self, registers_num: u32) {
        self.registers = vec![AppendOnlyRegisterBucket::new(); registers_num as usize];
    }

    fn registers(&self) -> &[AppendOnlyRegisterBucket] {
        &self.registers
    }

    fn registers_mut(&mut self) -> &mut [AppendOnlyRegisterBucket] {
        &mut self.registers
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableStateImpl<S> for AppendOnlyStreamingApproxCountDistinct {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        self.apply_batch_inner(ops, visibility, data)
    }

    fn get_output(&mut self) -> StreamExecutorResult<Datum> {
        self.get_output_inner()
    }

    async fn update_from_state_table(
        &mut self,
        state_table: &StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()> {
        let state_row = {
            let data_iter = state_table.iter_with_pk_prefix(&group_key, false).await?;
            pin_mut!(data_iter);
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };
        if let Some(state_row) = state_row {
            if let ScalarImpl::List(list) = state_row[group_key.len()].as_ref().unwrap() {
                let state = deserialize_buckets_from_list(list.values());
                for (idx, bucket) in self.registers_mut().iter_mut().enumerate() {
                    if state[idx] != 0 {
                        bucket.update_bucket(state[idx] as usize, true)?;
                    }
                }
            } else {
                panic!("The state of append-only ApproxCountDistinct must be List.");
            }
        }
        Ok(())
    }

    async fn flush_state_if_needed(
        &self,
        state_table: &mut StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()> {
        let list = Some(ScalarImpl::List(ListValue::new(
            serialize_buckets(
                &self
                    .registers()
                    .iter()
                    .map(|register| register.get_max())
                    .collect_vec(),
            )
            .into_iter()
            .map(|x| Some(ScalarImpl::Int64(x as i64)))
            .collect_vec(),
        )));
        let current_row = group_key.chain(row::once(list));

        let state_row = {
            let data_iter = state_table.iter_with_pk_prefix(&group_key, false).await?;
            pin_mut!(data_iter);
            if let Some(state_row) = data_iter.next().await {
                Some(state_row?)
            } else {
                None
            }
        };
        match state_row {
            Some(state_row) => {
                state_table.update(state_row, current_row);
            }
            None => {
                state_table.insert(current_row);
            }
        }

        Ok(())
    }
}

impl AppendOnlyStreamingApproxCountDistinct {
    pub fn new() -> Self {
        Self::with_no_initial()
    }
}
