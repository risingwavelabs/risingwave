// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;

use async_trait::async_trait;
use futures::{pin_mut, StreamExt};
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::Op::{Delete, Insert, UpdateDelete, UpdateInsert};
use risingwave_common::array::{Array, ArrayImpl, Row, Utf8Array};
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{option_to_owned_scalar, Datum, ScalarImpl, ScalarRef};
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::ManagedTableState;
use crate::executor::error::StreamExecutorResult;
use crate::executor::PkDataTypes;

pub struct ManagedStringAggState<S: StateStore> {
    // cache: BTreeMap if order by, Vec otherwise
    _phantom_data: PhantomData<S>,

    /// The upstream pk. Assembled as pk of relational table.
    upstream_pk_len: usize,
}

impl<S: StateStore> ManagedStringAggState<S> {
    pub fn new(pk_data_types: PkDataTypes) -> StreamExecutorResult<Self> {
        Ok(Self {
            _phantom_data: PhantomData,
            upstream_pk_len: pk_data_types.len(),
        })
    }

    fn make_state_row(&self, value: Option<String>, pk_values: Vec<Datum>) -> Row {
        let mut row_vals = pk_values.clone();
        row_vals.push(value.map(Into::into));
        Row::new(row_vals)
    }

    fn decompose_state_row(&self, row: &Row) -> (Option<String>, Vec<Datum>) {
        let value = row[row.size() - 1].clone().map(|x| match x {
            ScalarImpl::Utf8(s) => s,
            _ => panic!("Expected Utf8"),
        });
        let pk = row.values().take(self.upstream_pk_len).cloned().collect();
        (value, pk)
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedStringAggState<S> {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        epoch: u64,
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));
        println!(
            "[rc] ManagedStringAggState::apply_batch, ops: {:?}, visibility: {:?}, data: {:?}",
            ops, visibility, data
        );

        let agg_column: &Utf8Array = data[0].into();
        let pk_columns = (0..self.upstream_pk_len)
            .map(|idx| data[idx + 1])
            .collect_vec();

        for (id, (op, value)) in ops.iter().zip_eq(agg_column.iter()).enumerate() {
            let visible = visibility.map(|x| x.is_set(id).unwrap()).unwrap_or(true);
            if !visible {
                continue;
            }

            let value = value.map(|x| x.to_owned_scalar().into());
            let pk_values: Vec<Datum> = pk_columns.iter().map(|col| col.datum_at(id)).collect();

            println!(
                "[rc] apply_batch_inner, id: {}, op: {:?}, value: {:?}, pk_values: {:?}",
                id, op, value, pk_values
            );

            let state_row = self.make_state_row(value, pk_values);
            println!("[rc] state_row: {:?}", state_row);

            println!(
                "[rc] state table schema: {:?}, pk indices: {:?}",
                state_table.storage_table().schema(),
                state_table.pk_indices()
            );

            match op {
                Insert | UpdateInsert => {
                    state_table.insert(state_row)?;
                }
                Delete | UpdateDelete => {
                    state_table.delete(state_row)?;
                }
            }
        }

        Ok(())
    }

    async fn get_output(
        &mut self,
        epoch: u64,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<Datum> {
        let all_data_iter = state_table.iter(epoch).await?;
        pin_mut!(all_data_iter);

        let mut agg_result = String::new();

        #[for_await]
        for row in all_data_iter {
            let row = row?;
            let (value, pk_values) = self.decompose_state_row(&row);
            println!("[rc] value: {:?}, pk_values: {:?}", value, pk_values);
            if let Some(s) = value {
                agg_result.push_str(&s);
            }
        }

        Ok(Some(agg_result.into()))
    }

    fn is_dirty(&self) -> bool {
        unreachable!("It seems that this function will never be called.")
    }

    fn flush(&mut self, state_table: &mut StateTable<S>) -> StreamExecutorResult<()> {
        println!("[rc] ManagedStringAggState::flush");
        Ok(())
    }
}
