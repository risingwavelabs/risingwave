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

use std::mem::size_of;
use std::ops::Range;

use anyhow::Context as _;
use risingwave_common::array::{ListValue, Op, RowRef, StreamChunk, StructValue};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, ScalarImpl, StructType, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_estimate_size::EstimateSize;

use super::{
    AggCall, AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction,
    build_append_only,
};
use crate::Result;

/// State table column type of [`ListStateAgg`]: a list of `(order key, args...)` structs.
pub fn list_state_type(arg_types: &[DataType]) -> DataType {
    DataType::Struct(StructType::unnamed(
        std::iter::once(DataType::Bytea).chain(arg_types.iter().cloned()),
    ))
    .list()
}

/// Build a [`ListStateAgg`] around the append-only version of `agg`.
pub fn build_list_state(agg: &AggCall) -> Result<BoxedAggregateFunction> {
    let (order_col_indices, order_types) = agg
        .column_orders
        .iter()
        .map(|c| (c.column_index, c.order_type))
        .unzip();
    Ok(Box::new(ListStateAgg {
        inner: build_append_only(agg)?,
        elem_type: list_state_type(agg.args.arg_types())
            .into_list()
            .into_elem(),
        arg_types: agg.args.arg_types().to_vec(),
        arg_indices: agg.args.val_indices().to_vec(),
        order_col_indices,
        order_types,
    }))
}

/// Makes an append-only aggregate function (e.g. `array_agg`, `string_agg`) retractable by
/// keeping every input row in the state, sorted by the memcomparable `ORDER BY` key, and replaying
/// them into the inner function on `get_result`.
///
/// Unlike the materialized-input state, this receives the *unprojected* input chunk and projects
/// the arguments itself, since it also needs the `ORDER BY` columns. Rows with equal order keys
/// (always the case without `ORDER BY`) keep arrival order; a retraction removes the first row
/// equal to the retracted one.
pub struct ListStateAgg {
    inner: BoxedAggregateFunction,
    elem_type: DataType,
    arg_types: Vec<DataType>,
    arg_indices: Vec<usize>,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
}

type OrderKey = Box<[u8]>;

#[derive(Debug, Default)]
struct State {
    /// Sorted by key; ties in arrival order.
    rows: Vec<(OrderKey, OwnedRow)>,
    heap_size: usize,
}

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.rows.capacity() * size_of::<(OrderKey, OwnedRow)>() + self.heap_size
    }
}

impl AggStateDyn for State {}

impl State {
    fn insert(&mut self, key: OrderKey, row: OwnedRow) {
        self.heap_size += key.len() + row.estimated_heap_size();
        let pos = self.rows.partition_point(|(k, _)| *k <= key);
        self.rows.insert(pos, (key, row));
    }

    fn retract(&mut self, key: &[u8], row: &OwnedRow) {
        let start = self.rows.partition_point(|(k, _)| **k < *key);
        let found = self.rows[start..]
            .iter()
            .take_while(|(k, _)| **k == *key)
            .position(|(_, r)| r == row);
        // Retracting a row that was never inserted means an inconsistent upstream; ignore it,
        // like the materialized-input state does in release builds.
        if let Some(offset) = found {
            let (key, row) = self.rows.remove(start + offset);
            self.heap_size -= key.len() + row.estimated_heap_size();
        }
    }
}

impl ListStateAgg {
    fn apply_row(&self, state: &mut State, op: Op, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .context("failed to encode order key")?;
        let args = row.project(&self.arg_indices).to_owned_row();
        match op {
            Op::Insert | Op::UpdateInsert => state.insert(key.into(), args),
            Op::Delete | Op::UpdateDelete => state.retract(&key, &args),
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AggregateFunction for ListStateAgg {
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(Box::new(State::default())))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<State>();
        for (op, row) in input.rows() {
            self.apply_row(state, op, row)?;
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<State>();
        for (op, row) in input.rows_in(range) {
            self.apply_row(state, op, row)?;
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let mut inner_state = self.inner.create_state()?;
        let mut builder = DataChunkBuilder::new(self.arg_types.clone(), 1024);
        for (_, row) in &state.rows {
            if let Some(chunk) = builder.append_one_row(row) {
                self.inner
                    .update(&mut inner_state, &StreamChunk::from(chunk))
                    .await?;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            self.inner
                .update(&mut inner_state, &StreamChunk::from(chunk))
                .await?;
        }
        self.inner.get_result(&inner_state).await
    }

    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<State>();
        let elems = state.rows.iter().map(|(key, row)| {
            let fields = std::iter::once(Some(ScalarImpl::Bytea(key.clone())))
                .chain(row.iter().map(ToOwnedDatum::to_owned_datum))
                .collect();
            Some(ScalarImpl::Struct(StructValue::new(fields)))
        });
        Ok(Some(
            ListValue::from_datum_iter(&self.elem_type, elems).into(),
        ))
    }

    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let mut state = State::default();
        if let Some(list) = datum {
            for elem in list.as_list().iter() {
                let mut fields = elem
                    .expect("non-null element")
                    .into_struct()
                    .iter_fields_ref();
                let key: OrderKey = fields
                    .next()
                    .expect("order key field")
                    .expect("non-null order key")
                    .into_bytea()
                    .into();
                let row = OwnedRow::new(fields.map(ToOwnedDatum::to_owned_datum).collect());
                state.heap_size += key.len() + row.estimated_heap_size();
                state.rows.push((key, row));
            }
        }
        Ok(AggregateState::Any(Box::new(state)))
    }
}
