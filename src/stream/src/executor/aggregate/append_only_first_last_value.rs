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

use bytes::{Buf, BufMut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding;
use risingwave_expr::aggregate::{
    AggCall, AggStateDyn, AggregateFunction, AggregateState, BoxedAggregateFunction, PbAggKind,
};
use risingwave_expr::{ExprError, Result};

/// State for append-only `first_value`/`last_value` with ORDER BY.
///
/// We keep only one candidate per group:
/// - `best_key`: memcmp-encoded bytes for (ORDER BY columns [+ stream key tie-break]).
/// - `best_value`: the corresponding value to output.
#[derive(Debug, Default, Clone, EstimateSize)]
pub struct AppendOnlyFirstLastValueState {
    best_key: Option<Vec<u8>>,
    best_value: Datum,
}

impl AggStateDyn for AppendOnlyFirstLastValueState {}

/// Aggregate function for append-only `first_value`/`last_value`.
///
/// IMPORTANT: the `input` chunk passed to `update` must be UNPROJECTED, as we need to read
/// ORDER BY columns and stream key columns in addition to the value argument.
pub struct AppendOnlyFirstLastValueAgg {
    return_type: DataType,
    value_col_idx: usize,
    key_col_indices: Vec<usize>,
    key_serializer: OrderedRowSerde,
}

impl AppendOnlyFirstLastValueAgg {
    pub fn new(
        kind: PbAggKind,
        value_col_idx: usize,
        user_order_by: &[ColumnOrder],
        stream_key: &[usize],
        input_schema: &Schema,
        return_type: DataType,
    ) -> Self {
        let (order_col_indices, order_types): (Vec<usize>, Vec<OrderType>) = user_order_by
            .iter()
            .map(|o| {
                (
                    o.column_index,
                    if matches!(kind, PbAggKind::LastValue) {
                        o.order_type.reverse()
                    } else {
                        o.order_type
                    },
                )
            })
            .unzip();

        let mut key_col_indices = order_col_indices;
        key_col_indices.extend(stream_key.iter().copied());

        let mut key_data_types = Vec::with_capacity(key_col_indices.len());
        for &idx in &key_col_indices {
            key_data_types.push(input_schema[idx].data_type());
        }

        let mut key_order_types = order_types;
        key_order_types.extend(itertools::repeat_n(OrderType::ascending(), stream_key.len()));

        let key_serializer = OrderedRowSerde::new(key_data_types, key_order_types);

        Self {
            return_type,
            value_col_idx,
            key_col_indices,
            key_serializer,
        }
    }

    fn serialize_key(&self, row: impl Row) -> Vec<u8> {
        let mut key = Vec::new();
        self.key_serializer
            .serialize_datums(self.key_col_indices.iter().map(|&i| row.datum_at(i)), &mut key);
        key
    }
}

#[async_trait::async_trait]
impl AggregateFunction for AppendOnlyFirstLastValueAgg {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn create_state(&self) -> Result<AggregateState> {
        Ok(AggregateState::Any(
            Box::<AppendOnlyFirstLastValueState>::default(),
        ))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<AppendOnlyFirstLastValueState>();
        for (op, row) in input.rows() {
            if !matches!(op, Op::Insert | Op::UpdateInsert) {
                return Err(ExprError::InvalidState(
                    "append-only first/last value received retract input".into(),
                ));
            }

            let key = self.serialize_key(row);
            let should_replace = match &state.best_key {
                None => true,
                Some(best) => key < *best,
            };

            if should_replace {
                state.best_key = Some(key);
                state.best_value = row
                    .datum_at(self.value_col_idx)
                    .map(|v| v.into_scalar_impl());
            }
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: std::ops::Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<AppendOnlyFirstLastValueState>();
        for (op, row) in input.rows_in(range) {
            if !matches!(op, Op::Insert | Op::UpdateInsert) {
                return Err(ExprError::InvalidState(
                    "append-only first/last value received retract input".into(),
                ));
            }

            let key = self.serialize_key(row);
            let should_replace = match &state.best_key {
                None => true,
                Some(best) => key < *best,
            };

            if should_replace {
                state.best_key = Some(key);
                state.best_value = row
                    .datum_at(self.value_col_idx)
                    .map(|v| v.into_scalar_impl());
            }
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<AppendOnlyFirstLastValueState>();
        Ok(state.best_value.clone())
    }

    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<AppendOnlyFirstLastValueState>();
        let Some(best_key) = &state.best_key else {
            return Ok(None);
        };

        // Encoding format (v1):
        //   u8 version (=1)
        //   u32 key_len
        //   [key_bytes]
        //   [value_encoding::serialize_datum(best_value)]
        let mut buf = Vec::with_capacity(
            1 + 4
                + best_key.len()
                + value_encoding::estimate_serialize_datum_size(&state.best_value),
        );
        buf.put_u8(1);
        buf.put_u32(best_key.len() as u32);
        buf.put_slice(best_key);
        value_encoding::serialize_datum_into(&state.best_value, &mut buf);
        Ok(Some(ScalarImpl::Bytea(buf.into())))
    }

    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let mut st = AppendOnlyFirstLastValueState::default();
        let Some(scalar) = datum else {
            return Ok(AggregateState::Any(Box::new(st)));
        };

        let encoded: Box<[u8]> = scalar.into_bytea();
        let mut buf = bytes::Bytes::from(encoded);

        let version = buf.get_u8();
        if version != 1 {
            return Err(ExprError::InvalidState(
                format!(
                    "unknown append-only first/last value state version: {}",
                    version
                )
                .into(),
            ));
        }

        let key_len = buf.get_u32() as usize;
        if buf.remaining() < key_len {
            return Err(ExprError::InvalidState(
                "corrupted append-only first/last value state".into(),
            ));
        }
        let key = buf.copy_to_bytes(key_len);
        st.best_key = Some(key.to_vec());

        st.best_value = value_encoding::deserialize_datum(&mut buf, &self.return_type)
            .map_err(|e| ExprError::InvalidState(e.to_string().into()))?;
        Ok(AggregateState::Any(Box::new(st)))
    }
}

pub fn build_append_only_first_last_value_agg(
    agg_call: &AggCall,
    stream_key: &[usize],
    input_schema: &Schema,
) -> Result<BoxedAggregateFunction> {
    let kind = match agg_call.agg_type {
        risingwave_expr::aggregate::AggType::Builtin(kind) => kind,
        _ => {
            return Err(ExprError::InvalidState(
                "expected builtin first_value/last_value".into(),
            ))
        }
    };

    if !matches!(kind, PbAggKind::FirstValue | PbAggKind::LastValue) {
        return Err(ExprError::InvalidState(
            "expected first_value/last_value".into(),
        ));
    }
    if agg_call.args.val_indices().len() != 1 {
        return Err(ExprError::InvalidState(
            "first_value/last_value expects exactly one argument".into(),
        ));
    }

    Ok(Box::new(AppendOnlyFirstLastValueAgg::new(
        kind,
        agg_call.args.val_indices()[0],
        &agg_call.column_orders,
        stream_key,
        input_schema,
        agg_call.return_type.clone(),
    )))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::aggregate::AggCall;

    use super::*;

    fn schema_v_i_i() -> Schema {
        Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ])
    }

    #[tokio::test]
    async fn test_first_value_append_only_value_state_basic() {
        // columns: (val, ord, pk)
        let schema = schema_v_i_i();
        let agg_call = AggCall::from_pretty("(first_value:int4 $0:int4 orderby $1:asc)");
        let stream_key = vec![2];

        let agg = build_append_only_first_last_value_agg(&agg_call, &stream_key, &schema).unwrap();
        let mut state = agg.create_state().unwrap();

        let chunk = StreamChunk::from_pretty(
            " i i i
             + 10 5 2
             + 99 5 1
             +  1 3 9",
        );
        agg.update(&mut state, &chunk).await.unwrap();
        let res = agg.get_result(&state).await.unwrap();

        // smallest ord=3 => val=1
        assert_eq!(res, Some(ScalarImpl::Int32(1)));

        // round-trip encode/decode
        let encoded = agg.encode_state(&state).unwrap();
        let decoded_state = agg.decode_state(encoded).unwrap();
        let res2 = agg.get_result(&decoded_state).await.unwrap();
        assert_eq!(res2, Some(ScalarImpl::Int32(1)));
    }

    #[tokio::test]
    async fn test_last_value_append_only_value_state_tie_break_by_pk_asc() {
        // columns: (val, ord, pk)
        let schema = schema_v_i_i();
        let agg_call = AggCall::from_pretty("(last_value:int4 $0:int4 orderby $1:asc)");
        let stream_key = vec![2];

        let agg = build_append_only_first_last_value_agg(&agg_call, &stream_key, &schema).unwrap();
        let mut state = agg.create_state().unwrap();

        let chunk = StreamChunk::from_pretty(
            " i i i
             + 10 5 2
             + 99 5 1
             +  1 3 9",
        );
        agg.update(&mut state, &chunk).await.unwrap();
        let res = agg.get_result(&state).await.unwrap();

        // last_value with orderby asc means pick max ord=5.
        // tie-break uses pk ASC (same as existing minput plan): pick pk=1 => val=99
        assert_eq!(res, Some(ScalarImpl::Int32(99)));
    }
}
