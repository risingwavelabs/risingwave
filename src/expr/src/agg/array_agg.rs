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

use anyhow::anyhow;
use risingwave_common::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, ListValue, RowRef};
use risingwave_common::bail;
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{DataType, Datum, Scalar, ToOwnedDatum};
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr_macro::build_aggregate;

use super::Aggregator;
use crate::function::aggregate::AggCall;
use crate::{ExprError, Result};

#[build_aggregate("array_agg(list) -> list")]
fn build_array_agg(agg: AggCall) -> Result<Box<dyn Aggregator>> {
    Ok(if agg.column_orders.is_empty() {
        Box::new(ArrayAggUnordered::new(agg.return_type))
    } else {
        Box::new(ArrayAggOrdered::new(agg.return_type, agg.column_orders))
    })
}

#[derive(Clone)]
struct ArrayAggUnordered {
    return_type: DataType,
    values: Vec<Datum>,
}

impl ArrayAggUnordered {
    fn new(return_type: DataType) -> Self {
        debug_assert!(matches!(return_type, DataType::List { datatype: _ }));
        ArrayAggUnordered {
            return_type,
            values: vec![],
        }
    }

    fn push(&mut self, datum: Datum) {
        self.values.push(datum);
    }

    fn get_result_and_reset(&mut self) -> Option<ListValue> {
        if self.values.is_empty() {
            None
        } else {
            Some(ListValue::new(std::mem::take(&mut self.values)))
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for ArrayAggUnordered {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.values.reserve(end_row_id - start_row_id);
        for row_id in start_row_id..end_row_id {
            let array = input.column_at(0).array_ref();
            self.push(array.datum_at(row_id));
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::List(builder) = builder {
            builder.append(
                self.get_result_and_reset()
                    .as_ref()
                    .map(|s| s.as_scalar_ref()),
            );
            Ok(())
        } else {
            bail!("Builder fail to match {}.", stringify!(Utf8))
        }
    }
}

type OrderKey = Vec<u8>;

#[derive(Clone)]
struct ArrayAggOrdered {
    return_type: DataType,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
    unordered_values: Vec<(OrderKey, Datum)>,
}

impl ArrayAggOrdered {
    fn new(return_type: DataType, column_orders: Vec<ColumnOrder>) -> Self {
        assert!(matches!(return_type, DataType::List { datatype: _ }));
        let (order_col_indices, order_types) = column_orders
            .into_iter()
            .map(|c| (c.column_index, c.order_type))
            .unzip();
        ArrayAggOrdered {
            return_type,
            order_col_indices,
            order_types,
            unordered_values: vec![],
        }
    }

    fn push_row(&mut self, row: RowRef<'_>) -> Result<()> {
        let key =
            memcmp_encoding::encode_row(row.project(&self.order_col_indices), &self.order_types)
                .map_err(|e| ExprError::Internal(anyhow!("failed to encode row, error: {}", e)))?;
        let datum = row.datum_at(0).to_owned_datum();
        self.unordered_values.push((key, datum));
        Ok(())
    }

    fn get_result_and_reset(&mut self) -> ListValue {
        let mut rows = std::mem::take(&mut self.unordered_values);
        rows.sort_unstable_by(|(key_a, _), (key_b, _)| key_a.cmp(key_b));
        ListValue::new(rows.into_iter().map(|(_, datum)| datum).collect())
    }
}

#[async_trait::async_trait]
impl Aggregator for ArrayAggOrdered {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.unordered_values.reserve(end_row_id - start_row_id);
        for row_id in start_row_id..end_row_id {
            let (row, vis) = input.row_at(row_id);
            assert!(vis);
            self.push_row(row)?;
        }
        Ok(())
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::List(builder) = builder {
            builder.append(Some(self.get_result_and_reset().as_scalar_ref()));
            Ok(())
        } else {
            bail!("Builder fail to match {}.", stringify!(Utf8))
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::Array;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ScalarRef;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;

    #[tokio::test]
    async fn test_array_agg_basic() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "i
             123
             456
             789",
        );
        let return_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let mut agg = create_array_agg_state(return_type.clone(), 0, vec![]);
        let mut builder = return_type.create_array_builder(0);
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.into_list();
        let actual = actual
            .iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec();
        assert_eq!(
            actual,
            vec![Some(ListValue::new(vec![
                Some(123.into()),
                Some(456.into()),
                Some(789.into())
            ]))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_empty() -> Result<()> {
        let return_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let mut agg = create_array_agg_state(return_type.clone(), 0, vec![]);
        let mut builder = return_type.create_array_builder(0);
        agg.output(&mut builder)?;

        let output = builder.finish();
        let actual = output.into_list();
        let actual = actual
            .iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec();
        assert_eq!(actual, vec![None]);

        let chunk = DataChunk::from_pretty(
            "i
             .",
        );
        let mut builder = return_type.create_array_builder(0);
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.into_list();
        let actual = actual
            .iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec();
        assert_eq!(actual, vec![Some(ListValue::new(vec![None]))]);

        Ok(())
    }

    #[tokio::test]
    async fn test_array_agg_with_order() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "i    i
             123  3
             456  2
             789  2
             321  9",
        );
        let return_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let mut agg = create_array_agg_state(
            return_type.clone(),
            0,
            vec![
                ColumnOrder::new(1, OrderType::ascending()),
                ColumnOrder::new(0, OrderType::descending()),
            ],
        );
        let mut builder = return_type.create_array_builder(0);
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.into_list();
        let actual = actual
            .iter()
            .map(|v| v.map(|s| s.to_owned_scalar()))
            .collect_vec();
        assert_eq!(
            actual,
            vec![Some(ListValue::new(vec![
                Some(789.into()),
                Some(456.into()),
                Some(123.into()),
                Some(321.into())
            ]))]
        );
        Ok(())
    }
}
