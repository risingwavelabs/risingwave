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

use risingwave_common::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, ListValue, RowRef};
use risingwave_common::bail;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_common::util::ordered::OrderedRow;
use risingwave_common::util::sort_util::{OrderPair, OrderType};

use crate::vector_op::agg::aggregator::Aggregator;
use crate::Result;

#[derive(Clone)]
struct ArrayAggUnordered {
    return_type: DataType,
    agg_col_idx: usize,
    values: Vec<Datum>,
}

impl ArrayAggUnordered {
    fn new(return_type: DataType, agg_col_idx: usize) -> Self {
        debug_assert!(matches!(return_type, DataType::List { datatype: _ }));
        ArrayAggUnordered {
            return_type,
            agg_col_idx,
            values: vec![],
        }
    }

    fn push(&mut self, datum: Datum) {
        self.values.push(datum);
    }

    fn get_result_and_reset(&mut self) -> ListValue {
        ListValue::new(std::mem::take(&mut self.values))
    }
}

impl Aggregator for ArrayAggUnordered {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        let array = input.column_at(self.agg_col_idx).array_ref();
        self.push(array.datum_at(row_id));
        Ok(())
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.values.reserve(end_row_id - start_row_id);
        for row_id in start_row_id..end_row_id {
            self.update_single(input, row_id)?;
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

#[derive(Clone)]
struct ArrayAggOrdered {
    return_type: DataType,
    agg_col_idx: usize,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
    unordered_values: Vec<(OrderedRow, Datum)>,
}

impl ArrayAggOrdered {
    fn new(return_type: DataType, agg_col_idx: usize, order_pairs: Vec<OrderPair>) -> Self {
        debug_assert!(matches!(return_type, DataType::List { datatype: _ }));
        let (order_col_indices, order_types) = order_pairs
            .into_iter()
            .map(|p| (p.column_idx, p.order_type))
            .unzip();
        ArrayAggOrdered {
            return_type,
            agg_col_idx,
            order_col_indices,
            order_types,
            unordered_values: vec![],
        }
    }

    fn push_row(&mut self, row: RowRef<'_>) {
        let key = OrderedRow::new(
            row.row_by_indices(&self.order_col_indices),
            &self.order_types,
        );
        let datum = row.value_at(self.agg_col_idx).map(|x| x.into_scalar_impl());
        self.unordered_values.push((key, datum));
    }

    fn get_result_and_reset(&mut self) -> ListValue {
        let mut rows = std::mem::take(&mut self.unordered_values);
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        ListValue::new(rows.into_iter().map(|(_, datum)| datum).collect())
    }
}

impl Aggregator for ArrayAggOrdered {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        let (row, vis) = input.row_at(row_id)?;
        assert!(vis);
        self.push_row(row);
        Ok(())
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        self.unordered_values.reserve(end_row_id - start_row_id);
        for row_id in start_row_id..end_row_id {
            self.update_single(input, row_id)?;
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

pub fn create_array_agg_state(
    return_type: DataType,
    agg_col_idx: usize,
    order_pairs: Vec<OrderPair>,
) -> Result<Box<dyn Aggregator>> {
    if order_pairs.is_empty() {
        Ok(Box::new(ArrayAggUnordered::new(return_type, agg_col_idx)))
    } else {
        Ok(Box::new(ArrayAggOrdered::new(
            return_type,
            agg_col_idx,
            order_pairs,
        )))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::Array;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ScalarRef;

    use super::*;

    #[test]
    fn test_array_agg_basic() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "i
             123
             456
             789",
        );
        let return_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let mut agg = create_array_agg_state(return_type.clone(), 0, vec![])?;
        let mut builder = return_type.create_array_builder(0);
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
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

    #[test]
    fn test_array_agg_with_order() -> Result<()> {
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
                OrderPair::new(1, OrderType::Ascending),
                OrderPair::new(0, OrderType::Descending),
            ],
        )?;
        let mut builder = return_type.create_array_builder(0);
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
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
