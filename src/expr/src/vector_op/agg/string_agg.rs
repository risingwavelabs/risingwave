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

use itertools::Itertools;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, RowRef,
};
use risingwave_common::bail;
use risingwave_common::types::{DataType, Scalar};
use risingwave_common::util::ordered::OrderedRow;
use risingwave_common::util::sort_util::{OrderPair, OrderType};

use crate::vector_op::agg::aggregator::Aggregator;
use crate::Result;

#[derive(Clone)]
struct StringAggUnordered {
    agg_col_idx: usize,
    delim_col_idx: usize,
    result: Option<String>,
}

impl StringAggUnordered {
    fn new(agg_col_idx: usize, delim_col_idx: usize) -> Self {
        Self {
            agg_col_idx,
            delim_col_idx,
            result: None,
        }
    }

    fn push(&mut self, value: &str, delim: &str) {
        if let Some(result) = &mut self.result {
            result.push_str(delim);
            result.push_str(value);
        } else {
            self.result = Some(value.to_string());
        }
    }

    fn get_result_and_reset(&mut self) -> Option<String> {
        std::mem::take(&mut self.result)
    }
}

impl Aggregator for StringAggUnordered {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let (ArrayImpl::Utf8(agg_col), ArrayImpl::Utf8(delim_col)) = (
            input.column_at(self.agg_col_idx).array_ref(),
            input.column_at(self.delim_col_idx).array_ref(),
        ) {
            if let Some(value) = agg_col.value_at(row_id) {
                // only need to save rows with non-empty string value to aggregate
                let delim = delim_col.value_at(row_id).unwrap_or("");
                self.push(value, delim);
            }
            Ok(())
        } else {
            bail!("Input fail to match {}.", stringify!(Utf8))
        }
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        if let (ArrayImpl::Utf8(agg_col), ArrayImpl::Utf8(delim_col)) = (
            input.column_at(self.agg_col_idx).array_ref(),
            input.column_at(self.delim_col_idx).array_ref(),
        ) {
            for (value, delim) in agg_col
                .iter()
                .zip_eq(delim_col.iter())
                .skip(start_row_id)
                .take(end_row_id - start_row_id)
                .filter(|(v, _)| v.is_some())
            {
                self.push(value.unwrap(), delim.unwrap_or(""));
            }
            Ok(())
        } else {
            bail!("Input fail to match {}.", stringify!(Utf8))
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::Utf8(builder) = builder {
            let res = self.get_result_and_reset();
            builder
                .append(res.as_ref().map(|x| x.as_scalar_ref()))
                .map_err(Into::into)
        } else {
            bail!("Builder fail to match {}.", stringify!(Utf8))
        }
    }
}

#[derive(Clone)]
struct StringAggData {
    value: String,
    delim: String,
}

#[derive(Clone)]
struct StringAggOrdered {
    agg_col_idx: usize,
    delim_col_idx: usize,
    order_col_indices: Vec<usize>,
    order_types: Vec<OrderType>,
    unordered_values: Vec<(OrderedRow, StringAggData)>,
}

impl StringAggOrdered {
    fn new(agg_col_idx: usize, delim_col_idx: usize, order_pairs: Vec<OrderPair>) -> Self {
        let (order_col_indices, order_types) = order_pairs
            .into_iter()
            .map(|p| (p.column_idx, p.order_type))
            .unzip();
        Self {
            agg_col_idx,
            delim_col_idx,
            order_col_indices,
            order_types,
            unordered_values: vec![],
        }
    }

    fn push_row(&mut self, value: &str, delim: &str, row: RowRef) {
        let key = OrderedRow::new(
            row.row_by_indices(&self.order_col_indices),
            &self.order_types,
        );
        self.unordered_values.push((
            key,
            StringAggData {
                value: value.to_string(),
                delim: delim.to_string(),
            },
        ));
    }

    fn get_result_and_reset(&mut self) -> Option<String> {
        let mut rows = std::mem::take(&mut self.unordered_values);
        if rows.is_empty() {
            return None;
        }
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut rows_iter = rows.into_iter();
        let mut result = rows_iter.next().unwrap().1.value;
        for (_, data) in rows_iter {
            result.push_str(&data.delim);
            result.push_str(&data.value);
        }
        Some(result)
    }
}

impl Aggregator for StringAggOrdered {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let (ArrayImpl::Utf8(agg_col), ArrayImpl::Utf8(delim_col)) = (
            input.column_at(self.agg_col_idx).array_ref(),
            input.column_at(self.delim_col_idx).array_ref(),
        ) {
            if let Some(value) = agg_col.value_at(row_id) {
                // only need to save rows with non-empty string value to aggregate
                let delim = delim_col.value_at(row_id).unwrap_or("");
                let (row_ref, vis) = input.row_at(row_id)?;
                assert!(vis);
                self.push_row(value, delim, row_ref);
            }
            Ok(())
        } else {
            bail!("Input fail to match {}.", stringify!(Utf8))
        }
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        if let (ArrayImpl::Utf8(agg_col), ArrayImpl::Utf8(delim_col)) = (
            input.column_at(self.agg_col_idx).array_ref(),
            input.column_at(self.delim_col_idx).array_ref(),
        ) {
            for (row_id, (value, delim)) in agg_col
                .iter()
                .zip_eq(delim_col.iter())
                .enumerate()
                .skip(start_row_id)
                .take(end_row_id - start_row_id)
                .filter(|(_, (v, _))| v.is_some())
            {
                let (row_ref, vis) = input.row_at(row_id)?;
                assert!(vis);
                self.push_row(value.unwrap(), delim.unwrap_or(""), row_ref);
            }
            Ok(())
        } else {
            bail!("Input fail to match {}.", stringify!(Utf8))
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::Utf8(builder) = builder {
            let res = self.get_result_and_reset();
            builder
                .append(res.as_ref().map(|x| x.as_scalar_ref()))
                .map_err(Into::into)
        } else {
            bail!("Builder fail to match {}.", stringify!(Utf8))
        }
    }
}

pub fn create_string_agg_state(
    agg_col_idx: usize,
    delim_col_idx: usize,
    order_pairs: Vec<OrderPair>,
) -> Result<Box<dyn Aggregator>> {
    if order_pairs.is_empty() {
        Ok(Box::new(StringAggUnordered::new(
            agg_col_idx,
            delim_col_idx,
        )))
    } else {
        Ok(Box::new(StringAggOrdered::new(
            agg_col_idx,
            delim_col_idx,
            order_pairs,
        )))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt, Utf8ArrayBuilder};
    use risingwave_common::util::sort_util::{OrderPair, OrderType};

    use super::*;

    #[test]
    fn test_string_agg_basic() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   T
             aaa ,
             bbb ,
             ccc ,
             ddd ,",
        );
        let mut agg = create_string_agg_state(0, 1, vec![])?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
        agg.output(&mut builder)?;
        let output = builder.finish()?;
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "aaa,bbb,ccc,ddd";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }

    #[test]
    fn test_string_agg_complex() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   T
             aaa ,
             .   _
             ccc _
             ddd .",
        );
        let mut agg = create_string_agg_state(0, 1, vec![])?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
        agg.output(&mut builder)?;
        let output = builder.finish()?;
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "aaa_cccddd";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }

    #[test]
    fn test_string_agg_with_order() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T T   i i
             _ aaa 1 3
             _ bbb 0 4
             _ ccc 0 8
             _ ddd 1 3",
        );
        let mut agg = create_string_agg_state(
            1,
            0,
            vec![
                OrderPair::new(2, OrderType::Ascending),
                OrderPair::new(3, OrderType::Descending),
                OrderPair::new(1, OrderType::Descending),
            ],
        )?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality())?;
        agg.output(&mut builder)?;
        let output = builder.finish()?;
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "ccc_bbb_ddd_aaa";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }
}
