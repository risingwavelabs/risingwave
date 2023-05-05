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

use risingwave_common::types::{Datum, ScalarRef};
use risingwave_expr_macro::aggregate;

#[aggregate("array_agg(*) -> list", state = "Vec<Datum>")]
fn array_agg<'a, T: ScalarRef<'a>>(state: Option<Vec<Datum>>, value: Option<T>) -> Vec<Datum> {
    let mut state = state.unwrap_or_default();
    state.push(value.map(|v| v.to_owned_scalar().into()));
    state
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{Array, DataChunk, ListValue};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::{DataType, ScalarRef};
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use crate::agg::{AggArgs, AggCall, AggKind};
    use crate::Result;

    #[tokio::test]
    async fn test_array_agg_basic() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "i
             123
             456
             789",
        );
        let return_type = DataType::List(Box::new(DataType::Int32));
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Int32, 0),
            return_type: return_type.clone(),
            column_orders: vec![],
            filter: None,
            distinct: false,
        })?;
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
        let return_type = DataType::List(Box::new(DataType::Int32));
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Int32, 0),
            return_type: return_type.clone(),
            column_orders: vec![],
            filter: None,
            distinct: false,
        })?;
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
        let return_type = DataType::List(Box::new(DataType::Int32));
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::ArrayAgg,
            args: AggArgs::Unary(DataType::Int32, 0),
            return_type: return_type.clone(),
            column_orders: vec![
                ColumnOrder::new(1, OrderType::ascending()),
                ColumnOrder::new(0, OrderType::descending()),
            ],
            filter: None,
            distinct: false,
        })?;
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
