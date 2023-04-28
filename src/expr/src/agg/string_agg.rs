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

use risingwave_common::bail;
use risingwave_expr_macro::aggregate;

#[aggregate("string_agg(varchar, varchar) -> varchar", state = "String")]
fn string_agg(
    state: Option<String>,
    value: Option<&str>,
    delimiter: Option<&str>,
) -> Option<String> {
    let Some(value) = value else { return state };
    let Some(mut state) = state else { return Some(value.into()) };
    state += delimiter.unwrap_or("");
    state += value;
    Some(state)
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use crate::agg::{AggArgs, AggCall, AggKind};
    use crate::Result;

    #[tokio::test]
    async fn test_string_agg_basic() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   T
             aaa ,
             bbb ,
             ccc ,
             ddd ,",
        );
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            column_orders: vec![],
            filter: None,
            distinct: false,
        })?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "aaa,bbb,ccc,ddd";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_complex() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   T
             aaa ,
             .   _
             ccc _
             ddd .",
        );
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            column_orders: vec![],
            filter: None,
            distinct: false,
        })?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "aaa_cccddd";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }

    #[tokio::test]
    async fn test_string_agg_with_order() -> Result<()> {
        let chunk = DataChunk::from_pretty(
            "T   T i i
             aaa _ 1 3
             bbb _ 0 4
             ccc _ 0 8
             ddd _ 1 3",
        );
        let mut agg = crate::agg::build(AggCall {
            kind: AggKind::StringAgg,
            args: AggArgs::Binary([DataType::Varchar, DataType::Varchar], [0, 1]),
            return_type: DataType::Varchar,
            column_orders: vec![
                ColumnOrder::new(2, OrderType::ascending()),
                ColumnOrder::new(3, OrderType::descending()),
                ColumnOrder::new(0, OrderType::descending()),
            ],
            filter: None,
            distinct: false,
        })?;
        let mut builder = ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0));
        agg.update_multi(&chunk, 0, chunk.cardinality()).await?;
        agg.output(&mut builder)?;
        let output = builder.finish();
        let actual = output.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        let expected = "ccc_bbb_ddd_aaa";
        assert_eq!(actual, &[Some(expected)]);
        Ok(())
    }
}
