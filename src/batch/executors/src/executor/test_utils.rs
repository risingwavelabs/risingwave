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

#![cfg_attr(not(test), allow(dead_code))]

use assert_matches::assert_matches;
use futures_async_stream::for_await;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, DataChunkTestExt};
use risingwave_common::catalog::Schema;
use risingwave_common::field_generator::{FieldGeneratorImpl, VarcharProperty};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Datum, ToOwnedDatum};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_expr::expr::BoxedExpression;
use risingwave_pb::batch_plan::PbExchangeSource;

use crate::error::Result;
use crate::exchange_source::ExchangeSourceImpl;
use crate::executor::{BoxedExecutor, CreateSource, LookupExecutorBuilder};
use crate::task::BatchTaskContext;

const SEED: u64 = 0xFF67FEABBAEF76FF;

pub use risingwave_batch::executor::test_utils::*;

/// Generate `batch_num` data chunks with type `data_types`, each data chunk has cardinality of
/// `batch_size`.
pub fn gen_data(batch_size: usize, batch_num: usize, data_types: &[DataType]) -> Vec<DataChunk> {
    DataChunk::gen_data_chunks(
        batch_num,
        batch_size,
        data_types,
        &VarcharProperty::RandomFixedLength(None),
        1.0,
    )
}

/// Generate `batch_num` sorted data chunks with type `Int64`, each data chunk has cardinality of
/// `batch_size`.
pub fn gen_sorted_data(
    batch_size: usize,
    batch_num: usize,
    start: String,
    step: u64,
    offset: u64,
) -> Vec<DataChunk> {
    let mut data_gen = FieldGeneratorImpl::with_number_sequence(
        DataType::Int64,
        Some(start),
        Some(i64::MAX.to_string()),
        0,
        step,
        offset,
    )
    .unwrap();
    let mut ret = Vec::<DataChunk>::with_capacity(batch_num);

    for _ in 0..batch_num {
        let mut array_builder = DataType::Int64.create_array_builder(batch_size);

        for _ in 0..batch_size {
            array_builder.append(data_gen.generate_datum(0));
        }

        let array = array_builder.finish();
        ret.push(DataChunk::new(vec![array.into()], batch_size));
    }

    ret
}

/// Generate `batch_num` data chunks with type `Int64`, each data chunk has cardinality of
/// `batch_size`. Then project each data chunk with `expr`.
///
/// NOTE: For convenience, here we only use data type `Int64`.
pub fn gen_projected_data(
    batch_size: usize,
    batch_num: usize,
    expr: BoxedExpression,
) -> Vec<DataChunk> {
    let mut data_gen =
        FieldGeneratorImpl::with_number_random(DataType::Int64, None, None, SEED).unwrap();
    let mut ret = Vec::<DataChunk>::with_capacity(batch_num);

    for i in 0..batch_num {
        let mut array_builder = DataType::Int64.create_array_builder(batch_size);

        for j in 0..batch_size {
            array_builder.append(data_gen.generate_datum(((i + 1) * (j + 1)) as u64));
        }

        let chunk = DataChunk::new(vec![array_builder.finish().into()], batch_size);

        let array = futures::executor::block_on(expr.eval(&chunk)).unwrap();
        let chunk = DataChunk::new(vec![array], batch_size);
        ret.push(chunk);
    }

    ret
}

/// if the input from two child executor is same(considering order),
/// it will also check the columns structure of chunks from child executor
/// use for executor unit test.
///
/// if want diff ignoring order, add a `order_by` executor in manual currently, when the `schema`
/// method of `executor` is ready, an order-ignored version will be added.
pub async fn diff_executor_output(actual: BoxedExecutor, expect: BoxedExecutor) {
    let mut expect_cardinality = 0;
    let mut actual_cardinality = 0;
    let mut expects = vec![];
    let mut actuals = vec![];

    #[for_await]
    for chunk in expect.execute() {
        assert_matches!(chunk, Ok(_));
        let chunk = chunk.unwrap().compact();
        expect_cardinality += chunk.cardinality();
        expects.push(chunk);
    }

    #[for_await]
    for chunk in actual.execute() {
        assert_matches!(chunk, Ok(_));
        let chunk = chunk.unwrap().compact();
        actual_cardinality += chunk.cardinality();
        actuals.push(chunk);
    }

    assert_eq!(actual_cardinality, expect_cardinality);
    if actual_cardinality == 0 {
        return;
    }
    let expect = DataChunk::rechunk(expects.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let actual = DataChunk::rechunk(actuals.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let col_num = expect.columns().len();
    assert_eq!(col_num, actual.columns().len());
    expect
        .columns()
        .iter()
        .zip_eq_fast(actual.columns().iter())
        .for_each(|(c1, c2)| assert_eq!(c1, c2));

    is_data_chunk_eq(&expect, &actual)
}

fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) {
    assert!(left.is_compacted());
    assert!(right.is_compacted());

    assert_eq!(
        left.cardinality(),
        right.cardinality(),
        "two chunks cardinality is different"
    );

    left.rows()
        .zip_eq_debug(right.rows())
        .for_each(|(row1, row2)| assert_eq!(row1, row2));
}

pub struct FakeInnerSideExecutorBuilder {
    schema: Schema,
    datums: Vec<Vec<Datum>>,
}

impl FakeInnerSideExecutorBuilder {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            datums: vec![],
        }
    }
}

impl LookupExecutorBuilder for FakeInnerSideExecutorBuilder {
    async fn build_executor(&mut self) -> Result<BoxedExecutor> {
        let mut mock_executor = MockExecutor::new(self.schema.clone());

        let base_data_chunk = DataChunk::from_pretty(
            "i f
             1 9.2
             2 4.4
             2 5.5
             4 6.8
             5 3.7
             5 2.3
             . .",
        );

        for idx in 0..base_data_chunk.capacity() {
            let probe_row = base_data_chunk.row_at_unchecked_vis(idx);
            for datum in &self.datums {
                if datum[0] == probe_row.datum_at(0).to_owned_datum() {
                    let chunk =
                        DataChunk::from_rows(&[probe_row], &[DataType::Int32, DataType::Float32]);
                    mock_executor.add(chunk);
                    break;
                }
            }
        }

        Ok(Box::new(mock_executor))
    }

    async fn add_scan_range(&mut self, key_datums: Vec<Datum>) -> Result<()> {
        self.datums.push(key_datums.iter().cloned().collect_vec());
        Ok(())
    }

    fn reset(&mut self) {
        self.datums = vec![];
    }
}

#[derive(Debug, Clone)]
pub(super) struct FakeCreateSource {
    fake_exchange_source: FakeExchangeSource,
}

impl FakeCreateSource {
    pub fn new(fake_exchange_source: FakeExchangeSource) -> Self {
        Self {
            fake_exchange_source,
        }
    }
}

#[async_trait::async_trait]
impl CreateSource for FakeCreateSource {
    async fn create_source(
        &self,
        _: &dyn BatchTaskContext,
        _: &PbExchangeSource,
    ) -> Result<ExchangeSourceImpl> {
        Ok(ExchangeSourceImpl::Fake(self.fake_exchange_source.clone()))
    }
}
