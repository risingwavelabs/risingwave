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

use float_eq::assert_float_eq;
use itertools::Itertools;
use risingwave_common::array::{Op, Row};
use risingwave_common::types::decimal::ToPrimitive;
use risingwave_common::types::ScalarImpl;
use risingwave_source::{
    JsonParser, SourceColumnDesc, SourceFormat, SourceParser, SourceStreamChunkBuilder,
};
use risingwave_source_parser_test::{create_relational_schema, JsonGen};

fn parse_and_verify<P: SourceParser>(
    parser: &mut P,
    schema: Vec<SourceColumnDesc>,
    actual_row: Row,
    payload: &[u8],
) {
    let mut builder = SourceStreamChunkBuilder::with_capacity(schema, 1);
    parser.parse(payload, builder.row_writer()).unwrap();
    let chunk = builder.finish();
    let rows = chunk
        .rows()
        .map(|(op, row)| {
            assert!(matches!(op, Op::Insert));
            row
        })
        .collect_vec();
    assert_eq!(rows.len(), 1);
    let parsed_row = rows[0].to_owned_row();
    assert_row_eq(actual_row, parsed_row);
}

fn assert_row_eq(actual_row: Row, parsed_row: Row) {
    actual_row
        .0
        .iter()
        .zip_eq(parsed_row.0)
        .for_each(|(original, parsed)| {
            match (original, &parsed) {
                // The parsed floats will lose precision.
                (Some(ScalarImpl::Float32(v1)), Some(ScalarImpl::Float32(v2))) => {
                    assert_float_eq!(v1.0, v2.0, ulps <= 10);
                }
                (Some(ScalarImpl::Float64(v1)), Some(ScalarImpl::Float64(v2))) => {
                    assert_float_eq!(v1.0, v2.0, ulps <= 10);
                }
                (Some(ScalarImpl::Decimal(v1)), Some(ScalarImpl::Decimal(v2))) => {
                    assert_float_eq!(v1.to_f64().unwrap(), v2.to_f64().unwrap(), ulps <= 10);
                }
                _ => assert_eq!(original, &parsed),
            };
        });
}

#[test]
fn test_parse_json() {
    let schema = create_relational_schema(SourceFormat::Json);
    let mut gen = JsonGen::new(rand::thread_rng());
    let mut parser = JsonParser {};

    (0..1000).for_each(|_| {
        let (row, record) = gen.gen_record(&schema);
        parse_and_verify(&mut parser, schema.clone(), row, record.as_bytes())
    })
}
