// Copyright 2024 RisingWave Labs
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

use arrow::array::{ArrayRef, Int16Array};
use arrow::datatypes::{Fields, SchemaRef};
use risingwave_common::array::arrow::{ToArrow, UdfArrowConvert};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};

pub fn convert_chunk_to_arrow_arrays(schema: &Schema, chunk: &StreamChunk) -> Vec<ArrayRef> {
    let convert = UdfArrowConvert { legacy: false };

    let to_field = |filed: &Field| convert.to_arrow_field("", &filed.data_type);
    let arrow_schema = arrow::datatypes::Schema::new(
        schema
            .fields()
            .iter()
            .map(to_field)
            .try_collect::<Fields>()
            .unwrap(),
    );
    let ops = chunk.ops().iter().map(|op| op.to_i16()).collect::<Vec<_>>();
    let ops_arrow_vec = std::sync::Arc::new(Int16Array::from_iter_values(ops));
    let mut arrow_vec: Vec<ArrayRef> = vec![ops_arrow_vec];
    let record_batch = convert
        .to_record_batch(std::sync::Arc::new(arrow_schema), chunk)
        .unwrap();
    arrow_vec.extend(record_batch.columns().to_vec());
    arrow_vec
}
