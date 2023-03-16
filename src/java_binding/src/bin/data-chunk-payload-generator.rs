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

use std::io::Write;

use prost::Message;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, OrderedF32, OrderedF64, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

fn build_row(index: usize) -> OwnedRow {
    let mut row_value = Vec::with_capacity(8);
    row_value.push(Some(ScalarImpl::Int16(index as i16)));
    row_value.push(Some(ScalarImpl::Int32(index as i32)));
    row_value.push(Some(ScalarImpl::Int64(index as i64)));
    row_value.push(Some(ScalarImpl::Float32(OrderedF32::from(index as f32))));
    row_value.push(Some(ScalarImpl::Float64(OrderedF64::from(index as f64))));
    row_value.push(Some(ScalarImpl::Bool(index % 3 == 0)));
    row_value.push(Some(ScalarImpl::Utf8(
        format!("{}", index).repeat((index % 10) + 1).into(),
    )));
    row_value.push(if index % 5 == 0 {
        None
    } else {
        Some(ScalarImpl::Int64(index as i64))
    });

    OwnedRow::new(row_value)
}

fn main() {
    let row_count = 30000;
    let data_types = vec![
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Varchar,
        DataType::Int64,
    ];
    let mut ops = Vec::with_capacity(row_count);
    let mut builder = DataChunkBuilder::new(data_types, row_count * 1024);
    for i in 0..row_count {
        assert!(
            builder.append_one_row(build_row(i)).is_none(),
            "should not finish"
        );
        if i % 2 == 0 {
            ops.push(Op::Insert);
        } else {
            ops.push(Op::Delete);
        }
    }

    let data_chunk = builder.consume_all().expect("should not be empty");
    let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
    let prost_stream_chunk = stream_chunk.to_protobuf();

    let payload = Message::encode_to_vec(&prost_stream_chunk);

    std::io::stdout()
        .write_all(&payload)
        .expect("should success");
}
