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
use std::env;
use std::fs::File;
use std::io::{Read, Write};

use prost::Message;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Deserialize, Serialize)]
struct Line {
    id: u32,
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Operation {
    op_type: u32,
    line: Line,
}

fn convert_to_op(value: u32) -> Option<Op> {
    match value {
        1 => Some(Op::Insert),
        2 => Some(Op::Delete),
        3 => Some(Op::UpdateDelete),
        4 => Some(Op::UpdateInsert),
        _ => None,
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    // Read the JSON file
    let mut file = File::open(&args[1]).expect("Failed to open file");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Failed to read file");

    // Parse the JSON data
    let data: Vec<Vec<Operation>> = serde_json::from_str(&contents).expect("Failed to parse JSON");

    let data_types: Vec<_> = vec![DataType::Int32, DataType::Varchar];

    // Access the data
    let mut row_count = 0;
    for operations in &data {
        row_count += operations.len();
    }
    let mut ops = Vec::with_capacity(row_count);
    let mut builder = DataChunkBuilder::new(data_types, row_count * 1024);

    for operations in data {
        for operation in operations {
            let mut row_value = Vec::with_capacity(10);
            row_value.push(Some(ScalarImpl::Int32(operation.line.id as i32)));
            row_value.push(Some(ScalarImpl::Utf8(operation.line.name.into_boxed_str())));
            let _ = builder.append_one_row(OwnedRow::new(row_value));
            // let op: Op = unsafe { ::std::mem::transmute(operation.op_type as u8) };
            if let Some(op) = convert_to_op(operation.op_type) {
                ops.push(op);
            } else {
                println!("Invalid value");
            }
        }
    }

    let data_chunk = builder.consume_all().expect("should not be empty");
    let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
    let prost_stream_chunk: risingwave_pb::data::StreamChunk = stream_chunk.to_protobuf();

    let payload = Message::encode_to_vec(&prost_stream_chunk);

    std::io::stdout()
        .write_all(&payload)
        .expect("should success");
}
