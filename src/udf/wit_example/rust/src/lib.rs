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

use std::sync::Arc;

use arrow_array::Array;

use crate::risingwave::udf::types::{DataType, Field};

wit_bindgen::generate!({
    // optional, since there's only one world. We make it explicit here.
    world: "udf",
    // path is relative to Cargo.toml
    path:"../../wit"
});

// Define a custom type and implement the generated `Udf` trait for it which
// represents implementing all the necesssary exported interfaces for this
// component.
/// User defined function tou count number of specified characters.
/// Ref <https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/udf/CountChar.java>
struct CountChar;

export_udf!(CountChar);

fn count_char(s: &str, char: &str) -> i64 {
    let mut count = 0;
    let char = char.bytes().next().unwrap();

    for c in s.bytes() {
        if c == char {
            count += 1;
        }
    }
    count
}

impl Udf for CountChar {
    fn eval(batch: RecordBatch) -> Result<RecordBatch, EvalErrno> {
        // Read data from IPC buffer
        let batch = arrow_ipc::reader::StreamReader::try_new(batch.as_slice(), None).unwrap();

        // Do UDF computation (for each batch, for each row, do scalar -> scalar)
        let mut ret = arrow_array::builder::Int64Builder::new();
        for batch in batch {
            let batch = batch.unwrap();
            for i in 0..batch.num_rows() {
                let s = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect(
                        format!(
                            "expected StringArray, got {:?}",
                            batch.column(0).data_type()
                        )
                        .as_str(),
                    )
                    .value(i);
                let c = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect(
                        format!(
                            "expected StringArray, got {:?}",
                            batch.column(1).data_type()
                        )
                        .as_str(),
                    )
                    .value(i);
                ret.append_value(count_char(s, c));
            }
        }

        // Write data to IPC buffer
        let mut buf = vec![];
        {
            let array = ret.finish();
            let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result",
                arrow_schema::DataType::Int64,
                false,
            )]);
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            let batch =
                arrow_array::RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        Ok(buf)
    }

    fn input_schema() -> Schema {
        vec![Field {
            name: "input".to_string(),
            data_type: DataType::DtString,
        }]
    }

    fn output_schema() -> Schema {
        vec![Field {
            name: "result".to_string(),
            data_type: DataType::DtI64,
        }]
    }
}
