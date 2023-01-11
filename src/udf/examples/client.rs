// Copyright 2023 Singularity Data
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

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use risingwave_udf::ArrowFlightUdfClient;

#[tokio::main]
async fn main() {
    let addr = "http://localhost:8815";
    let client = ArrowFlightUdfClient::connect(addr).await.unwrap();

    // build `RecordBatch` to send (equivalent to our `DataChunk`)
    let array1 = Int32Array::from_iter(vec![1, 6, 10]);
    let array2 = Int32Array::from_iter(vec![3, 4, 15]);
    let input_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]);
    let output_schema = Schema::new(vec![Field::new("c", DataType::Int32, true)]);

    // check function
    let id = client
        .check("gcd", &input_schema, &output_schema)
        .await
        .unwrap();

    let input = RecordBatch::try_new(
        Arc::new(input_schema),
        vec![Arc::new(array1), Arc::new(array2)],
    )
    .unwrap();

    let output = client
        .call(&id, input)
        .await
        .expect("failed to call function");

    println!("{:?}", output);
}
