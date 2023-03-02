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

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use risingwave_udf::ArrowFlightUdfClient;

#[tokio::main]
async fn main() {
    let addr = "http://localhost:8815";
    let client = ArrowFlightUdfClient::connect(addr).await.unwrap();

    // build `RecordBatch` to send (equivalent to our `DataChunk`)
    let array1 = Arc::new(Int32Array::from_iter(vec![1, 6, 10]));
    let array2 = Arc::new(Int32Array::from_iter(vec![3, 4, 15]));
    let array3 = Arc::new(Int32Array::from_iter(vec![6, 8, 3]));
    let input2_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]);
    let input3_schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
    ]);
    let output_schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);

    // check function
    client
        .check("gcd", &input2_schema, &output_schema)
        .await
        .unwrap();
    client
        .check("gcd3", &input3_schema, &output_schema)
        .await
        .unwrap();

    let input2 = RecordBatch::try_new(
        Arc::new(input2_schema),
        vec![array1.clone(), array2.clone()],
    )
    .unwrap();

    let output = client
        .call("gcd", input2)
        .await
        .expect("failed to call function");

    println!("{:?}", output);

    let input3 = RecordBatch::try_new(
        Arc::new(input3_schema),
        vec![array1.clone(), array2.clone(), array3.clone()],
    )
    .unwrap();

    let output = client
        .call("gcd3", input3)
        .await
        .expect("failed to call function");

    println!("{:?}", output);
}
