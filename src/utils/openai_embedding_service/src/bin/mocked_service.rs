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

use openai_embedding_service::serve_embedding_service;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    serve_embedding_service(
        async |inputs: Vec<String>| {
            let len = inputs.iter().map(|s| s.len()).sum();
            let embeddings = inputs
                .into_iter()
                .map(|input| {
                    const VECTOR_DIM: usize = 3;
                    let vector: [_; VECTOR_DIM] = match input.as_str() {
                        "first" => [1.0, 2.0, 3.0],
                        "second" => [4.0, 5.0, 6.0],
                        "query" => [3.0, 2.0, 1.0],
                        _ => [0.0, 0.0, 0.0],
                    };
                    vector.into()
                })
                .collect();
            (embeddings, len, len)
        },
        "127.0.0.1:8088",
    )
    .await
}
