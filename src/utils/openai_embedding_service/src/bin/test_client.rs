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

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::{CreateEmbeddingRequest, EmbeddingInput};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let config = OpenAIConfig::new()
        .with_api_key("api_key")
        .with_api_base("http://127.0.0.1:8088/v1");

    let client = Client::with_config(config);

    let request = CreateEmbeddingRequest {
        model: "mock_embedding".to_owned(),
        input: EmbeddingInput::String("first".to_owned()),
        ..Default::default()
    };

    let response = client.embeddings().create(request).await;
    println!("Embedding response: {:?}", response);
}
