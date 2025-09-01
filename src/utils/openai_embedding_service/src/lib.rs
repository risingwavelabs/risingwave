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

use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::ToSocketAddrs;

#[derive(Deserialize, Debug)]
struct EmbeddingRequest {
    input: serde_json::Value,
    model: String,
}

#[derive(Serialize)]
struct Usage {
    prompt_tokens: usize,
    total_tokens: usize,
}

#[derive(Serialize)]
struct EmbeddingResponse {
    object: String,
    model: String,
    data: Vec<EmbeddingData>,
    usage: Usage,
}

#[derive(Serialize)]
struct EmbeddingData {
    object: String,
    embedding: Vec<f32>,
    index: usize,
}

async fn embedding_handler<F: Future<Output = (Vec<Vec<f32>>, usize, usize)> + Send>(
    Json(payload): Json<EmbeddingRequest>,
    gen_embedding: impl Fn(Vec<String>) -> F + Send,
) -> Json<EmbeddingResponse> {
    let inputs: Vec<_> = match payload.input {
        serde_json::Value::Array(array) => array
            .into_iter()
            .map(|s| s.as_str().unwrap().to_owned())
            .collect(),
        serde_json::Value::String(s) => {
            vec![s]
        }
        _ => unreachable!(),
    };
    let (embeddings, prompt_tokens, total_tokens) = gen_embedding(inputs).await;
    let data = embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| {
            // Return a fixed embedding vector
            EmbeddingData {
                object: "embedding".to_owned(),
                embedding,
                index,
            }
        })
        .collect();

    Json(EmbeddingResponse {
        object: "list".to_owned(),
        model: payload.model,
        data,
        usage: Usage {
            prompt_tokens,
            total_tokens,
        },
    })
}

pub async fn serve_embedding_service<F: Future<Output = (Vec<Vec<f32>>, usize, usize)> + Send>(
    gen_embedding: impl Fn(Vec<String>) -> F + Send + Clone + 'static,
    bind_addr: impl ToSocketAddrs,
) {
    let app = Router::new().route(
        "/v1/embeddings",
        post(|request| embedding_handler(request, gen_embedding)),
    );
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();

    tracing::info!("Listening on {}", listener.local_addr().unwrap());

    // Serve the application.
    axum::serve(listener, app).await.unwrap();
}
