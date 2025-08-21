use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

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

async fn embedding_handler(Json(payload): Json<EmbeddingRequest>) -> Json<EmbeddingResponse> {
    const VECTOR_DIM: usize = 3;
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
    let len = inputs.iter().map(|s| s.len()).sum();
    let data = inputs
        .into_iter()
        .enumerate()
        .map(|(index, input)| {
            let vector: [_; VECTOR_DIM] = match input.as_str() {
                "first" => [1.0, 2.0, 3.0],
                "second" => [4.0, 5.0, 6.0],
                _ => [0.0, 0.0, 0.0],
            };
            // Return a fixed embedding vector
            EmbeddingData {
                object: "embedding".to_owned(),
                embedding: vector.into(),
                index,
            }
        })
        .collect();

    Json(EmbeddingResponse {
        object: "list".to_owned(),
        model: payload.model,
        data,
        usage: Usage {
            prompt_tokens: len,
            total_tokens: len,
        },
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let app = Router::new().route("/v1/embeddings", post(embedding_handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8088")
        .await
        .unwrap();

    println!("Listening on {}", listener.local_addr().unwrap());

    // Serve the application.
    axum::serve(listener, app).await.unwrap();
}
