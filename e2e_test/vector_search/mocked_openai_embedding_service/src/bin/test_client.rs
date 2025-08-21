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
        model: "mock_embedding".to_string(),
        input: EmbeddingInput::String("first".to_string()),
        ..Default::default()
    };

    let response = client.embeddings().create(request).await;
    println!("Embedding response: {:?}", response);
}
