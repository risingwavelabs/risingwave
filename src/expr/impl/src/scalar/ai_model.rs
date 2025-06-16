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
use async_openai::types::{CreateEmbeddingRequestArgs, EmbeddingInput};
use risingwave_common::array::{F32Array, ListValue};
use risingwave_common::types::F32;
use risingwave_expr::{ExprError, Result, function};
use thiserror_ext::AsReport;

/// `OpenAI` embedding context that holds the client and model configuration
#[derive(Debug)]
pub struct OpenAiEmbeddingContext {
    pub client: Client<OpenAIConfig>,
    pub model: String,
}

impl OpenAiEmbeddingContext {
    /// Create a new `OpenAI` embedding context from `api_key` and model
    pub fn from_config(api_key: &str, model: &str) -> Result<Self> {
        let config = OpenAIConfig::new().with_api_key(api_key);
        let client = Client::with_config(config);
        Ok(Self {
            client,
            model: model.to_owned(),
        })
    }
}

/// `OpenAI` embedding function with prebuild optimization for constant `api_key` and `model`
#[function(
    "openai_embedding(varchar, varchar, varchar) -> float4[]",
    prebuild = "OpenAiEmbeddingContext::from_config($0, $1)?"
)]
async fn openai_embedding(
    text: &str,
    context: &OpenAiEmbeddingContext,
) -> Result<Option<ListValue>> {
    if text.is_empty() {
        return Ok(None);
    }

    // Call the OpenAI API for embedding
    let embedding_request = CreateEmbeddingRequestArgs::default()
        .model(&context.model)
        .input(EmbeddingInput::String(text.to_owned()))
        .build()
        .map_err(|e| ExprError::InvalidParam {
            name: "openai_embedding",
            reason: format!("failed to build embedding request: {}", e.as_report()).into(),
        })?;

    let response = context
        .client
        .embeddings()
        .create(embedding_request)
        .await
        .map_err(|e| {
            tracing::error!(error = %e.as_report(), "Failed to get embedding from OpenAI");
            ExprError::InvalidParam {
                name: "openai_embedding",
                reason: "failed to get embedding from OpenAI".into(),
            }
        })?;

    if response.data.is_empty() {
        return Err(ExprError::InvalidParam {
            name: "openai_embedding",
            reason: "no embedding data returned from OpenAI".into(),
        });
    }

    let embedding_values = &response.data[0].embedding;
    let float_array = F32Array::from_iter(embedding_values.iter().map(|&v| Some(F32::from(v))));

    let list_value = ListValue::new(float_array.into());
    Ok(Some(list_value))
}
