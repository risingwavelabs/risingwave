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

use std::sync::Arc;

use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::{CreateEmbeddingRequestArgs, Embedding, EmbeddingInput};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, F32Array, ListArrayBuilder, ListValue,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, F32, ScalarImpl};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{ExprError, Result, build_function};
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

#[derive(Debug)]
struct OpenAiEmbedding {
    text_expr: BoxedExpression,
    context: OpenAiEmbeddingContext,
}

impl OpenAiEmbedding {
    async fn get_embeddings(&self, input: EmbeddingInput) -> Result<Vec<Embedding>> {
        let request = CreateEmbeddingRequestArgs::default()
            .model(&self.context.model)
            .input(input)
            .build()
            .map_err(|e| {
                tracing::error!(error = %e.as_report(), "Failed to build OpenAI embedding request");
                ExprError::Custom("failed to build OpenAI embedding request".into())
            })?;

        let response = self
            .context
            .client
            .embeddings()
            .create(request)
            .await
            .map_err(|e| {
                tracing::error!(error = %e.as_report(), "Failed to get embedding from OpenAI");
                ExprError::Custom("failed to get embedding from OpenAI".into())
            })?;

        if response.data.is_empty() {
            return Err(ExprError::Custom(
                "no embedding data returned from OpenAI".into(),
            ));
        }

        Ok(response.data)
    }
}

#[async_trait::async_trait]
impl Expression for OpenAiEmbedding {
    fn return_type(&self) -> DataType {
        DataType::List(Box::new(DataType::Float32))
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let text_array = self.text_expr.eval(input).await?;
        let text_array = text_array.as_utf8();

        // Collect non-null and non-empty texts
        let mut texts_to_embed = Vec::new();

        for i in 0..input.capacity() {
            if let Some(text) = text_array.value_at(i) {
                if !text.is_empty() {
                    texts_to_embed.push(text.to_owned());
                }
            }
        }
        let n_texts_to_embed = texts_to_embed.len();

        // Get embeddings in batch
        let embeddings = if texts_to_embed.is_empty() {
            Vec::new()
        } else {
            self.get_embeddings(EmbeddingInput::StringArray(texts_to_embed))
                .await?
        };
        if embeddings.len() != n_texts_to_embed {
            return Err(ExprError::Custom(
                "number of embeddings returned from OpenAI does not match the number of texts"
                    .into(),
            ));
        }

        // Map results back to original positions
        let mut builder = ListArrayBuilder::with_type(
            input.capacity(),
            DataType::List(Box::new(DataType::Float32)),
        );
        let mut embedding_idx = 0;

        for i in 0..input.capacity() {
            if let Some(text) = text_array.value_at(i) {
                if !text.is_empty() {
                    // Non-empty text, use the embedding result
                    if embedding_idx < embeddings.len() {
                        let embedding = &embeddings[embedding_idx].embedding;
                        let float_array =
                            F32Array::from_iter(embedding.iter().map(|&v| Some(F32::from(v))));
                        let list_value = ListValue::new(float_array.into());
                        builder.append_owned(Some(list_value));
                        embedding_idx += 1;
                    } else {
                        builder.append(None);
                    }
                } else {
                    // Empty text returns NULL
                    builder.append(None);
                }
            } else {
                // Null text returns NULL
                builder.append(None);
            }
        }

        Ok(Arc::new(ArrayImpl::List(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let text_datum = self.text_expr.eval_row(input).await?;

        if let Some(ScalarImpl::Utf8(text)) = text_datum.as_ref() {
            if text.is_empty() {
                return Ok(None);
            }

            let embeddings = self
                .get_embeddings(EmbeddingInput::String(text.to_owned().into_string()))
                .await?;
            let embedding = &embeddings[0].embedding;
            let float_array = F32Array::from_iter(embedding.iter().map(|&v| Some(F32::from(v))));
            Ok(Some(ListValue::new(float_array.into()).into()))
        } else {
            Ok(None)
        }
    }
}

#[build_function("openai_embedding(varchar, varchar, varchar) -> float4[]")]
fn build_openai_embedding_expr(
    _: DataType,
    mut children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    if children.len() != 3 {
        return Err(ExprError::InvalidParam {
            name: "openai_embedding",
            reason: "expected 3 arguments".into(),
        });
    }

    // Check if the first two parameters are constants
    let api_key = if let Ok(Some(api_key_scalar)) = children[0].eval_const() {
        if let ScalarImpl::Utf8(api_key_str) = api_key_scalar {
            api_key_str.to_string()
        } else {
            return Err(ExprError::InvalidParam {
                name: "openai_embedding",
                reason: "`api_key` must be a string constant".into(),
            });
        }
    } else {
        return Err(ExprError::InvalidParam {
            name: "openai_embedding",
            reason: "`api_key` must be a constant".into(),
        });
    };

    let model = if let Ok(Some(model_scalar)) = children[1].eval_const() {
        if let ScalarImpl::Utf8(model_str) = model_scalar {
            model_str.to_string()
        } else {
            return Err(ExprError::InvalidParam {
                name: "openai_embedding",
                reason: "`model` must be a string constant".into(),
            });
        }
    } else {
        return Err(ExprError::InvalidParam {
            name: "openai_embedding",
            reason: "`model` must be a constant".into(),
        });
    };

    let context = OpenAiEmbeddingContext::from_config(&api_key, &model)?;

    Ok(Box::new(OpenAiEmbedding {
        text_expr: children.pop().unwrap(), // Take the third expression
        context,
    }))
}
