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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::bail;
use async_openai::Client;
use async_openai::config::OpenAIConfig;
use async_openai::types::{
    CreateEmbeddingRequest, CreateEmbeddingRequestArgs, Embedding, EmbeddingInput,
};
use futures_util::{StreamExt, TryStreamExt};
use risingwave_common::array::arrow::arrow_array_udf::{
    Array, ArrayRef, Float32Array, RecordBatch, StringArray,
};
use risingwave_common::array::arrow::arrow_schema_udf::{self, DataType as ArrowDataType, Field};
use risingwave_common::array::arrow::{IcebergToArrow, UdfArrowConvert, UdfFromArrow};
use risingwave_common::array::{
    ArrayBuilderImpl, ArrayImpl, ArrayImplBuilder, DataChunk, ListArray, ListValue,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, Datum, F32, Scalar, ScalarImpl, ScalarRefImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_expr::sig::UdfKind;
use risingwave_pb::catalog::PbFunction;
use thiserror_ext::AsReport;

use super::*;

/// Supported AI model functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AiModelFunctionKind {
    OpenaiEmbedding,
}

impl AiModelFunctionKind {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "openai_embedding" => Some(Self::OpenaiEmbedding),
            _ => None,
        }
    }
}

#[linkme::distributed_slice(UDF_IMPLS)]
static AI_MODEL: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |_language, runtime, _link| runtime == Some("_ai_model"),
    create_fn: |opts| {
        let name_in_runtime = opts.as_.context("AS must be specified")?;
        let hyper_params = opts
            .hyper_params
            .expect("hyper_params must be provided for AI model function");

        // check if the AI function is supported
        let Some(kind) = AiModelFunctionKind::from_name(name_in_runtime) else {
            bail!("Unsupported AI model: {}", name_in_runtime);
        };

        match kind {
            AiModelFunctionKind::OpenaiEmbedding => {
                // check function signature
                if opts.kind != UdfKind::Scalar {
                    bail!("`openai_embedding` should be created as a scalar function");
                }
                match (opts.arg_types, opts.return_type) {
                    ([DataType::Varchar], DataType::List(_)) => {
                        if opts.return_type.as_list_element_type() != &DataType::Float32 {
                            bail!("`openai_embedding` should return a `float4[]`");
                        }
                    }
                    _ => bail!(
                        "`openai_embedding` model should accept a `text` argument and return a `float4[]`"
                    ),
                }
                // check required options
                hyper_params
                    .get("api_key")
                    .context("`api_key` hyper parameter must be provided in WITH option")?;
                hyper_params
                    .get("model")
                    .context("`model` hyper parameter must be provided in WITH option")?;
            }
        }

        Ok(CreateFunctionOutput {
            name_in_runtime: name_in_runtime.to_owned(),
            body: None,
            compressed_binary: None,
        })
    },
    build_fn: |opts| {
        let name_in_runtime = opts.name_in_runtime;
        let hyper_params = opts
            .hyper_params
            .expect("hyper_params must be provided for AI model function");

        let Some(kind) = AiModelFunctionKind::from_name(name_in_runtime) else {
            bail!("Unsupported AI model function: {}", name_in_runtime);
        };

        match kind {
            AiModelFunctionKind::OpenaiEmbedding => {
                let api_key = hyper_params
                    .get("api_key")
                    .with_context(|| "`api_key` is required for `openai_embedding` function")?;

                let model = hyper_params
                    .get("model")
                    .with_context(|| "`model` is required for `openai_embedding` function")?;

                let config = OpenAIConfig::new().with_api_key(api_key);
                let client = Client::with_config(config);

                Ok(Box::new(OpenaiEmbeddingFunction {
                    client,
                    model: model.to_owned(),
                    arrow_convert: UdfArrowConvert::default(),
                }))
            }
        }
    },
};

#[derive(Debug)]
struct OpenaiEmbeddingFunction {
    client: Client<OpenAIConfig>,
    model: String,
    arrow_convert: UdfArrowConvert,
}

#[async_trait::async_trait]
impl UdfImpl for OpenaiEmbeddingFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let column = input.column(0);
        let n_rows = column.len();

        // Get the string array
        let string_array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .with_context(|| "Expected string array")?;

        // Prepare batch input and tracking non-null inputs
        let mut input_texts = Vec::with_capacity(n_rows);
        let mut index_map = Vec::with_capacity(n_rows);

        // Collect all non-null values with their indices
        for (i, value_opt) in string_array.iter().enumerate() {
            if let Some(value) = value_opt {
                input_texts.push(value.to_owned());
                index_map.push(i);
            }
        }

        // Initialize embeddings vector with None values
        let mut embeddings = vec![None; n_rows];

        // Only call the API if we have non-null inputs
        if !input_texts.is_empty() {
            // Call the OpenAI API for batch embeddings
            let embedding_request = CreateEmbeddingRequestArgs::default()
                .model(&self.model)
                .input(EmbeddingInput::StringArray(input_texts))
                .build()?;

            let response = match self.client.embeddings().create(embedding_request).await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Failed to get embedding from OpenAI");
                    bail!("Failed to get embedding from OpenAI");
                }
            };

            if response.data.is_empty() {
                bail!("No embedding data returned from OpenAI");
            }

            // Map the returned embeddings to their original positions
            for (batch_idx, &orig_idx) in index_map.iter().enumerate() {
                if batch_idx < response.data.len() {
                    let embedding_values = &response.data[batch_idx].embedding;
                    let float_array = risingwave_common::array::F32Array::from_iter(
                        embedding_values.iter().map(|&v| Some(F32::from(v))),
                    );

                    let float_array_impl = float_array.into();
                    let list_value = ListValue::new(float_array_impl);
                    embeddings[orig_idx] = Some(list_value);
                } else {
                    bail!("OpenAI API returned fewer embeddings than requested");
                }
            }
        }

        let vector_type = DataType::List(Box::new(DataType::Float32));
        let output_array = {
            let mut builder = vector_type.create_array_builder(n_rows);
            for embedding in embeddings {
                builder.append(
                    embedding
                        .as_ref()
                        .map(|v| ScalarRefImpl::from(v.as_scalar_ref())),
                );
            }
            builder.finish()
        };

        let float_field = Field::new("item", ArrowDataType::Float32, false);
        let arrow_vector_type = ArrowDataType::List(Arc::new(float_field));

        let arrow_array = self
            .arrow_convert
            .to_array(&arrow_vector_type, &output_array)
            .with_context(|| "Failed to convert to Arrow array")?;

        let schema = Arc::new(arrow_schema_udf::Schema::new(vec![Field::new(
            "embedding",
            arrow_array.data_type().clone(),
            false,
        )]));

        let result = RecordBatch::try_new(schema, vec![arrow_array])?;
        Ok(result)
    }

    async fn call_table_function<'a>(
        &'a self,
        _input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        unreachable!("`openai_embedding` is not a table function");
    }
}
