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
            bail!("Unsupported AI model function: {}", name_in_runtime);
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
                        "`openai_embedding` should have a `text` argument and return a `float4[]`"
                    ),
                }
                // check required options
                hyper_params
                    .get("api_key")
                    .context("`api_key` option is required")?;
                hyper_params
                    .get("model")
                    .context("`model` option is required")?;
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

                Ok(Box::new(OpenAiEmbeddingFunction {
                    api_key: api_key.to_owned(),
                    model: model.to_owned(),
                    arrow_convert: UdfArrowConvert::default(),
                }))
            }
        }
    },
};

#[derive(Debug)]
struct OpenAiEmbeddingFunction {
    api_key: String,
    model: String,
    arrow_convert: UdfArrowConvert,
}

#[async_trait::async_trait]
impl UdfImpl for OpenAiEmbeddingFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let column = input.column(0);
        let n_rows = column.len();

        let input_chunk = self.arrow_convert.from_record_batch(input)?;
        println!("[rc] input_chunk: \n{}", input_chunk.to_pretty());

        let mut embeddings = Vec::with_capacity(n_rows);

        // TODO(): call the OpenAI API here
        for i in 0..n_rows {
            if column.is_null(i) {
                embeddings.push(None);
                continue;
            }

            let float_array = risingwave_common::array::F32Array::from_iter([
                Some(F32::from(0.1)),
                Some(F32::from(0.2)),
                Some(F32::from(0.3)),
                Some(F32::from(0.4)),
                Some(F32::from(0.5)),
            ]);

            let float_array_impl = float_array.into();
            let list_value = ListValue::new(float_array_impl);
            embeddings.push(Some(list_value));
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
