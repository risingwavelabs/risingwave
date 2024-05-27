// Copyright 2024 RisingWave Labs
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

use anyhow::bail;
use arrow_udf_js_deno::{CallMode, Runtime};
use futures_async_stream::try_stream;
use risingwave_common::array::arrow::{ToArrow, UdfArrowConvert};

use super::*;

#[linkme::distributed_slice(UDF_IMPLS)]
static DENO: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, runtime, _link| language == "javascript" && runtime == Some("deno"),
    create_fn: |opts| {
        let mut body = None;
        let mut compressed_binary = None;
        let identifier = opts.name.to_string();
        match (opts.using_link, opts.using_base64_decoded, opts.as_) {
            (None, None, Some(as_)) => body = Some(as_.to_string()),
            (Some(link), None, None) => {
                let bytes = read_file_from_link(link)?;
                compressed_binary = Some(zstd::stream::encode_all(bytes.as_slice(), 0)?);
            }
            (None, Some(bytes), None) => {
                compressed_binary = Some(zstd::stream::encode_all(bytes, 0)?);
            }
            (None, None, None) => bail!("Either USING or AS must be specified"),
            _ => bail!("Both USING and AS cannot be specified"),
        }
        Ok(CreateFunctionOutput {
            identifier,
            body,
            compressed_binary,
        })
    },
    build_fn: |opts| {
        let runtime = Runtime::new();
        let body = match (opts.body, opts.compressed_binary) {
            (Some(body), _) => body.to_string(),
            (_, Some(compressed_binary)) => {
                let binary = zstd::stream::decode_all(compressed_binary)
                    .context("failed to decompress binary")?;
                String::from_utf8(binary).context("failed to decode binary")?
            }
            _ => bail!("UDF body or compressed binary is required for deno UDF"),
        };

        let body = format!(
            "export {} {}({}) {{ {} }}",
            match opts.function_type {
                Some("sync") => "function",
                Some("async") => "async function",
                Some("generator") => "function*",
                Some("async_generator") => "async function*",
                _ if opts.kind.is_table() => "function*",
                _ => "function",
            },
            opts.identifier,
            opts.arg_names.join(","),
            body
        );

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(runtime.add_function(
                opts.identifier,
                UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
                CallMode::CalledOnNullInput,
                &body,
            ))
        })?;

        Ok(Box::new(DenoFunction {
            runtime,
            identifier: opts.identifier.to_string(),
        }))
    },
};

#[derive(Debug)]
struct DenoFunction {
    runtime: Arc<Runtime>,
    identifier: String,
}

#[async_trait::async_trait]
impl UdfImpl for DenoFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // FIXME(runji): make the future Send
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(self.runtime.call(&self.identifier, input.clone()))
        })
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        Ok(self.call_table_function_inner(input.clone()))
    }
}

impl DenoFunction {
    #[try_stream(boxed, ok = RecordBatch, error = anyhow::Error)]
    async fn call_table_function_inner<'a>(&'a self, input: RecordBatch) {
        let mut stream = self
            .runtime
            .call_table_function(&self.identifier, input, 1024)
            .await?;
        while let Some(batch) = stream.next().await {
            yield batch?;
        }
    }
}
