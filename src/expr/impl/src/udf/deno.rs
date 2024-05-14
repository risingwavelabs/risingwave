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

#[linkme::distributed_slice(UDF_RUNTIMES)]
static DENO: UdfRuntimeDescriptor = UdfRuntimeDescriptor {
    language: "javascript",
    runtime: "deno",
    build: |opts| {
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
            match opts.function_type.as_deref() {
                Some("sync") => "function",
                Some("async") => "async function",
                Some("generator") => "function*",
                Some("async_generator") => "async function*",
                _ if opts.table_function => "function*",
                _ => "function",
            },
            opts.name,
            opts.arg_names.join(","),
            body
        );

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(runtime.add_function(
                opts.name,
                UdfArrowConvert::default().to_arrow_field("", &opts.return_type)?,
                CallMode::CalledOnNullInput,
                &body,
            ))
        })?;

        Ok(Box::new(DenoFunction {
            runtime,
            name: opts.name.to_string(),
        }))
    },
};

#[derive(Debug)]
struct DenoFunction {
    runtime: Arc<Runtime>,
    name: String,
}

#[async_trait::async_trait]
impl UdfRuntime for DenoFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // FIXME(runji): make the future Send
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.runtime.call(&self.name, input.clone()))
        })
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        Ok(self.call_table_function_inner(&self.name, input.clone()))
    }
}

impl DenoFunction {
    #[try_stream(boxed, ok = RecordBatch, error = anyhow::Error)]
    async fn call_table_function_inner<'a>(&'a self, name: &'a str, input: RecordBatch) {
        let mut stream = self.runtime.call_table_function(name, input, 1024).await?;
        while let Some(batch) = stream.next().await {
            yield batch?;
        }
    }
}
