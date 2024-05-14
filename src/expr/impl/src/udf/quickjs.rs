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

use arrow_udf_js::{CallMode, Runtime};
use futures_util::StreamExt;
use risingwave_common::array::arrow::{ToArrow, UdfArrowConvert};

use super::*;

#[linkme::distributed_slice(UDF_RUNTIMES)]
static QUICKJS: UdfRuntimeDescriptor = UdfRuntimeDescriptor {
    language: "javascript",
    runtime: "quickjs",
    build: |opts| {
        let body = format!(
            "export function{} {}({}) {{ {} }}",
            if opts.table_function { "*" } else { "" },
            opts.name,
            opts.arg_names.join(","),
            opts.body.context("body is required")?,
        );
        let mut runtime = Runtime::new()?;
        runtime.add_function(
            opts.name,
            UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
            CallMode::CalledOnNullInput,
            &body,
        )?;
        Ok(Box::new(QuickJSFunction {
            runtime,
            name: opts.name.to_string(),
        }))
    },
};

#[derive(Debug)]
struct QuickJSFunction {
    runtime: Runtime,
    name: String,
}

#[async_trait::async_trait]
impl UdfRuntime for QuickJSFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.runtime.call(&self.name, input)
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        self.runtime
            .call_table_function(&self.name, input, 1024)
            .map(|s| futures_util::stream::iter(s).boxed())
    }
}
