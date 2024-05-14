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

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow_udf_wasm::Runtime;
use futures_util::StreamExt;

use super::*;

#[linkme::distributed_slice(UDF_RUNTIMES)]
static WASM: UdfRuntimeDescriptor = UdfRuntimeDescriptor {
    language: "wasm",
    runtime: "",
    build: |opts| {
        let compressed_binary = opts
            .compressed_binary
            .context("compressed binary is required")?;
        let wasm_binary = zstd::stream::decode_all(compressed_binary)
            .context("failed to decompress wasm binary")?;
        let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
        Ok(Box::new(WasmFunction {
            runtime,
            name: opts.name.to_string(),
        }))
    },
};

#[derive(Debug)]
struct WasmFunction {
    runtime: Arc<Runtime>,
    name: String,
}

#[async_trait::async_trait]
impl UdfRuntime for WasmFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.runtime.call(&self.name, input)
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        self.runtime
            .call_table_function(&self.name, input)
            .map(|s| futures_util::stream::iter(s).boxed())
    }

    fn is_legacy(&self) -> bool {
        // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
        self.runtime.abi_version().0 <= 2
    }
}

/// Get or create a wasm runtime.
///
/// Runtimes returned by this function are cached inside for at least 60 seconds.
/// Later calls with the same binary will reuse the same runtime.
fn get_or_create_wasm_runtime(binary: &[u8]) -> Result<Arc<Runtime>> {
    static RUNTIMES: LazyLock<moka::sync::Cache<md5::Digest, Arc<Runtime>>> = LazyLock::new(|| {
        moka::sync::Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build()
    });

    let md5 = md5::compute(binary);
    if let Some(runtime) = RUNTIMES.get(&md5) {
        return Ok(runtime.clone());
    }

    let runtime = Arc::new(arrow_udf_wasm::Runtime::new(binary)?);
    RUNTIMES.insert(md5, runtime.clone());
    Ok(runtime)
}
