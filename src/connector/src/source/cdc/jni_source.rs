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

use anyhow::Context;
use risingwave_common::global_jvm::JVM;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_jni_core::{call_method, call_static_method};

pub fn commit_cdc_offset(source_id: u64, encoded_offset: String) -> anyhow::Result<()> {
    let jvm = JVM.get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        // get source handler by source id
        let handler = call_static_method!(
            env,
            {com.risingwave.connector.source.core.JniDbzSourceRegistry},
            {com.risingwave.connector.source.core.JniDbzSourceHandler getSourceHandler(long sourceId)},
            source_id
        )?;

        let offset_str = env.new_string(&encoded_offset).with_context(|| {
            format!("Failed to create jni string from source offset: {encoded_offset}.")
        })?;
        // commit offset to upstream
        call_method!(env, handler, {void commitOffset(String)}, &offset_str).with_context(
            || format!("Failed to commit offset to upstream for source: {source_id}."),
        )?;
        Ok(())
    })
}
