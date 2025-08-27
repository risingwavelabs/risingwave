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

use std::sync::OnceLock;

use anyhow::Context as _;
use jni::JavaVM;

pub static JVM: JavaVmWrapper = JavaVmWrapper;
static INSTANCE: OnceLock<JavaVM> = OnceLock::new();
pub type JvmBuilder = fn() -> anyhow::Result<JavaVM>;

pub struct JavaVmWrapper;

/// Registered JVM builder from other crates. Should only be one.
#[linkme::distributed_slice]
pub static JVM_BUILDER: [JvmBuilder];

impl JavaVmWrapper {
    /// Get the global singleton JVM instance, initializing it with the registered builder if not already initialized.
    pub fn get_or_init(&self) -> anyhow::Result<&'static JavaVM> {
        INSTANCE.get_or_try_init(|| {
            let builder = JVM_BUILDER
                .iter()
                .next()
                .context("no JVM builder is registered")?;
            builder()
        })
    }

    /// Get the global singleton JVM instance, returning None if not initialized.
    pub fn get(&self) -> Option<&'static JavaVM> {
        INSTANCE.get()
    }
}
