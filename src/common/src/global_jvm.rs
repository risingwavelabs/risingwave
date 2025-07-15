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

use std::sync::{Mutex, OnceLock};

use jni::JavaVM;

pub static JVM: JavaVmWrapper = JavaVmWrapper;
static INSTANCE: OnceLock<JavaVM> = OnceLock::new();
static JVM_BUILDER: Mutex<Option<Box<dyn Fn() -> JavaVM + Send + Sync>>> = Mutex::new(None);

pub struct JavaVmWrapper;

impl JavaVmWrapper {
    pub fn register_jvm_builder(&self, builder: Box<dyn Fn() -> JavaVM + Send + Sync>) {
        let mut guard = JVM_BUILDER.lock().unwrap();
        if guard.is_some() {
            tracing::warn!("JVM builder already registered, overwriting the previous one.");
        }
        *guard = Some(builder);
    }

    /// Get the global singleton JVM instance, initializing it with the registered closure if not already initialized.
    pub fn get_or_init(&self) -> anyhow::Result<&'static JavaVM> {
        INSTANCE.get_or_try_init(|| {
            let guard = JVM_BUILDER.lock().unwrap();
            let builder = guard.as_ref().ok_or_else(|| {
                anyhow::anyhow!("JVM builder must be registered (and only once) before get_or_init")
            })?;
            Ok(builder())
        })
    }

    /// Get the global singleton JVM instance, returning None if not initialized.
    pub fn get(&self) -> Option<&'static JavaVM> {
        INSTANCE.get()
    }
}
