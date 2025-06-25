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

use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::{Context, bail};
use fs_err as fs;
use fs_err::PathExt;
use jni::{AttachGuard, InitArgsBuilder, JNIVersion, JavaVM};
use rw_resource_util::memory::system_memory_available_bytes;
use thiserror_ext::AsReport;
use tracing::error;

/// Use 10% of compute total memory by default. Compute node uses 0.7 * system memory by default.
const DEFAULT_MEMORY_PROPORTION: f64 = 0.07;

pub static JVM: JavaVmWrapper = JavaVmWrapper;
static INSTANCE: OnceLock<JavaVM> = OnceLock::new();

pub struct JavaVmWrapper;

impl JavaVmWrapper {
    /// Get the initialized JVM instance. If JVM is not initialized, initialize it first.
    /// If JVM cannot be initialized, return an error.
    pub fn get_or_init(&self) -> anyhow::Result<&'static JavaVM> {
        match INSTANCE.get_or_try_init(Self::inner_new) {
            Ok(jvm) => Ok(jvm),
            Err(e) => {
                error!(error = %e.as_report(), "jvm not initialized properly");
                Err(e.context("jvm not initialized properly"))
            }
        }
    }

    /// Get the initialized JVM instance. If JVM is not initialized, return None.
    ///
    /// Generally `get_or_init` should be preferred.
    pub fn get(&self) -> Option<&'static JavaVM> {
        INSTANCE.get()
    }

    fn locate_libs_path() -> anyhow::Result<PathBuf> {
        let libs_path = if let Ok(libs_path) = std::env::var("CONNECTOR_LIBS_PATH") {
            PathBuf::from(libs_path)
        } else {
            tracing::info!(
                "environment variable CONNECTOR_LIBS_PATH is not specified, use default path `./libs` instead"
            );
            std::env::current_exe()
                .and_then(|p| p.fs_err_canonicalize()) // resolve symlink of the current executable
                .context("unable to get path of the executable")?
                .parent()
                .expect("not root")
                .join("libs")
        };

        // No need to validate the path now, as it will be further checked when calling `fs::read_dir` later.
        Ok(libs_path)
    }

    fn inner_new() -> anyhow::Result<JavaVM> {
        let libs_path = Self::locate_libs_path().context("failed to locate connector libs")?;
        tracing::info!(path = %libs_path.display(), "located connector libs");

        let mut class_vec = vec![];

        let entries = fs::read_dir(&libs_path).context(if cfg!(debug_assertions) {
            "failed to read connector libs; \
            for RiseDev users, please check if ENABLE_BUILD_RW_CONNECTOR is set with `risedev configure`
            "
        } else {
            "failed to read connector libs, \
            please check if env var CONNECTOR_LIBS_PATH is correctly configured"
        })?;
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.file_name().is_some() {
                let path = fs::canonicalize(entry_path)
                    .expect("invalid entry_path obtained from fs::read_dir");
                class_vec.push(path.to_str().unwrap().to_owned());
            }
        }

        // move risingwave-source-cdc to the head of classpath, because we have some patched Debezium classes
        // in this jar which needs to be loaded first.
        let mut new_class_vec = Vec::with_capacity(class_vec.len());
        for path in class_vec {
            if path.contains("risingwave-source-cdc") {
                new_class_vec.insert(0, path.clone());
            } else {
                new_class_vec.push(path.clone());
            }
        }
        class_vec = new_class_vec;

        let jvm_heap_size = if let Ok(heap_size) = std::env::var("JVM_HEAP_SIZE") {
            heap_size
        } else {
            format!(
                "{}",
                (system_memory_available_bytes() as f64 * DEFAULT_MEMORY_PROPORTION) as usize
            )
        };

        // FIXME: passing custom arguments to the embedded jvm when compute node start
        // Build the VM properties
        let args_builder = InitArgsBuilder::new()
            // Pass the JNI API version (default is 8)
            .version(JNIVersion::V8)
            .option("-Dis_embedded_connector=true")
            .option(format!("-Djava.class.path={}", class_vec.join(":")))
            .option("--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED")
            .option("-Xms16m")
            .option(format!("-Xmx{}", jvm_heap_size));

        tracing::info!("JVM args: {:?}", args_builder);
        let jvm_args = args_builder.build().context("invalid jvm args")?;

        // Create a new VM
        let jvm = match JavaVM::new(jvm_args) {
            Err(err) => {
                tracing::error!(error = ?err.as_report(), "fail to new JVM");
                bail!("fail to new JVM");
            }
            Ok(jvm) => jvm,
        };

        tracing::info!("initialize JVM successfully");

        Ok(jvm)
    }
}

pub fn jvm_env(jvm: &JavaVM) -> Result<AttachGuard<'_>, jni::errors::Error> {
    jvm.attach_current_thread()
        .inspect_err(|e| tracing::error!(error = ?e.as_report(), "jvm attach thread error"))
}
