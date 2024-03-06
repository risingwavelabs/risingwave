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

use core::option::Option::Some;
use std::ffi::c_void;
use std::path::PathBuf;
use std::sync::OnceLock;

use anyhow::{bail, Context};
use fs_err as fs;
use fs_err::PathExt;
use jni::objects::{JObject, JString};
use jni::strings::JNIString;
use jni::{InitArgsBuilder, JNIEnv, JNIVersion, JavaVM, NativeMethod};
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use thiserror_ext::AsReport;
use tracing::error;

use crate::call_method;

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
    fn get(&self) -> Option<&'static JavaVM> {
        INSTANCE.get()
    }

    fn locate_libs_path() -> anyhow::Result<PathBuf> {
        let libs_path = if let Ok(libs_path) = std::env::var("CONNECTOR_LIBS_PATH") {
            PathBuf::from(libs_path)
        } else {
            tracing::info!("environment variable CONNECTOR_LIBS_PATH is not specified, use default path `./libs` instead");
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

        let entries = fs::read_dir(&libs_path).context("failed to read connector libs")?;
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.file_name().is_some() {
                let path = fs::canonicalize(entry_path)
                    .expect("invalid entry_path obtained from fs::read_dir");
                class_vec.push(path.to_str().unwrap().to_string());
            }
        }

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
            .option("-Xms16m")
            .option(format!("-Xmx{}", jvm_heap_size))
            // Quoted from the debezium document:
            // > Your application should always properly stop the engine to ensure graceful and complete
            // > shutdown and that each source record is sent to the application exactly one time.
            // In RisingWave we assume the upstream changelog may contain duplicate events and
            // handle conflicts in the mview operator, thus we don't need to obey the above
            // instructions. So we decrease the wait time here to reclaim jvm thread faster.
            .option("-Ddebezium.embedded.shutdown.pause.before.interrupt.ms=1")
            .option("-Dcdc.source.wait.streaming.before.exit.seconds=30");

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

        register_native_method_for_jvm(&jvm).context("failed to register native method")?;

        Ok(jvm)
    }
}

pub fn register_native_method_for_jvm(jvm: &JavaVM) -> Result<(), jni::errors::Error> {
    let mut env = jvm
        .attach_current_thread()
        .inspect_err(|e| tracing::error!(error = ?e.as_report(), "jvm attach thread error"))?;

    let binding_class = env
        .find_class("com/risingwave/java/binding/Binding")
        .inspect_err(|e| tracing::error!(error = ?e.as_report(), "jvm find class error"))?;
    use crate::*;
    macro_rules! gen_native_method_array {
        () => {{
            $crate::for_all_native_methods! {gen_native_method_array}
        }};
        ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} })*}) => {
            [
                $(
                    {
                        let fn_ptr = paste::paste! {[<Java_com_risingwave_java_binding_Binding_ $func_name> ]} as *mut c_void;
                        let sig = $crate::gen_jni_sig! { {$($ret)+}, {$($args)*}};
                        NativeMethod {
                            name: JNIString::from(stringify! {$func_name}),
                            sig: JNIString::from(sig),
                            fn_ptr,
                        }
                    },
                )*
            ]
        }
    }
    env.register_native_methods(binding_class, &gen_native_method_array!())
        .inspect_err(
            |e| tracing::error!(error = ?e.as_report(), "jvm register native methods error"),
        )?;

    tracing::info!("register native methods for jvm successfully");
    Ok(())
}

/// Load JVM memory statistics from the runtime. If JVM is not initialized or fail to initialize,
/// return zero.
pub fn load_jvm_memory_stats() -> (usize, usize) {
    match JVM.get() {
        Some(jvm) => {
            let result: Result<(usize, usize), jni::errors::Error> = try {
                let mut env = jvm.attach_current_thread()?;

                let runtime_instance = crate::call_static_method!(
                    env,
                    {Runtime},
                    {Runtime getRuntime()}
                )?;

                let total_memory =
                    call_method!(env, runtime_instance.as_ref(), {long totalMemory()})?;
                let free_memory =
                    call_method!(env, runtime_instance.as_ref(), {long freeMemory()})?;

                (total_memory as usize, (total_memory - free_memory) as usize)
            };
            match result {
                Ok(ret) => ret,
                Err(e) => {
                    error!(error = ?e.as_report(), "failed to collect jvm stats");
                    (0, 0)
                }
            }
        }
        _ => (0, 0),
    }
}

pub fn execute_with_jni_env<T>(
    jvm: &JavaVM,
    f: impl FnOnce(&mut JNIEnv<'_>) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let _guard = jvm
        .attach_current_thread()
        .with_context(|| "Failed to attach current rust thread to jvm")?;

    let mut env = jvm.get_env().with_context(|| "Failed to get jni env")?;

    let ret = f(&mut env);

    match env.exception_check() {
        Ok(true) => {
            env.exception_describe().inspect_err(|e| {
                tracing::warn!(error = %e.as_report(), "Failed to describe jvm exception");
            })?;
            env.exception_clear().inspect_err(|e| {
                tracing::warn!(error = %e.as_report(), "Exception occurred but failed to clear");
            })?;
        }
        Ok(false) => {
            // No exception, do nothing
        }
        Err(e) => {
            tracing::warn!(error = %e.as_report(), "Failed to check exception");
        }
    }

    ret
}

/// A helper method to convert an java object to rust string.
pub fn jobj_to_str(env: &mut JNIEnv<'_>, obj: JObject<'_>) -> anyhow::Result<String> {
    if !env.is_instance_of(&obj, "java/lang/String")? {
        bail!("Input object is not a java string and can't be converted!")
    }
    let jstr = JString::from(obj);
    let java_str = env.get_string(&jstr)?;
    Ok(java_str.to_str()?.to_string())
}
