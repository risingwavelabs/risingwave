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

use std::ffi::c_void;

use anyhow::{Context, bail};
use jni::objects::{JObject, JString};
use jni::{JNIEnv, JavaVM};
use risingwave_common::jvm_runtime::JVM;
use thiserror_ext::AsReport;
use tracing::error;

use crate::{call_method, call_static_method};

pub fn register_java_binding_native_methods(
    env: &mut JNIEnv<'_>,
) -> Result<(), jni::errors::Error> {
    let binding_class = env
        .find_class(gen_class_name!(com.risingwave.java.binding.Binding))
        .inspect_err(|e| tracing::error!(error = ?e.as_report(), "jvm find class error"))?;
    use crate::*;
    macro_rules! gen_native_method_array {
        () => {{
            $crate::for_all_native_methods! {gen_native_method_array}
        }};
        ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} })*}) => {
            [
                $(
                    $crate::gen_native_method_entry! {
                        Java_com_risingwave_java_binding_Binding_, $func_name, {$($ret)+}, {$($args)*}
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
            let result: Result<(usize, usize), anyhow::Error> = try {
                execute_with_jni_env(jvm, |env| {
                    let runtime_instance = crate::call_static_method!(
                        env,
                        {Runtime},
                        {Runtime getRuntime()}
                    )?;

                    let total_memory =
                        call_method!(env, runtime_instance.as_ref(), {long totalMemory()})?;
                    let free_memory =
                        call_method!(env, runtime_instance.as_ref(), {long freeMemory()})?;

                    Ok((total_memory as usize, (total_memory - free_memory) as usize))
                })?
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
    let mut env = jvm
        .attach_current_thread()
        .with_context(|| "Failed to attach current rust thread to jvm")?;

    // set context class loader for the thread
    // java.lang.Thread.currentThread()
    //     .setContextClassLoader(java.lang.ClassLoader.getSystemClassLoader());

    let thread = crate::call_static_method!(
        env,
        {Thread},
        {Thread currentThread()}
    )?;

    let system_class_loader = crate::call_static_method!(
        env,
        {ClassLoader},
        {ClassLoader getSystemClassLoader()}
    )?;

    crate::call_method!(
        env,
        thread,
        {void setContextClassLoader(ClassLoader)},
        &system_class_loader
    )?;

    let ret = f(&mut env);

    match env.exception_check() {
        Ok(true) => {
            let exception = env.exception_occurred().inspect_err(|e| {
                tracing::warn!(error = %e.as_report(), "Failed to get jvm exception");
            })?;
            env.exception_describe().inspect_err(|e| {
                tracing::warn!(error = %e.as_report(), "Failed to describe jvm exception");
            })?;
            env.exception_clear().inspect_err(|e| {
                tracing::warn!(error = %e.as_report(), "Exception occurred but failed to clear");
            })?;
            let message = call_method!(env, exception, {String getMessage()})?;
            let message = jobj_to_str(&mut env, message)?;
            return Err(anyhow::anyhow!("Caught Java Exception: {}", message));
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
    Ok(java_str.to_str()?.to_owned())
}

/// Dumps the JVM stack traces.
///
/// # Returns
///
/// - `Ok(None)` if JVM is not initialized.
/// - `Ok(Some(String))` if JVM is initialized and stack traces are dumped.
/// - `Err` if failed to dump stack traces.
pub fn dump_jvm_stack_traces() -> anyhow::Result<Option<String>> {
    match JVM.get() {
        None => Ok(None),
        Some(jvm) => execute_with_jni_env(jvm, |env| {
            let result = call_static_method!(
                env,
                {com.risingwave.connector.api.Monitor},
                {String dumpStackTrace()}
            )
            .with_context(|| "Failed to call Java function")?;
            let result = JString::from(result);
            let result = env
                .get_string(&result)
                .with_context(|| "Failed to convert JString")?;
            let result = result
                .to_str()
                .with_context(|| "Failed to convert JavaStr")?;
            Ok(Some(result.to_owned()))
        }),
    }
}
