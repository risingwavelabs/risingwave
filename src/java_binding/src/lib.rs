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

#![feature(type_alias_impl_trait)]
#![feature(try_blocks)]

mod hummock_iterator;
use std::ffi::c_void;
use std::ops::Deref;

use anyhow::anyhow;
use cfg_or_panic::cfg_or_panic;
use jni::objects::JByteArray;
use jni::sys::{JNI_VERSION_1_2, jint};
use jni::{JNIEnv, JavaVM};
use prost::Message;
use risingwave_common::error::AsReport;
use risingwave_common::jvm_runtime::jvm_env;
use risingwave_jni_core::jvm_runtime::register_java_binding_native_methods;
use risingwave_jni_core::{
    EnvParam, JAVA_BINDING_ASYNC_RUNTIME, JavaBindingIterator, Pointer, execute_and_catch,
    gen_class_name, to_guarded_slice,
};

use crate::hummock_iterator::new_hummock_java_binding_iter;

fn register_hummock_java_binding_native_methods(
    env: &mut JNIEnv<'_>,
) -> Result<(), jni::errors::Error> {
    let binding_class = env
        .find_class(gen_class_name!(com.risingwave.java.binding.HummockIterator))
        .inspect_err(|e| tracing::error!(error = ?e.as_report(), "jvm find class error"))?;
    macro_rules! gen_native_method_array {
        () => {{
            risingwave_jni_core::split_extract_plain_native_methods! {{long iteratorNewHummock(byte[] readPlan);}, gen_native_method_array}
        }};
        ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} })*}) => {
            [
                $(
                    risingwave_jni_core::gen_native_method_entry! {
                        Java_com_risingwave_java_binding_HummockIterator_, $func_name, {$($ret)+}, {$($args)*}
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

#[unsafe(no_mangle)]
#[allow(non_snake_case)]
pub extern "system" fn JNI_OnLoad(jvm: JavaVM, _reserved: *mut c_void) -> jint {
    let result: Result<(), jni::errors::Error> = try {
        let mut env = jvm_env(&jvm)?;
        register_java_binding_native_methods(&mut env)?;
        register_hummock_java_binding_native_methods(&mut env)?;
    };
    let _ =
        result.inspect_err(|e| eprintln!("unable to register native method: {:?}", e.as_report()));

    JNI_VERSION_1_2
}

#[cfg_or_panic(not(madsim))]
#[unsafe(no_mangle)]
extern "system" fn Java_com_risingwave_java_binding_HummockIterator_iteratorNewHummock<'a>(
    env: EnvParam<'a>,
    read_plan: JByteArray<'a>,
) -> Pointer<'static, JavaBindingIterator<'static>> {
    execute_and_catch(env, move |env| {
        let read_plan = Message::decode(to_guarded_slice(&read_plan, env)?.deref())?;
        let iter = JAVA_BINDING_ASYNC_RUNTIME
            .block_on(new_hummock_java_binding_iter(read_plan))
            .map_err(|e| anyhow!(e))?;
        let iter = JavaBindingIterator::new_hummock_iter(iter);
        Ok(iter.into())
    })
}
