// Copyright 2023 RisingWave Labs
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

#![feature(result_option_inspect)]

use std::ffi::c_void;

use jni::sys::{jint, JNI_VERSION_1_2};
use jni::JavaVM;
use risingwave_jni_core::register_native_method_for_jvm;

#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn JNI_OnLoad(jvm: JavaVM, _reserved: *mut c_void) -> jint {
    let _ = register_native_method_for_jvm(&jvm)
        .inspect_err(|_e| eprintln!("unable to register native method"));
    JNI_VERSION_1_2
}
