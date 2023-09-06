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

use core::option::Option::Some;
use core::result::Result::{Err, Ok};
use std::ffi::c_void;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::LazyLock;

use jni::strings::JNIString;
use jni::{InitArgsBuilder, JNIVersion, JavaVM, NativeMethod};
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;

use crate::run_this_func_to_get_valid_ptr_from_java_binding;

pub static JVM: LazyLock<Option<JavaVM>> = LazyLock::new(|| {
    let libs_path = if let Ok(libs_path) = std::env::var("CONNECTOR_LIBS_PATH") {
        libs_path
    } else if std::env::var("ENABLE_BUILD_RW_CONNECTOR").is_ok() {
        // If ENABLE_BUILD_RW_CONNECTOR is set which means it is started from risedev with a
        // connector.
        ".risingwave/bin/connector-node/libs/".to_string()
    } else {
        return None;
    };

    let dir = Path::new(&libs_path);

    if !dir.is_dir() {
        panic!("{} is not a directory", libs_path);
    }

    let mut class_vec = vec![];

    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.path().file_name() {
                class_vec.push(libs_path.to_owned() + name.to_str().to_owned().unwrap());
            }
        }
    } else {
        panic!("failed to read directory {}", libs_path);
    }

    let jvm_heap_size = if let Ok(heap_size) = std::env::var("JVM_HEAP_SIZE") {
        heap_size
    } else {
        // Use 10% of total memory by default
        format!("{}", total_memory_available_bytes() / 10)
    };

    // Build the VM properties
    let args_builder = InitArgsBuilder::new()
        // Pass the JNI API version (default is 8)
        .version(JNIVersion::V8)
        .option("-ea")
        .option("-Dis_embedded_connector=true")
        .option(format!("-Djava.class.path={}", class_vec.join(":")))
        .option(format!("-Xms{}", jvm_heap_size))
        .option(format!("-Xmx{}", jvm_heap_size));

    tracing::info!("JVM args: {:?}", args_builder);
    let jvm_args = args_builder.build().unwrap();

    // Create a new VM
    let jvm = match JavaVM::new(jvm_args) {
        Err(err) => {
            panic!("{:?}", err)
        }
        Ok(jvm) => jvm,
    };

    tracing::info!("initialize JVM successfully");
    Some(jvm)
});

static REGISTERED: AtomicBool = AtomicBool::new(false);

pub fn register_native_method_for_jvm() {
    // JVM is not initialized
    if JVM.is_none() {
        return;
    }

    // Ensure registering only once.
    if REGISTERED
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    let mut env = JVM
        .as_ref()
        .unwrap()
        .attach_current_thread()
        .inspect_err(|e| tracing::error!("jni call error: {:?}", e))
        .unwrap();

    // FIXME: remove this function might cause segment fault.
    run_this_func_to_get_valid_ptr_from_java_binding();

    let binding_class = env
        .find_class("com/risingwave/java/binding/Binding")
        .unwrap();
    env.register_native_methods(
        binding_class,
        &[
            NativeMethod {
                name: JNIString::from("vnodeCount"),
                sig: JNIString::from("()I"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_vnodeCount as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("hummockIteratorNew"),
                sig: JNIString::from("([B)J"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_hummockIteratorNew
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("hummockIteratorNext"),
                sig: JNIString::from("(J)J"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_hummockIteratorNext
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("hummockIteratorClose"),
                sig: JNIString::from("(J)V"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_hummockIteratorClose
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetKey"),
                sig: JNIString::from("(J)[B"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetKey as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetOp"),
                sig: JNIString::from("(J)I"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetOp as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowIsNull"),
                sig: JNIString::from("(JI)Z"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowIsNull as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetInt16Value"),
                sig: JNIString::from("(JI)S"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetInt16Value
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetInt32Value"),
                sig: JNIString::from("(JI)I"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetInt32Value
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetInt64Value"),
                sig: JNIString::from("(JI)J"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetInt64Value
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetFloatValue"),
                sig: JNIString::from("(JI)F"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetFloatValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetDoubleValue"),
                sig: JNIString::from("(JI)D"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetDoubleValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetBooleanValue"),
                sig: JNIString::from("(JI)Z"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetBooleanValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetStringValue"),
                sig: JNIString::from("(JI)Ljava/lang/String;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetStringValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetTimestampValue"),
                sig: JNIString::from("(JI)Ljava/sql/Timestamp;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetTimestampValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetDecimalValue"),
                sig: JNIString::from("(JI)Ljava/math/BigDecimal;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetDecimalValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetTimeValue"),
                sig: JNIString::from("(JI)Ljava/sql/Time;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetTimeValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetDateValue"),
                sig: JNIString::from("(JI)Ljava/sql/Date;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetDateValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetIntervalValue"),
                sig: JNIString::from("(JI)Ljava/lang/String;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetIntervalValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetJsonbValue"),
                sig: JNIString::from("(JI)Ljava/lang/String;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetJsonbValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetByteaValue"),
                sig: JNIString::from("(JI)[B"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetByteaValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowGetArrayValue"),
                sig: JNIString::from("(JILjava/lang/Class;)Ljava/lang/Object;"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowGetArrayValue
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("rowClose"),
                sig: JNIString::from("(J)V"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_rowClose as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("streamChunkIteratorNew"),
                sig: JNIString::from("([B)J"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_streamChunkIteratorNew
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("streamChunkIteratorNext"),
                sig: JNIString::from("(J)J"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_streamChunkIteratorNext
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("streamChunkIteratorClose"),
                sig: JNIString::from("(J)V"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_streamChunkIteratorClose
                    as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("streamChunkIteratorFromPretty"),
                sig: JNIString::from("(Ljava/lang/String;)J"),
                fn_ptr:
                    crate::Java_com_risingwave_java_binding_Binding_streamChunkIteratorFromPretty
                        as *mut c_void,
            },
            NativeMethod {
                name: JNIString::from("sendCdcSourceMsgToChannel"),
                sig: JNIString::from("(J[B)Z"),
                fn_ptr: crate::Java_com_risingwave_java_binding_Binding_sendCdcSourceMsgToChannel
                    as *mut c_void,
            },
        ],
    )
    .unwrap();

    tracing::info!("register native methods for jvm successfully");
}
