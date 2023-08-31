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

#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(generators)]
#![feature(type_alias_impl_trait)]
#![feature(let_chains)]
#![feature(result_option_inspect)]
#![feature(lint_reasons)]
#![feature(impl_trait_in_assoc_type)]
#![feature(lazy_cell)]
#![cfg_attr(coverage, feature(no_coverage))]

#[macro_use]
extern crate tracing;

pub mod memory_management;
pub mod observer;
pub mod rpc;
pub mod server;
pub mod telemetry;

use std::ffi::c_void;
use std::future::Future;
use std::pin::Pin;

use clap::{Parser, ValueEnum};
use jni::objects::{JObject, JValue};
use jni::strings::JNIString;
use jni::NativeMethod;
use risingwave_common::config::{AsyncStackTraceOption, OverrideConfig};
use risingwave_common::jvm_runtime::JVM;
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;
use risingwave_java_binding::run_this_func_to_get_valid_ptr_from_java_binding;
use serde::{Deserialize, Serialize};

/// Command-line arguments for compute-node.
#[derive(Parser, Clone, Debug, OverrideConfig)]
#[command(
    version,
    about = "The worker node that executes query plans and handles data ingestion and output"
)]
pub struct ComputeNodeOpts {
    // TODO: rename to listen_addr and separate out the port.
    /// The address that this service listens to.
    /// Usually the localhost + desired port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5688")]
    pub listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// Optional, we will use listen_addr if not specified.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,

    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:1222"
    )]
    pub prometheus_listener_addr: String,

    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// Endpoint of the connector node
    #[clap(long, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// Payload format of connector sink rpc
    #[clap(long, env = "RW_CONNECTOR_RPC_SINK_PAYLOAD_FORMAT")]
    pub connector_rpc_sink_payload_format: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    /// Total available memory for the compute node in bytes. Used by both computing and storage.
    #[clap(long, env = "RW_TOTAL_MEMORY_BYTES", default_value_t = default_total_memory_bytes())]
    pub total_memory_bytes: usize,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[clap(long, env = "RW_PARALLELISM", default_value_t = default_parallelism())]
    #[override_opts(if_absent, path = streaming.actor_runtime_worker_threads_num)]
    pub parallelism: usize,

    /// Decides whether the compute node can be used for streaming and serving.
    #[clap(long, env = "RW_COMPUTE_NODE_ROLE", value_enum, default_value_t = default_role())]
    pub role: Role,

    /// Used for control the metrics level, similar to log level.
    /// 0 = disable metrics
    /// >0 = enable metrics
    #[clap(long, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<u32>,

    /// Path to data file cache data directory.
    /// Left empty to disable file cache.
    #[clap(long, env = "RW_DATA_FILE_CACHE_DIR")]
    #[override_opts(path = storage.data_file_cache.dir)]
    pub data_file_cache_dir: Option<String>,

    /// Path to meta file cache data directory.
    /// Left empty to disable file cache.
    #[clap(long, env = "RW_META_FILE_CACHE_DIR")]
    #[override_opts(path = storage.meta_file_cache.dir)]
    pub meta_file_cache_dir: Option<String>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[clap(long, env = "RW_ASYNC_STACK_TRACE", value_enum)]
    #[override_opts(path = streaming.async_stack_trace)]
    pub async_stack_trace: Option<AsyncStackTraceOption>,

    /// Enable heap profile dump when memory usage is high.
    #[clap(long, env = "RW_AUTO_DUMP_HEAP_PROFILE_DIR")]
    #[override_opts(path = server.auto_dump_heap_profile.dir)]
    pub auto_dump_heap_profile_dir: Option<String>,

    #[clap(long, env = "RW_OBJECT_STORE_STREAMING_READ_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_streaming_read_timeout_ms)]
    pub object_store_streaming_read_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_STREAMING_UPLOAD_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_streaming_upload_timeout_ms)]
    pub object_store_streaming_upload_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_UPLOAD_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_upload_timeout_ms)]
    pub object_store_upload_timeout_ms: Option<u64>,
    #[clap(long, env = "RW_OBJECT_STORE_READ_TIMEOUT_MS", value_enum)]
    #[override_opts(path = storage.object_store_read_timeout_ms)]
    pub object_store_read_timeout_ms: Option<u64>,
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum Role {
    Serving,
    Streaming,
    #[default]
    Both,
}

impl Role {
    pub fn for_streaming(&self) -> bool {
        match self {
            Role::Serving => false,
            Role::Streaming => true,
            Role::Both => true,
        }
    }

    pub fn for_serving(&self) -> bool {
        match self {
            Role::Serving => true,
            Role::Streaming => false,
            Role::Both => true,
        }
    }
}

fn validate_opts(opts: &ComputeNodeOpts) {
    let total_memory_available_bytes = total_memory_available_bytes();
    if opts.total_memory_bytes > total_memory_available_bytes {
        let error_msg = format!("total_memory_bytes {} is larger than the total memory available bytes {} that can be acquired.", opts.total_memory_bytes, total_memory_available_bytes);
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    if opts.parallelism == 0 {
        let error_msg = "parallelism should not be zero";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
    let total_cpu_available = total_cpu_available().ceil() as usize;
    if opts.parallelism > total_cpu_available {
        let error_msg = format!(
            "parallelism {} is larger than the total cpu available {} that can be acquired.",
            opts.parallelism, total_cpu_available
        );
        tracing::warn!(error_msg);
    }
}

use crate::server::compute_node_serve;

/// Start compute node
pub fn start(opts: ComputeNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        tracing::info!("options: {:?}", opts);
        validate_opts(&opts);

        let listen_addr = opts.listen_addr.parse().unwrap();
        tracing::info!("Server Listening at {}", listen_addr);

        let advertise_addr = opts
            .advertise_addr
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("advertise addr is not specified, defaulting to listen_addr");
                &opts.listen_addr
            })
            .parse()
            .unwrap();
        tracing::info!("advertise addr is {}", advertise_addr);

        let (join_handle_vec, _shutdown_send) =
            compute_node_serve(listen_addr, advertise_addr, opts).await;

        tokio::task::spawn_blocking(move || {
            run_jvm();
        });

        for join_handle in join_handle_vec {
            join_handle.await.unwrap();
        }
    })
}

fn run_jvm() {
    let mut env = JVM.attach_current_thread_as_daemon().unwrap();

    // FIXME: remove this function would cause segment fault.
    run_this_func_to_get_valid_ptr_from_java_binding();

    let binding_class = env
        .find_class("com/risingwave/java/binding/Binding")
        .unwrap();
    env.register_native_methods(binding_class, &[
        NativeMethod {
            name: JNIString::from("vnodeCount"),
            sig: JNIString::from("()I"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_vnodeCount as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("hummockIteratorNew"),
            sig: JNIString::from("([B)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_hummockIteratorNew as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("hummockIteratorNext"),
            sig: JNIString::from("(J)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_hummockIteratorNext as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("hummockIteratorClose"),
            sig: JNIString::from("(J)V"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_hummockIteratorClose as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetKey"),
            sig: JNIString::from("(J)[B"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetKey as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetOp"),
            sig: JNIString::from("(J)I"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetOp as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowIsNull"),
            sig: JNIString::from("(JI)Z"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowIsNull as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetInt16Value"),
            sig: JNIString::from("(JI)S"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetInt16Value as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetInt32Value"),
            sig: JNIString::from("(JI)I"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetInt32Value as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetInt64Value"),
            sig: JNIString::from("(JI)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetInt64Value as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetFloatValue"),
            sig: JNIString::from("(JI)F"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetFloatValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetDoubleValue"),
            sig: JNIString::from("(JI)D"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetDoubleValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetBooleanValue"),
            sig: JNIString::from("(JI)Z"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetBooleanValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetStringValue"),
            sig: JNIString::from("(JI)Ljava/lang/String;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetStringValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetTimestampValue"),
            sig: JNIString::from("(JI)Ljava/sql/Timestamp;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetTimestampValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetDecimalValue"),
            sig: JNIString::from("(JI)Ljava/math/BigDecimal;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetDecimalValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetTimeValue"),
            sig: JNIString::from("(JI)Ljava/sql/Time;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetTimeValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetDateValue"),
            sig: JNIString::from("(JI)Ljava/sql/Date;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetDateValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetIntervalValue"),
            sig: JNIString::from("(JI)Ljava/lang/String;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetIntervalValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetJsonbValue"),
            sig: JNIString::from("(JI)Ljava/lang/String;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetJsonbValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetByteaValue"),
            sig: JNIString::from("(JI)[B"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetByteaValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowGetArrayValue"),
            sig: JNIString::from("(JILjava/lang/Class;)Ljava/lang/Object;"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowGetArrayValue as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("rowClose"),
            sig: JNIString::from("(J)V"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_rowClose as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("streamChunkIteratorNew"),
            sig: JNIString::from("([B)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_streamChunkIteratorNew as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("streamChunkIteratorNext"),
            sig: JNIString::from("(J)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_streamChunkIteratorNext as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("streamChunkIteratorClose"),
            sig: JNIString::from("(J)V"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_streamChunkIteratorClose as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("streamChunkIteratorFromPretty"),
            sig: JNIString::from("(Ljava/lang/String;)J"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_streamChunkIteratorFromPretty as *mut c_void,
        },


        NativeMethod {
            name: JNIString::from("sendMsgToChannel"),
            sig: JNIString::from("(J[B)Z"),
            fn_ptr: risingwave_java_binding::Java_com_risingwave_java_binding_Binding_sendMsgToChannel as *mut c_void,
        },

    ]).unwrap();

    let string_class = env.find_class("java/lang/String").unwrap();
    let jarray = env
        .new_object_array(0, string_class, JObject::null())
        .unwrap();

    let _ = env
        .call_static_method(
            "com/risingwave/connector/ConnectorService",
            "main",
            "([Ljava/lang/String;)V",
            &[JValue::Object(&jarray)],
        )
        .inspect_err(|e| eprintln!("{:?}", e));
}

fn default_total_memory_bytes() -> usize {
    total_memory_available_bytes()
}

fn default_parallelism() -> usize {
    total_cpu_available().ceil() as usize
}

fn default_role() -> Role {
    Role::Both
}
