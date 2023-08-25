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
#![cfg_attr(coverage, feature(no_coverage))]

#[macro_use]
extern crate tracing;

pub mod memory_management;
pub mod observer;
pub mod rpc;
pub mod server;
pub mod telemetry;

use std::ffi::c_void;
use clap::{Parser, ValueEnum};
use risingwave_common::config::{AsyncStackTraceOption, OverrideConfig};
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::total_memory_available_bytes;
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

use std::future::Future;
use std::marker::PhantomData;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr;
use std::ptr::null;
use jni::{JNIEnv, NativeMethod};
use jni::objects::{JClass, JObject, JString, JValue, JValueGen};
use jni::strings::JNIString;
use jni::sys::{jint, jlong};
use risingwave_common::jvm_runtime::{JVM, MyJniSender};
use risingwave_pb::connector_service::{CdcMessage, GetEventStreamResponse};

use crate::server::compute_node_serve;

/// Start compute node
pub fn start(
    opts: ComputeNodeOpts,
    registry: prometheus::Registry,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
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
            compute_node_serve(listen_addr, advertise_addr, opts, registry).await;

        tokio::task::spawn_blocking(move || {
            run_jvm();
        });


        for join_handle in join_handle_vec {
            join_handle.await.unwrap();
        }
    })
}

#[repr(C)]
pub struct EnvParam<'a> {
    env: JNIEnv<'a>,
    class: JClass<'a>,
}

impl<'a> Deref for EnvParam<'a> {
    type Target = JNIEnv<'a>;

    fn deref(&self) -> &Self::Target {
        &self.env
    }
}

impl<'a> DerefMut for EnvParam<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.env
    }
}

impl<'a> EnvParam<'a> {
    pub fn get_class(&self) -> &JClass<'a> {
        &self.class
    }
}

#[repr(transparent)]
pub struct Pointer<'a, T> {
    pointer: jlong,
    _phantom: PhantomData<&'a T>,
}

impl<'a, T> Default for Pointer<'a, T> {
    fn default() -> Self {
        Self {
            pointer: 0,
            _phantom: Default::default(),
        }
    }
}

impl<T> From<T> for Pointer<'static, T> {
    fn from(value: T) -> Self {
        Pointer {
            pointer: Box::into_raw(Box::new(value)) as jlong,
            _phantom: PhantomData,
        }
    }
}

impl<T> Pointer<'static, T> {
    fn null() -> Self {
        Pointer {
            pointer: 0,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T> Pointer<'a, T> {
    fn as_ref(&self) -> &'a T {
        debug_assert!(self.pointer != 0);
        unsafe { &*(self.pointer as *const T) }
    }

    fn as_mut(&mut self) -> &'a mut T {
        debug_assert!(self.pointer != 0);
        unsafe { &mut *(self.pointer as *mut T) }
    }

    fn drop(self) {
        debug_assert!(self.pointer != 0);
        unsafe { drop(Box::from_raw(self.pointer as *mut T)) }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendMsgToChannel<'a>(
    mut env: EnvParam<'a>,
    channel: Pointer<'a, MyJniSender>,
    mut msg: JObject<'a>,
) {
    let source_id = env.env.call_method(&mut msg, "getSourceId", "()J", &[]).unwrap();
    let source_id = source_id.j().unwrap();

    let events_list = env.env.call_method(&mut msg, "getEventsList", "()Ljava/util/List;", &[]).unwrap();
    let mut events_list = match events_list {
        JValueGen::Object(obj) => obj,
        _ => unreachable!()
    };


    let size = env.env.call_method(&mut events_list, "size", "()I", &[]).unwrap().i().unwrap();
    let mut events = Vec::with_capacity(size as usize);
    for i in 0..size {
        let java_element = env.call_method(&mut events_list, "get", "(I)Ljava/lang/Object;", &[JValue::from(i as i32)]).unwrap();
        let mut java_element = match java_element {
            JValueGen::Object(obj) => obj,
            _ => unreachable!()
        };
        let payload = env.call_method(&mut java_element, "getPayload", "()Ljava/lang/String;", &[]).unwrap();
        let payload = match payload {
            JValueGen::Object(obj) => obj,
            _ => unreachable!()
        };
        let payload: String = env.get_string(&JString::from(payload)).unwrap().into();

        let partition = env.call_method(&mut java_element, "getPartition", "()Ljava/lang/String;", &[]).unwrap();
        let partition = match partition {
            JValueGen::Object(obj) => obj,
            _ => unreachable!()
        };
        let partition: String = env.get_string(&JString::from(partition)).unwrap().into();

        let offset = env.call_method(&mut java_element, "getOffset", "()Ljava/lang/String;", &[]).unwrap();
        let offset = match offset {
            JValueGen::Object(obj) => obj,
            _ => unreachable!()
        };
        let offset: String = env.get_string(&JString::from(offset)).unwrap().into();

        println!("source_id = {:?}, payload = {:?}, partition = {:?}, offset = {:?}", source_id, payload, partition, offset);
        events.push(CdcMessage {
            payload,
            partition,
            offset,
        })
    }
    let get_event_stream_response = GetEventStreamResponse {
        source_id: source_id as u64,
        events,
    };
    println!("before send");
    let _ = channel.as_ref().blocking_send(get_event_stream_response).inspect_err(|e| eprintln!("{:?}", e)).unwrap();
    println!("send successfully");
}



fn run_jvm() {
    let mut env = JVM.attach_current_thread_as_daemon().unwrap();
    let string_class = env.find_class("java/lang/String").unwrap();
    let jarray = env.new_object_array(0, string_class, JObject::null()).unwrap();

    let fn_ptr = Java_com_risingwave_java_binding_Binding_sendMsgToChannel as *mut c_void;

    let binding_class = env.find_class("com/risingwave/java/binding/Binding").unwrap();
    env.register_native_methods(binding_class, &[NativeMethod {
        name: JNIString::from("sendMsgToChannel"),
        sig: JNIString::from("(JLjava/lang/Object;)V"),
        fn_ptr,
    }]).unwrap();

    let _ = env.call_static_method("com/risingwave/connector/ConnectorService", "main", "([Ljava/lang/String;)V", &[JValue::Object(&jarray)]).inspect_err(|e| eprintln!("{:?}", e));
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
