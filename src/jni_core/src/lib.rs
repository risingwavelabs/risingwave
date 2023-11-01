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

#![feature(error_generic_member_access)]
#![feature(lazy_cell)]
#![feature(once_cell_try)]
#![feature(type_alias_impl_trait)]
#![feature(result_option_inspect)]
#![feature(try_blocks)]

pub mod hummock_iterator;
pub mod jvm_runtime;
mod macros;
pub mod stream_chunk_iterator;

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::slice::from_raw_parts;
use std::sync::{LazyLock, OnceLock};

use anyhow::anyhow;
use bytes::Bytes;
use cfg_or_panic::cfg_or_panic;
use jni::objects::{
    AutoElements, GlobalRef, JByteArray, JClass, JMethodID, JObject, JStaticMethodID, JString,
    JValue, JValueGen, JValueOwned, ReleaseMode,
};
use jni::signature::ReturnType;
use jni::sys::{
    jboolean, jbyte, jdouble, jfloat, jint, jlong, jshort, jsize, jvalue, JNI_FALSE, JNI_TRUE,
};
use jni::JNIEnv;
use prost::{DecodeError, Message};
use risingwave_common::array::{ArrayError, StreamChunk};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::test_prelude::StreamChunkTestExt;
use risingwave_common::types::ScalarRefImpl;
use risingwave_common::util::panic::rw_catch_unwind;
use risingwave_pb::connector_service::{
    GetEventStreamResponse, SinkCoordinatorStreamRequest, SinkCoordinatorStreamResponse,
    SinkWriterStreamRequest, SinkWriterStreamResponse,
};
use risingwave_pb::data::Op;
use risingwave_storage::error::StorageError;
use thiserror::Error;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::hummock_iterator::HummockJavaBindingIterator;
pub use crate::jvm_runtime::register_native_method_for_jvm;
use crate::stream_chunk_iterator::{into_iter, StreamChunkRowIterator};

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| tokio::runtime::Runtime::new().unwrap());

#[derive(Error, Debug)]
pub enum BindingError {
    #[error("JniError {error}")]
    Jni {
        #[from]
        error: jni::errors::Error,
        backtrace: Backtrace,
    },

    #[error("StorageError {error}")]
    Storage {
        #[from]
        error: StorageError,
        backtrace: Backtrace,
    },

    #[error("DecodeError {error}")]
    Decode {
        #[from]
        error: DecodeError,
        backtrace: Backtrace,
    },

    #[error("StreamChunkArrayError {error}")]
    StreamChunkArray {
        #[from]
        error: ArrayError,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, BindingError>;

pub fn to_guarded_slice<'array, 'env>(
    array: &'array JByteArray<'env>,
    env: &'array mut JNIEnv<'env>,
) -> Result<SliceGuard<'env, 'array>> {
    unsafe {
        let array = env.get_array_elements(array, ReleaseMode::NoCopyBack)?;
        let slice = from_raw_parts(array.as_ptr() as *mut u8, array.len());

        Ok(SliceGuard {
            _array: array,
            slice,
        })
    }
}

/// Wrapper around `&[u8]` derived from `jbyteArray` to prevent it from being auto-released.
pub struct SliceGuard<'env, 'array> {
    _array: AutoElements<'env, 'env, 'array, jbyte>,
    slice: &'array [u8],
}

impl<'env, 'array> Deref for SliceGuard<'env, 'array> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
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

/// In most Jni interfaces, the first parameter is `JNIEnv`, and the second parameter is `JClass`.
/// This struct simply encapsulates the two common parameters into a single struct for simplicity.
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

fn execute_and_catch<'env, F, Ret>(mut env: EnvParam<'env>, inner: F) -> Ret
where
    F: FnOnce(&mut EnvParam<'env>) -> Result<Ret>,
    Ret: Default + 'env,
{
    match rw_catch_unwind(std::panic::AssertUnwindSafe(|| inner(&mut env))) {
        Ok(Ok(ret)) => ret,
        Ok(Err(e)) => {
            match e {
                BindingError::Jni {
                    error: jni::errors::Error::JavaException,
                    backtrace,
                } => {
                    tracing::error!("get JavaException thrown from: {:?}", backtrace);
                    // the exception is already thrown. No need to throw again
                }
                _ => {
                    env.throw(format!("get error while processing: {:?}", e))
                        .expect("should be able to throw");
                }
            }
            Ret::default()
        }
        Err(e) => {
            env.throw(format!("panic while processing: {:?}", e))
                .expect("should be able to throw");
            Ret::default()
        }
    }
}

#[derive(Default)]
struct JavaClassMethodCache {
    big_decimal_ctor: OnceLock<(GlobalRef, JMethodID)>,
    timestamp_ctor: OnceLock<(GlobalRef, JMethodID)>,

    date_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
    time_ctor: OnceLock<(GlobalRef, JStaticMethodID)>,
}

enum JavaBindingIteratorInner {
    Hummock(HummockJavaBindingIterator),
    StreamChunk(StreamChunkRowIterator),
}

enum RowExtra {
    Op(Op),
    Key(Bytes),
}

impl RowExtra {
    fn as_op(&self) -> Op {
        match self {
            RowExtra::Op(op) => *op,
            RowExtra::Key(_) => unreachable!("should be op"),
        }
    }

    fn as_key(&self) -> &Bytes {
        match self {
            RowExtra::Key(key) => key,
            RowExtra::Op(_) => unreachable!("should be key"),
        }
    }
}

struct RowCursor {
    row: OwnedRow,
    extra: RowExtra,
}

struct JavaBindingIterator {
    inner: JavaBindingIteratorInner,
    cursor: Option<RowCursor>,
    class_cache: JavaClassMethodCache,
}

impl Deref for JavaBindingIterator {
    type Target = OwnedRow;

    fn deref(&self) -> &Self::Target {
        &self
            .cursor
            .as_ref()
            .expect("should exist when call row methods")
            .row
    }
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_vnodeCount(_env: EnvParam<'_>) -> jint {
    VirtualNode::COUNT as jint
}

#[cfg_or_panic(not(madsim))]
#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNewHummock<'a>(
    env: EnvParam<'a>,
    read_plan: JByteArray<'a>,
) -> Pointer<'static, JavaBindingIterator> {
    execute_and_catch(env, move |env| {
        let read_plan = Message::decode(to_guarded_slice(&read_plan, env)?.deref())?;
        let iter = RUNTIME.block_on(HummockJavaBindingIterator::new(read_plan))?;
        let iter = JavaBindingIterator {
            inner: JavaBindingIteratorInner::Hummock(iter),
            cursor: None,
            class_cache: Default::default(),
        };
        Ok(iter.into())
    })
}

#[cfg_or_panic(not(madsim))]
#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNext<'a>(
    env: EnvParam<'a>,
    mut pointer: Pointer<'a, JavaBindingIterator>,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        let iter = pointer.as_mut();
        match &mut iter.inner {
            JavaBindingIteratorInner::Hummock(ref mut hummock_iter) => {
                match RUNTIME.block_on(hummock_iter.next())? {
                    None => {
                        iter.cursor = None;
                        Ok(JNI_FALSE)
                    }
                    Some((key, row)) => {
                        iter.cursor = Some(RowCursor {
                            row,
                            extra: RowExtra::Key(key),
                        });
                        Ok(JNI_TRUE)
                    }
                }
            }
            JavaBindingIteratorInner::StreamChunk(ref mut stream_chunk_iter) => {
                match stream_chunk_iter.next() {
                    None => {
                        iter.cursor = None;
                        Ok(JNI_FALSE)
                    }
                    Some((op, row)) => {
                        iter.cursor = Some(RowCursor {
                            row,
                            extra: RowExtra::Op(op),
                        });
                        Ok(JNI_TRUE)
                    }
                }
            }
        }
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorClose<'a>(
    _env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
) {
    pointer.drop()
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNewFromStreamChunkPayload<
    'a,
>(
    env: EnvParam<'a>,
    stream_chunk_payload: JByteArray<'a>,
) -> Pointer<'static, JavaBindingIterator> {
    execute_and_catch(env, move |env| {
        let prost_stream_chumk =
            Message::decode(to_guarded_slice(&stream_chunk_payload, env)?.deref())?;
        let iter = into_iter(StreamChunk::from_protobuf(&prost_stream_chumk)?);
        let iter = JavaBindingIterator {
            inner: JavaBindingIteratorInner::StreamChunk(iter),
            cursor: None,
            class_cache: Default::default(),
        };
        Ok(iter.into())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorNewFromStreamChunkPretty<'a>(
    env: EnvParam<'a>,
    str: JString<'a>,
) -> Pointer<'static, JavaBindingIterator> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let iter = into_iter(StreamChunk::from_pretty(
            env.get_string(&str)
                .expect("cannot get java string")
                .to_str()
                .unwrap(),
        ));
        let iter = JavaBindingIterator {
            inner: JavaBindingIteratorInner::StreamChunk(iter),
            cursor: None,
            class_cache: Default::default(),
        };
        Ok(iter.into())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetKey<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        Ok(env.byte_array_from_slice(
            pointer
                .as_ref()
                .cursor
                .as_ref()
                .expect("should exists when call get key")
                .extra
                .as_key()
                .as_ref(),
        )?)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetOp<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
) -> jint {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .cursor
            .as_ref()
            .expect("should exist when call get op")
            .extra
            .as_op() as jint)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorIsNull<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        Ok(pointer.as_ref().datum_at(idx as usize).is_none() as jboolean)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt16Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jshort {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int16())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt32Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jint {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int32())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetInt64Value<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jlong {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_int64())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetFloatValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jfloat {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float32()
            .into())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDoubleValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jdouble {
    execute_and_catch(env, move |_env| {
        Ok(pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_float64()
            .into())
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetBooleanValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        Ok(pointer.as_ref().datum_at(idx as usize).unwrap().into_bool() as jboolean)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetStringValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'a>| {
        Ok(env.new_string(pointer.as_ref().datum_at(idx as usize).unwrap().into_utf8())?)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetIntervalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'a>| {
        let interval = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_interval()
            .as_iso_8601();
        Ok(env.new_string(interval)?)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetJsonbValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let jsonb = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_jsonb()
            .to_string();
        Ok(env.new_string(jsonb)?)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetTimestampValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let scalar_value = pointer.as_ref().datum_at(idx as usize).unwrap();
        let millis = match scalar_value {
            // supports sinking rw timestamptz to mysql timestamp
            ScalarRefImpl::Timestamptz(tz) => tz.timestamp_millis(),
            ScalarRefImpl::Timestamp(ts) => ts.0.timestamp_millis(),
            _ => panic!("expect timestamp or timestamptz"),
        };
        let (ts_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .timestamp_ctor
            .get_or_try_init(|| {
                let cls = env.find_class("java/sql/Timestamp")?;
                let init_method = env.get_method_id(&cls, "<init>", "(J)V")?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let ts_class = <&JClass<'_>>::from(ts_class_ref.as_obj());
            let date_obj =
                env.new_object_unchecked(ts_class, *constructor, &[jvalue { j: millis }])?;
            Ok(date_obj)
        }
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDecimalValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_decimal()
            .to_string();
        let string_value = env.new_string(value)?;
        let (decimal_class_ref, constructor) = pointer
            .as_ref()
            .class_cache
            .big_decimal_ctor
            .get_or_try_init(|| {
                let cls = env.find_class("java/math/BigDecimal")?;
                let init_method = env.get_method_id(&cls, "<init>", "(Ljava/lang/String;)V")?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let decimal_class = <&JClass<'_>>::from(decimal_class_ref.as_obj());
            let date_obj = env.new_object_unchecked(
                decimal_class,
                *constructor,
                &[jvalue {
                    l: string_value.into_raw(),
                }],
            )?;
            Ok(date_obj)
        }
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetDateValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_date()
            .0
            .to_string();

        let string_value = env.new_string(value)?;
        let (class_ref, constructor) =
            pointer.as_ref().class_cache.date_ctor.get_or_try_init(|| {
                let cls = env.find_class("java/sql/Date")?;
                let init_method = env.get_static_method_id(
                    &cls,
                    "valueOf",
                    "(Ljava/lang/String;)Ljava/sql/Date;",
                )?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let JValueOwned::Object(date_obj) = env.call_static_method_unchecked(
                <&JClass<'_>>::from(class_ref.as_obj()),
                *constructor,
                ReturnType::Object,
                &[jvalue {
                    l: string_value.into_raw(),
                }],
            )?
            else {
                return Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "valueOf".to_string(),
                    sig: "(Ljava/lang/String;)Ljava/sql/Date;".into(),
                }));
            };
            Ok(date_obj)
        }
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetTimeValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let value = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_time()
            .0
            .to_string();

        let string_value = env.new_string(value)?;
        let (class_ref, constructor) =
            pointer.as_ref().class_cache.time_ctor.get_or_try_init(|| {
                let cls = env.find_class("java/sql/Time")?;
                let init_method = env.get_static_method_id(
                    &cls,
                    "valueOf",
                    "(Ljava/lang/String;)Ljava/sql/Time;",
                )?;
                Ok::<_, jni::errors::Error>((env.new_global_ref(cls)?, init_method))
            })?;
        unsafe {
            let class = <&JClass<'_>>::from(class_ref.as_obj());
            match env.call_static_method_unchecked(
                class,
                *constructor,
                ReturnType::Object,
                &[jvalue {
                    l: string_value.into_raw(),
                }],
            )? {
                JValueGen::Object(obj) => Ok(obj),
                _ => Err(BindingError::from(jni::errors::Error::MethodNotFound {
                    name: "valueOf".to_string(),
                    sig: "(Ljava/lang/String;)Ljava/sql/Time;".into(),
                })),
            }
        }
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetByteaValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let bytes = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_bytea();
        Ok(env.byte_array_from_slice(bytes)?)
    })
}

#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_iteratorGetArrayValue<'a>(
    env: EnvParam<'a>,
    pointer: Pointer<'a, JavaBindingIterator>,
    idx: jint,
    class: JClass<'a>,
) -> JObject<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let elems = pointer
            .as_ref()
            .datum_at(idx as usize)
            .unwrap()
            .into_list()
            .iter();

        // convert the Rust elements to a Java object array (Object[])
        let jarray = env.new_object_array(elems.len() as jsize, &class, JObject::null())?;

        for (i, ele) in elems.enumerate() {
            let index = i as jsize;
            match ele {
                None => env.set_object_array_element(&jarray, i as jsize, JObject::null())?,
                Some(val) => match val {
                    ScalarRefImpl::Int16(v) => {
                        let obj = env.call_static_method(
                            &class,
                            "valueOf",
                            "(S)Ljava/lang/Short;",
                            &[JValue::from(v as jshort)],
                        )?;
                        if let JValueOwned::Object(o) = obj {
                            env.set_object_array_element(&jarray, index, &o)?
                        }
                    }
                    ScalarRefImpl::Int32(v) => {
                        let obj = env.call_static_method(
                            &class,
                            "valueOf",
                            "(I)Ljava/lang/Integer;",
                            &[JValue::from(v as jint)],
                        )?;
                        if let JValueOwned::Object(o) = obj {
                            env.set_object_array_element(&jarray, index, &o)?
                        }
                    }
                    ScalarRefImpl::Int64(v) => {
                        let obj = env.call_static_method(
                            &class,
                            "valueOf",
                            "(J)Ljava/lang/Long;",
                            &[JValue::from(v as jlong)],
                        )?;
                        if let JValueOwned::Object(o) = obj {
                            env.set_object_array_element(&jarray, index, &o)?
                        }
                    }
                    ScalarRefImpl::Float32(v) => {
                        let obj = env.call_static_method(
                            &class,
                            "valueOf",
                            "(F)Ljava/lang/Float;",
                            &[JValue::from(v.into_inner() as jfloat)],
                        )?;
                        if let JValueOwned::Object(o) = obj {
                            env.set_object_array_element(&jarray, index, &o)?
                        }
                    }
                    ScalarRefImpl::Float64(v) => {
                        let obj = env.call_static_method(
                            &class,
                            "valueOf",
                            "(D)Ljava/lang/Double;",
                            &[JValue::from(v.into_inner() as jdouble)],
                        )?;
                        if let JValueOwned::Object(o) = obj {
                            env.set_object_array_element(&jarray, index, &o)?
                        }
                    }
                    ScalarRefImpl::Utf8(v) => {
                        let obj = env.new_string(v)?;
                        env.set_object_array_element(&jarray, index, obj)?
                    }
                    _ => env.set_object_array_element(&jarray, index, JObject::null())?,
                },
            }
        }
        let output = unsafe { JObject::from_raw(jarray.into_raw()) };
        Ok(output)
    })
}

pub type JniSenderType<T> = Sender<anyhow::Result<T>>;
pub type JniReceiverType<T> = Receiver<T>;

/// Send messages to the channel received by `CdcSplitReader`.
/// If msg is null, just check whether the channel is closed.
/// Return true if sending is successful, otherwise, return false so that caller can stop
/// gracefully.
#[no_mangle]
extern "system" fn Java_com_risingwave_java_binding_Binding_sendCdcSourceMsgToChannel<'a>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<GetEventStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        // If msg is null means just check whether channel is closed.
        if msg.is_null() {
            if channel.as_ref().is_closed() {
                return Ok(JNI_FALSE);
            } else {
                return Ok(JNI_TRUE);
            }
        }

        let get_event_stream_response: GetEventStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(get_event_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!("send error.  {:?}", e);
                Ok(JNI_FALSE)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_recvSinkWriterRequestFromChannel<
    'a,
>(
    env: EnvParam<'a>,
    mut channel: Pointer<'a, JniReceiverType<SinkWriterStreamRequest>>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env| match channel.as_mut().blocking_recv() {
        Some(msg) => {
            let bytes = env
                .byte_array_from_slice(&Message::encode_to_vec(&msg))
                .unwrap();
            Ok(bytes)
        }
        None => Ok(JObject::null().into()),
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkWriterResponseToChannel<
    'a,
>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<SinkWriterStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let sink_writer_stream_response: SinkWriterStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(sink_writer_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!("send error.  {:?}", e);
                Ok(JNI_FALSE)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkWriterErrorToChannel<'a>(
    env: EnvParam<'a>,
    channel: Pointer<'a, Sender<anyhow::Result<SinkWriterStreamResponse>>>,
    msg: JString<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let err_msg: String = env
            .get_string(&msg)
            .expect("sink error message should be a java string")
            .into();

        match channel.as_ref().blocking_send(Err(anyhow!(err_msg))) {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!("send error.  {:?}", e);
                Ok(JNI_FALSE)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_recvSinkCoordinatorRequestFromChannel<
    'a,
>(
    env: EnvParam<'a>,
    mut channel: Pointer<'a, JniReceiverType<SinkCoordinatorStreamRequest>>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env| match channel.as_mut().blocking_recv() {
        Some(msg) => {
            let bytes = env
                .byte_array_from_slice(&Message::encode_to_vec(&msg))
                .unwrap();
            Ok(bytes)
        }
        None => Ok(JObject::null().into()),
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_sendSinkCoordinatorResponseToChannel<
    'a,
>(
    env: EnvParam<'a>,
    channel: Pointer<'a, JniSenderType<SinkCoordinatorStreamResponse>>,
    msg: JByteArray<'a>,
) -> jboolean {
    execute_and_catch(env, move |env| {
        let sink_coordinator_stream_response: SinkCoordinatorStreamResponse =
            Message::decode(to_guarded_slice(&msg, env)?.deref())?;

        match channel
            .as_ref()
            .blocking_send(Ok(sink_coordinator_stream_response))
        {
            Ok(_) => Ok(JNI_TRUE),
            Err(e) => {
                tracing::info!("send error.  {:?}", e);
                Ok(JNI_FALSE)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::Timestamptz;

    /// make sure that the [`ScalarRefImpl::Int64`] received by
    /// [`Java_com_risingwave_java_binding_Binding_iteratorGetTimestampValue`]
    /// is of type [`DataType::Timestamptz`] stored in microseconds
    #[test]
    fn test_timestamptz_to_i64() {
        assert_eq!(
            "2023-06-01 09:45:00+08:00".parse::<Timestamptz>().unwrap(),
            Timestamptz::from_micros(1_685_583_900_000_000)
        );
    }
}
