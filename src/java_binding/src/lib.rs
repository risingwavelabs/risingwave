#![feature(error_generic_member_access)]
#![feature(provide_any)]

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::panic::{catch_unwind, UnwindSafe};

use jni::objects::{JClass, JObject};
use jni::sys::{jbyteArray, jlong};
use jni::JNIEnv;
use thiserror::Error;

#[derive(Error, Debug)]
enum BindingError {
    #[error("JniError {error}")]
    JniError {
        #[from]
        error: jni::errors::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, BindingError>;

#[repr(transparent)]
#[derive(Default)]
pub struct ByteArray<'a>(JObject<'a>);

impl<'a> From<jbyteArray> for ByteArray<'a> {
    fn from(inner: jbyteArray) -> Self {
        unsafe { Self(JObject::from_raw(inner)) }
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
            _phantom: PhantomData::default(),
        }
    }
}

impl<T> Pointer<'static, T> {
    fn null() -> Self {
        Pointer {
            pointer: 0,
            _phantom: PhantomData::default(),
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

fn execute_and_catch<F, Ret>(env: JNIEnv<'_>, inner: F) -> Ret
where
    F: FnOnce() -> Result<Ret> + UnwindSafe,
    Ret: Default,
{
    match catch_unwind(move || inner()) {
        Ok(Ok(ret)) => ret.into(),
        Ok(Err(e)) => {
            match e {
                BindingError::JniError {
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

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_iteratorNew(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
) -> Pointer<'static, Iterator> {
    Iterator { cur_idx: 0 }.into()
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_iteratorNext<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    mut pointer: Pointer<'a, Iterator>,
) -> Pointer<'static, Record> {
    execute_and_catch(env, move || match pointer.as_mut().next() {
        None => Ok(Pointer::null()),
        Some(record) => Ok(record.into()),
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_iteratorClose(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    pointer: Pointer<'_, Iterator>,
) {
    pointer.drop();
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_recordGetKey<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    pointer: Pointer<'a, Record>,
) -> ByteArray<'a> {
    execute_and_catch(env.clone(), move || {
        Ok(ByteArray::from(
            env.byte_array_from_slice(pointer.as_ref().key)?,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_recordGetValue<'a>(
    env: JNIEnv<'a>,
    _class: JClass<'a>,
    pointer: Pointer<'a, Record>,
) -> ByteArray<'a> {
    execute_and_catch(env.clone(), move || {
        Ok(ByteArray::from(
            env.byte_array_from_slice(pointer.as_ref().value)?,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_recordClose<'a>(
    _env: JNIEnv<'a>,
    _class: JClass<'a>,
    pointer: Pointer<'a, Record>,
) {
    pointer.drop()
}

pub struct Iterator {
    cur_idx: usize,
}

pub struct Record {
    key: &'static [u8],
    value: &'static [u8],
}

impl Iterator {
    fn next(&mut self) -> Option<Record> {
        if self.cur_idx == 0 {
            self.cur_idx += 1;
            Some(Record {
                key: &b"first"[..],
                value: &b"value1"[..],
            })
        } else if self.cur_idx == 1 {
            self.cur_idx += 1;
            Some(Record {
                key: &b"second"[..],
                value: &b"value2"[..],
            })
        } else {
            None
        }
    }
}
