#![feature(error_generic_member_access)]
#![feature(provide_any)]

use std::backtrace::Backtrace;
use std::marker::PhantomData;
use std::panic::{catch_unwind, UnwindSafe};

use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jlong, jobject};
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
pub struct Pointer<'a, T> {
    pointer: jlong,
    _phantom: PhantomData<&'a T>,
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
                    ..
                } => {
                    // TODO: add log here
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
) -> NextResult<'a> {
    execute_and_catch(env.clone(), move || pointer.as_mut().next(env))
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_binding_Binding_iteratorClose(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    pointer: Pointer<'_, Iterator>,
) {
    pointer.drop();
}

pub struct Iterator {
    cur_idx: usize,
}

impl Iterator {
    fn next<'a>(&'a mut self, env: JNIEnv<'a>) -> Result<NextResult<'a>> {
        if self.cur_idx == 0 {
            self.cur_idx += 1;
            NextResult::from_kv(env, &b"first"[..], &b"value1"[..])
        } else if self.cur_idx == 1 {
            self.cur_idx += 1;
            NextResult::from_kv(env, &b"second"[..], &b"value2"[..])
        } else {
            NextResult::none(env)
        }
    }
}

const NEXT_RESULT_CLASSNAME: &'static str = "com/risingwave/binding/NextResult";

#[derive(Default)]
#[repr(transparent)]
pub struct NextResult<'a>(JObject<'a>);

impl<'a> NextResult<'a> {
    fn from_kv(env: JNIEnv<'a>, key: &'a [u8], value: &'a [u8]) -> Result<Self> {
        let key = unsafe { JValue::Object(JObject::from_raw(env.byte_array_from_slice(key)?)) };
        let value = unsafe { JValue::Object(JObject::from_raw(env.byte_array_from_slice(value)?)) };
        let class = env.find_class(NEXT_RESULT_CLASSNAME)?;
        Ok(Self(env.new_object(class, "([B[B)V", &[key, value])?))
    }

    fn none(env: JNIEnv<'a>) -> Result<Self> {
        let class = env.find_class(NEXT_RESULT_CLASSNAME)?;
        Ok(Self(env.new_object(class, "()V", &[])?))
    }
}

impl<'a> Into<jobject> for NextResult<'a> {
    fn into(self) -> jobject {
        self.0.into_raw()
    }
}
