use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Write;

use super::{OrderedF32, OrderedF64};
use crate::error::ErrorCode::IoError;
use crate::error::{Result, RwError};

pub trait NativeType:
    PartialOrd + PartialEq + Debug + Copy + Send + Sync + Sized + Default + 'static
{
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize>;
    fn hash_wrapper<H: Hasher>(&self, state: &mut H);
}

impl NativeType for i16 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for i32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for i64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for OrderedF32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        state.write_i32(self.0 as i32);
    }
}

impl NativeType for OrderedF64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        state.write_i64(self.0 as i64);
    }
}

impl NativeType for u8 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for u16 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for u32 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}

impl NativeType for u64 {
    fn to_protobuf<T: Write>(self, output: &mut T) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }

    fn hash_wrapper<H: Hasher>(&self, state: &mut H) {
        self.hash(state);
    }
}
