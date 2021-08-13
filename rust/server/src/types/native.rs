use crate::error::ErrorCode::{InternalError, IoError};
use crate::error::Result;
use crate::error::RwError;
use crate::expr::Datum;
use std::io::Write;

pub(crate) trait NativeType: Copy + Send + Sync + Sized + 'static {
    fn from_datum(datum: &Datum) -> Result<Self>;
    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize>;
}

impl NativeType for i16 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int16(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i16: {:?}", datum)).into()),
        }
    }

    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }
}
impl NativeType for i32 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int32(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i32: {:?}", datum)).into()),
        }
    }

    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }
}
impl NativeType for i64 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int64(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i64: {:?}", datum)).into()),
        }
    }

    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }
}
impl NativeType for f32 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Float32(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for f32: {:?}", datum)).into()),
        }
    }

    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }
}
impl NativeType for f64 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Float64(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for f64: {:?}", datum)).into()),
        }
    }

    fn to_protobuf(self, output: &mut dyn Write) -> Result<usize> {
        output
            .write(&self.to_be_bytes())
            .map_err(|e| RwError::from(IoError(e)))
    }
}
