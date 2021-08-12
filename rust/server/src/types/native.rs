use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::Datum;

pub(crate) trait NativeType: Copy + Send + Sync + Sized + 'static {
    fn from_datum(datum: &Datum) -> Result<Self>;
}

impl NativeType for i16 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int16(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i16: {:?}", datum)).into()),
        }
    }
}
impl NativeType for i32 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int32(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i32: {:?}", datum)).into()),
        }
    }
}
impl NativeType for i64 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Int64(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for i64: {:?}", datum)).into()),
        }
    }
}
impl NativeType for f32 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Float32(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for f32: {:?}", datum)).into()),
        }
    }
}
impl NativeType for f64 {
    fn from_datum(datum: &Datum) -> Result<Self> {
        match datum {
            Datum::Float64(v) => Ok(*v),
            _ => Err(InternalError(format!("Incorrect datum for f64: {:?}", datum)).into()),
        }
    }
}
