use crate::array::*;
use crate::types::DataTypeKind;

/// This trait combine the array with its data type. It helps generate the across type expression
pub trait DataTypeTrait {
    const DATA_TYPE_ENUM: DataTypeKind;
    type ArrayType: Array;
}

pub struct BoolType;
impl DataTypeTrait for BoolType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Boolean;
    type ArrayType = BoolArray;
}

pub struct Int16Type;
impl DataTypeTrait for Int16Type {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Int16;
    type ArrayType = I16Array;
}

pub struct Int32Type;
impl DataTypeTrait for Int32Type {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Int32;
    type ArrayType = I32Array;
}

pub struct Int64Type;
impl DataTypeTrait for Int64Type {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Int64;
    type ArrayType = I64Array;
}

pub struct Float32Type;
impl DataTypeTrait for Float32Type {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Float32;
    type ArrayType = F32Array;
}

pub struct Float64Type;
impl DataTypeTrait for Float64Type {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Float64;
    type ArrayType = F64Array;
}

pub struct DecimalType;
impl DataTypeTrait for DecimalType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Decimal;
    type ArrayType = DecimalArray;
}

pub struct TimestampType;
impl DataTypeTrait for TimestampType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Timestamp;
    type ArrayType = I64Array;
}

pub struct TimestampWithTimeZoneType;
impl DataTypeTrait for TimestampWithTimeZoneType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Timestampz;
    type ArrayType = I64Array;
}

pub struct DateType;
impl DataTypeTrait for DateType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Date;
    type ArrayType = I32Array;
}

pub struct IntervalType;
impl DataTypeTrait for IntervalType {
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Interval;
    type ArrayType = IntervalArray;
}

pub struct StringType;
impl DataTypeTrait for StringType {
    // TODO: How about char?
    const DATA_TYPE_ENUM: DataTypeKind = DataTypeKind::Varchar;
    type ArrayType = Utf8Array;
}
