use risingwave_pb::data::{Array as ProstArray, ArrayType as ProstArrayType};

use super::{ArrayBuilderImpl, ArrayImpl};
use crate::error::Result;

#[derive(Clone)]
pub enum ArrayMeta {
    Simple, // Simple array without given any extra metadata.
    Struct { children: Vec<ArrayType> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArrayType {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Bool,
    Decimal,
    Interval,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Struct { children: Vec<ArrayType> },
}

impl ArrayType {
    pub fn create_array_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        use crate::array::*;
        let builder = match self {
            ArrayType::Bool => BoolArrayBuilder::new(capacity)?.into(),
            ArrayType::Int16 => PrimitiveArrayBuilder::<i16>::new(capacity)?.into(),
            ArrayType::Int32 => PrimitiveArrayBuilder::<i32>::new(capacity)?.into(),
            ArrayType::Int64 => PrimitiveArrayBuilder::<i64>::new(capacity)?.into(),
            ArrayType::Float32 => PrimitiveArrayBuilder::<OrderedF32>::new(capacity)?.into(),
            ArrayType::Float64 => PrimitiveArrayBuilder::<OrderedF64>::new(capacity)?.into(),
            ArrayType::Decimal => DecimalArrayBuilder::new(capacity)?.into(),
            ArrayType::NaiveDate => NaiveDateArrayBuilder::new(capacity)?.into(),
            ArrayType::Utf8 => Utf8ArrayBuilder::new(capacity)?.into(),
            ArrayType::NaiveTime => NaiveTimeArrayBuilder::new(capacity)?.into(),
            ArrayType::NaiveDateTime => NaiveDateTimeArrayBuilder::new(capacity)?.into(),
            ArrayType::Interval => IntervalArrayBuilder::new(capacity)?.into(),
            ArrayType::Struct { children } => StructArrayBuilder::new_with_meta(
                capacity,
                ArrayMeta::Struct {
                    children: children.clone(),
                },
            )?
            .into(),
        };
        Ok(builder)
    }

    pub fn from_protobuf(array: &ProstArray) -> Result<ArrayType> {
        let t = match array.get_array_type()? {
            ProstArrayType::Int16 => ArrayType::Int16,
            ProstArrayType::Int32 => ArrayType::Int32,
            ProstArrayType::Int64 => ArrayType::Int64,
            ProstArrayType::Float32 => ArrayType::Float32,
            ProstArrayType::Float64 => ArrayType::Float64,
            ProstArrayType::Bool => ArrayType::Bool,
            ProstArrayType::Utf8 => ArrayType::Utf8,
            ProstArrayType::Decimal => ArrayType::Decimal,
            ProstArrayType::Date => ArrayType::NaiveDate,
            ProstArrayType::Time => ArrayType::NaiveTime,
            ProstArrayType::Timestamp => ArrayType::NaiveDateTime,
            ProstArrayType::Struct => ArrayType::Struct {
                children: array
                    .children_array
                    .iter()
                    .map(ArrayType::from_protobuf)
                    .collect::<Result<Vec<ArrayType>>>()?,
            },
        };
        Ok(t)
    }

    pub fn from_array_impl(a: &ArrayImpl) -> Result<ArrayType> {
        use crate::array::*;
        let t = match a {
            ArrayImpl::Bool(_) => ArrayType::Bool,
            ArrayImpl::Int16(_) => ArrayType::Int16,
            ArrayImpl::Int32(_) => ArrayType::Int32,
            ArrayImpl::Int64(_) => ArrayType::Int64,
            ArrayImpl::Float32(_) => ArrayType::Float32,
            ArrayImpl::Float64(_) => ArrayType::Float64,
            ArrayImpl::Decimal(_) => ArrayType::Decimal,
            ArrayImpl::NaiveDate(_) => ArrayType::NaiveDate,
            ArrayImpl::Utf8(_) => ArrayType::Utf8,
            ArrayImpl::NaiveTime(_) => ArrayType::NaiveTime,
            ArrayImpl::NaiveDateTime(_) => ArrayType::NaiveDateTime,
            ArrayImpl::Interval(_) => ArrayType::Interval,
            ArrayImpl::Struct(array) => ArrayType::Struct {
                children: array.children_array_types().to_vec(),
            },
        };
        Ok(t)
    }
}
