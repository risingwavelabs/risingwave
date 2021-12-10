use crate::array::{
    Array, ArrayImpl, BoolArray, DataChunk, F32Array, F64Array, I16Array, I32Array, I64Array,
    Utf8Array,
};
use crate::error::{ErrorCode, Result};
use crate::types::{DataTypeKind, DataTypeRef};
use crate::util::sort_util::{OrderPair, OrderType};
use itertools::Itertools;
use std::sync::Arc;

struct EncodedColumn {
    pub buf: Vec<u8>,
    pub width: usize,
}

enum EncodableArray<'a> {
    Bool(&'a BoolArray),
    Int16(&'a I16Array),
    Int32(&'a I32Array),
    Int64(&'a I64Array),
    Float32(&'a F32Array),
    Float64(&'a F64Array),
    Utf8(&'a Utf8Array),
}

impl EncodableArray<'_> {
    pub fn width(&self) -> usize {
        match self {
            Self::Bool(_) => 1usize,
            Self::Int16(_) => 2usize,
            Self::Int32(_) => 4usize,
            Self::Int64(_) => 8usize,
            Self::Float32(_) => 4usize,
            Self::Float64(_) => 8usize,
            Self::Utf8(array) => {
                // Find the max length of utf string
                array.iter().fold(0, |m, s| {
                    m.max(match s {
                        Some(s) => s.len(),
                        None => 0usize,
                    })
                })
            }
        }
    }
    /// The order preserved encoding for each array is implemented here.
    ///
    /// ## Bool / String
    ///
    /// The original binary representation is order preserved. Note that we need
    /// to add padding for string to make them in same length.
    ///
    /// ## Int16 / Int32 / Int64:
    ///
    /// The original binary representation (big-endian) between two positive integer
    /// and negative integer is order preserved, we just need to flip the sign bit
    /// to make the negative integer smaller than the positive.
    ///
    /// ## Float32 / Float64:
    ///
    /// For the positive float, the original binary representation (big-endian) is
    /// order preserved since the exp part is at higher position and order preserved.
    /// For the negative, we need to flip the fraction part. And same as the integer,
    /// we need to swap the sign bit between negative and positive float.
    pub fn encoded_value_at(&self, idx: usize, width: usize) -> Vec<u8> {
        match self {
            Self::Bool(bool_array) => {
                if let Some(value) = bool_array.value_at(idx) {
                    vec![value.into()]
                } else {
                    // TODO: handle NULL
                    vec![0u8; 1]
                }
            }
            Self::Int16(int_array) => {
                if let Some(value) = int_array.value_at(idx) {
                    (value ^ (1 << 15)).to_be_bytes().to_vec()
                } else {
                    // TODO: handle NULL
                    vec![0u8; 2]
                }
            }
            Self::Int32(int_array) => {
                if let Some(value) = int_array.value_at(idx) {
                    (value ^ (1 << 31)).to_be_bytes().to_vec()
                } else {
                    // TODO: handle NULL
                    vec![0u8; 4]
                }
            }
            Self::Int64(int_array) => {
                if let Some(value) = int_array.value_at(idx) {
                    (value ^ (1 << 63)).to_be_bytes().to_vec()
                } else {
                    // TODO: handle NULL
                    vec![0u8; 8]
                }
            }
            Self::Float32(float_array) => {
                if let Some(value) = float_array.value_at(idx) {
                    let mut buf = value.to_be_bytes();
                    if (buf[0] & 128) == 0 {
                        buf[0] |= 128;
                    } else {
                        for b in &mut buf {
                            *b = !*b;
                        }
                    }
                    buf.to_vec()
                } else {
                    // TODO: handle NULL
                    vec![0u8; 4]
                }
            }
            Self::Float64(float_array) => {
                if let Some(value) = float_array.value_at(idx) {
                    let mut buf = value.to_be_bytes();
                    if (buf[0] & 128) == 0 {
                        buf[0] |= 128;
                    } else {
                        for b in &mut buf {
                            *b = !*b;
                        }
                    }
                    buf.to_vec()
                } else {
                    // TODO: handle NULL
                    vec![0u8; 8]
                }
            }
            Self::Utf8(utf8_array) => {
                if let Some(value) = utf8_array.value_at(idx) {
                    let mut buf = value.as_bytes().to_vec();
                    // padding
                    if buf.len() < width {
                        buf.extend(vec![0u8; width - buf.len()]);
                    }
                    buf
                } else {
                    // TODO: handle NULL
                    vec![0u8; width]
                }
            }
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(bool_array) => bool_array.len(),
            Self::Int16(int_array) => int_array.len(),
            Self::Int32(int_array) => int_array.len(),
            Self::Int64(int_array) => int_array.len(),
            Self::Float32(float_array) => float_array.len(),
            Self::Float64(float_array) => float_array.len(),
            Self::Utf8(utf8_array) => utf8_array.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encode_array(&self, order: &OrderType) -> EncodedColumn {
        let len = self.len();
        let width = self.width();
        let mut buf = vec![];
        for idx in 0..len {
            let sub = self.encoded_value_at(idx, width);
            let sub = match order {
                OrderType::Ascending => sub,
                OrderType::Descending => sub.iter().map(|b| !b).collect_vec(),
            };
            buf.extend_from_slice(sub.as_slice());
        }
        EncodedColumn { width, buf }
    }
}

/// This function is used to check whether we can perform encoding on this type.
pub fn is_type_encodable(t: DataTypeRef) -> bool {
    matches!(
        t.data_type_kind(),
        DataTypeKind::Boolean
            | DataTypeKind::Int16
            | DataTypeKind::Int32
            | DataTypeKind::Int64
            | DataTypeKind::Float32
            | DataTypeKind::Float64
            | DataTypeKind::Varchar
            | DataTypeKind::Char
    )
}

fn encode_array(array: &ArrayImpl, order: &OrderType) -> Result<EncodedColumn> {
    match array {
        ArrayImpl::Bool(array) => Ok(EncodableArray::Bool(array).encode_array(order)),
        ArrayImpl::Int16(array) => Ok(EncodableArray::Int16(array).encode_array(order)),
        ArrayImpl::Int32(array) => Ok(EncodableArray::Int32(array).encode_array(order)),
        ArrayImpl::Int64(array) => Ok(EncodableArray::Int64(array).encode_array(order)),
        ArrayImpl::Float32(array) => Ok(EncodableArray::Float32(array).encode_array(order)),
        ArrayImpl::Float64(array) => Ok(EncodableArray::Float64(array).encode_array(order)),
        ArrayImpl::Utf8(array) => Ok(EncodableArray::Utf8(array).encode_array(order)),
        _ => Err(ErrorCode::NotImplementedError(
            "Binary Compression is not implemented for this type".to_string(),
        )
        .into()),
    }
}

/// This function is used to accelerate the comparison of tuples. It takes datachunk and
/// user-defined order as input, yield encoded binary string with order preserved for each tuple in
/// the datachunk. We only support encoding for Bool, Int16, Int32, Int64, Float32, Float64, Utf8 by
/// now.
pub fn encode_chunk(chunk: &DataChunk, order_pairs: Arc<Vec<OrderPair>>) -> Arc<Vec<Vec<u8>>> {
    let encoded_columns = order_pairs
        .iter()
        .map(|o| encode_array(o.order.eval_immut(chunk).unwrap().as_ref(), &o.order_type).unwrap())
        .collect_vec();
    let mut encoded_chunk = vec![vec![]; chunk.capacity()];
    for encoded_column in encoded_columns {
        for (i, encoded_row) in encoded_chunk.iter_mut().enumerate() {
            let width = encoded_column.width;
            let offset = width * i;
            encoded_row.extend_from_slice(&encoded_column.buf[offset..offset + width]);
        }
    }
    Arc::new(encoded_chunk)
}
