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

//! Converts between arrays and Apache Arrow arrays.
//!
//! This file acts as a template file for conversion code between
//! arrays and different version of Apache Arrow.
//!
//! The conversion logic will be implemented for the arrow version specified in the outer mod by
//! `super::arrow_xxx`, such as `super::arrow_array`.
//!
//! When we want to implement the conversion logic for an arrow version, we first
//! create a new mod file, and rename the corresponding arrow package name to `arrow_xxx`
//! using the `use` clause, and then declare a sub-mod and set its file path with attribute
//! `#[path = "./arrow_impl.rs"]` so that the code in this template file can be embedded to
//! the new mod file, and the conversion logic can be implemented for the corresponding arrow
//! version.
//!
//! Example can be seen in `arrow_default.rs`, which is also as followed:
//! ```ignore
//! use {arrow_array, arrow_buffer, arrow_cast, arrow_schema};
//!
//! #[allow(clippy::duplicate_mod)]
//! #[path = "./arrow_impl.rs"]
//! mod arrow_impl;
//! ```

// Is this a bug? Why do we have these lints?
#![allow(unused_imports)]
#![allow(dead_code)]

use std::fmt::Write;

use arrow_array::array;
use arrow_array::cast::AsArray;
use arrow_buffer::OffsetBuffer;
use arrow_schema::TimeUnit;
use chrono::{DateTime, NaiveDateTime, NaiveTime};
use itertools::Itertools;

use super::arrow_schema::IntervalUnit;
// This is important because we want to use the arrow version specified by the outer mod.
use super::{ArrowIntervalType, arrow_array, arrow_buffer, arrow_cast, arrow_schema};
// Other import should always use the absolute path.
use crate::array::*;
use crate::types::{DataType as RwDataType, Scalar, *};
use crate::util::iter_util::ZipEqFast;

/// Defines how to convert RisingWave arrays to Arrow arrays.
///
/// This trait allows for customized conversion logic for different external systems using Arrow.
/// The default implementation is based on the `From` implemented in this mod.
pub trait ToArrow {
    /// Converts RisingWave `DataChunk` to Arrow `RecordBatch` with specified schema.
    ///
    /// This function will try to convert the array if the type is not same with the schema.
    fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        // compact the chunk if it's not compacted
        if !chunk.is_vis_compacted() {
            let c = chunk.clone();
            return self.to_record_batch(schema, &c.compact_vis());
        }

        // convert each column to arrow array
        let columns: Vec<_> = chunk
            .columns()
            .iter()
            .zip_eq_fast(schema.fields().iter())
            .map(|(column, field)| self.to_array(field.data_type(), column))
            .try_collect()?;

        // create record batch
        let opts =
            arrow_array::RecordBatchOptions::default().with_row_count(Some(chunk.capacity()));
        arrow_array::RecordBatch::try_new_with_options(schema, columns, &opts)
            .map_err(ArrayError::to_arrow)
    }

    /// Converts RisingWave array to Arrow array.
    fn to_array(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ArrayImpl,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let arrow_array = match array {
            ArrayImpl::Bool(array) => self.bool_to_arrow(array),
            ArrayImpl::Int16(array) => self.int16_to_arrow(array),
            ArrayImpl::Int32(array) => self.int32_to_arrow(array),
            ArrayImpl::Int64(array) => self.int64_to_arrow(array),
            ArrayImpl::Int256(array) => self.int256_to_arrow(array),
            ArrayImpl::Float32(array) => self.float32_to_arrow(array),
            ArrayImpl::Float64(array) => self.float64_to_arrow(array),
            ArrayImpl::Date(array) => self.date_to_arrow(array),
            ArrayImpl::Time(array) => self.time_to_arrow(array),
            ArrayImpl::Timestamp(array) => self.timestamp_to_arrow(array),
            ArrayImpl::Timestamptz(array) => self.timestamptz_to_arrow(array),
            ArrayImpl::Interval(array) => self.interval_to_arrow(array),
            ArrayImpl::Utf8(array) => self.utf8_to_arrow(array),
            ArrayImpl::Bytea(array) => self.bytea_to_arrow(array),
            ArrayImpl::Decimal(array) => self.decimal_to_arrow(data_type, array),
            ArrayImpl::Jsonb(array) => self.jsonb_to_arrow(array),
            ArrayImpl::Serial(array) => self.serial_to_arrow(array),
            ArrayImpl::List(array) => self.list_to_arrow(data_type, array),
            ArrayImpl::Struct(array) => self.struct_to_arrow(data_type, array),
            ArrayImpl::Map(array) => self.map_to_arrow(data_type, array),
            ArrayImpl::Vector(inner) => self.vector_to_arrow(data_type, inner),
        }?;
        if arrow_array.data_type() != data_type {
            arrow_cast::cast(&arrow_array, data_type).map_err(ArrayError::to_arrow)
        } else {
            Ok(arrow_array)
        }
    }

    #[inline]
    fn bool_to_arrow(&self, array: &BoolArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::BooleanArray::from(array)))
    }

    #[inline]
    fn int16_to_arrow(&self, array: &I16Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Int16Array::from(array)))
    }

    #[inline]
    fn int32_to_arrow(&self, array: &I32Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Int32Array::from(array)))
    }

    #[inline]
    fn int64_to_arrow(&self, array: &I64Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Int64Array::from(array)))
    }

    #[inline]
    fn float32_to_arrow(&self, array: &F32Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Float32Array::from(array)))
    }

    #[inline]
    fn float64_to_arrow(&self, array: &F64Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Float64Array::from(array)))
    }

    #[inline]
    fn utf8_to_arrow(&self, array: &Utf8Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::StringArray::from(array)))
    }

    #[inline]
    fn int256_to_arrow(&self, array: &Int256Array) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Decimal256Array::from(array)))
    }

    #[inline]
    fn date_to_arrow(&self, array: &DateArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Date32Array::from(array)))
    }

    #[inline]
    fn timestamp_to_arrow(
        &self,
        array: &TimestampArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::TimestampMicrosecondArray::from(
            array,
        )))
    }

    #[inline]
    fn timestamptz_to_arrow(
        &self,
        array: &TimestamptzArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(
            arrow_array::TimestampMicrosecondArray::from(array).with_timezone_utc(),
        ))
    }

    #[inline]
    fn time_to_arrow(&self, array: &TimeArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Time64MicrosecondArray::from(array)))
    }

    #[inline]
    fn interval_to_arrow(
        &self,
        array: &IntervalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::IntervalMonthDayNanoArray::from(
            array,
        )))
    }

    #[inline]
    fn bytea_to_arrow(&self, array: &BytesArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::BinaryArray::from(array)))
    }

    // Decimal values are stored as ASCII text representation in a string array.
    #[inline]
    fn decimal_to_arrow(
        &self,
        _data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::StringArray::from(array)))
    }

    // JSON values are stored as text representation in a string array.
    #[inline]
    fn jsonb_to_arrow(&self, array: &JsonbArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::StringArray::from(array)))
    }

    #[inline]
    fn serial_to_arrow(&self, array: &SerialArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::Int64Array::from(array)))
    }

    #[inline]
    fn list_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ListArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let arrow_schema::DataType::List(field) = data_type else {
            return Err(ArrayError::to_arrow("Invalid list type"));
        };
        let values = self.to_array(field.data_type(), array.values())?;
        let offsets = OffsetBuffer::new(array.offsets().iter().map(|&o| o as i32).collect());
        let nulls = (!array.null_bitmap().all()).then(|| array.null_bitmap().into());
        Ok(Arc::new(arrow_array::ListArray::new(
            field.clone(),
            offsets,
            values,
            nulls,
        )))
    }

    #[inline]
    fn vector_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &VectorArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let arrow_schema::DataType::List(field) = data_type else {
            return Err(ArrayError::to_arrow("Invalid list type"));
        };
        if field.data_type() != &arrow_schema::DataType::Float32 {
            return Err(ArrayError::to_arrow("Invalid list inner type for vector"));
        }
        let values = Arc::new(arrow_array::Float32Array::from(
            array.as_raw_slice().to_vec(),
        ));
        let offsets = OffsetBuffer::new(array.offsets().iter().map(|&o| o as i32).collect());
        let nulls = (!array.null_bitmap().all()).then(|| array.null_bitmap().into());
        Ok(Arc::new(arrow_array::ListArray::new(
            field.clone(),
            offsets,
            values,
            nulls,
        )))
    }

    #[inline]
    fn struct_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &StructArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let arrow_schema::DataType::Struct(fields) = data_type else {
            return Err(ArrayError::to_arrow("Invalid struct type"));
        };
        // Use `try_new_with_length` so that empty-field structs keep their row count;
        // `StructArray::new` panics for empty `fields` because it derives length from
        // the child arrays.
        let len = array.len();
        let child_arrays = array
            .fields()
            .zip_eq_fast(fields)
            .map(|(arr, field)| self.to_array(field.data_type(), arr))
            .try_collect::<_, _, ArrayError>()?;
        let nulls = Some(array.null_bitmap().into());
        Ok(Arc::new(
            arrow_array::StructArray::try_new_with_length(fields.clone(), child_arrays, nulls, len)
                .map_err(ArrayError::from_arrow)?,
        ))
    }

    #[inline]
    fn map_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &MapArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let arrow_schema::DataType::Map(field, ordered) = data_type else {
            return Err(ArrayError::to_arrow("Invalid map type"));
        };
        if *ordered {
            return Err(ArrayError::to_arrow("Sorted map is not supported"));
        }
        let values = self
            .struct_to_arrow(field.data_type(), array.as_struct())?
            .as_struct()
            .clone();
        let offsets = OffsetBuffer::new(array.offsets().iter().map(|&o| o as i32).collect());
        let nulls = (!array.null_bitmap().all()).then(|| array.null_bitmap().into());
        Ok(Arc::new(arrow_array::MapArray::new(
            field.clone(),
            offsets,
            values,
            nulls,
            *ordered,
        )))
    }

    /// Convert RisingWave data type to Arrow data type.
    ///
    /// This function returns a `Field` instead of `DataType` because some may be converted to
    /// extension types which require additional metadata in the field.
    fn to_arrow_field(
        &self,
        name: &str,
        value: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        let data_type = match value {
            // using the inline function
            DataType::Boolean => self.bool_type_to_arrow(),
            DataType::Int16 => self.int16_type_to_arrow(),
            DataType::Int32 => self.int32_type_to_arrow(),
            DataType::Int64 => self.int64_type_to_arrow(),
            DataType::Int256 => self.int256_type_to_arrow(),
            DataType::Float32 => self.float32_type_to_arrow(),
            DataType::Float64 => self.float64_type_to_arrow(),
            DataType::Date => self.date_type_to_arrow(),
            DataType::Time => self.time_type_to_arrow(),
            DataType::Timestamp => self.timestamp_type_to_arrow(),
            DataType::Timestamptz => self.timestamptz_type_to_arrow(),
            DataType::Interval => self.interval_type_to_arrow(),
            DataType::Varchar => self.varchar_type_to_arrow(),
            DataType::Bytea => self.bytea_type_to_arrow(),
            DataType::Serial => self.serial_type_to_arrow(),
            DataType::Decimal => return Ok(self.decimal_type_to_arrow(name)),
            DataType::Jsonb => return Ok(self.jsonb_type_to_arrow(name)),
            DataType::Struct(fields) => self.struct_type_to_arrow(fields)?,
            DataType::List(list) => self.list_type_to_arrow(list)?,
            DataType::Map(map) => self.map_type_to_arrow(map)?,
            DataType::Vector(_) => self.vector_type_to_arrow()?,
        };
        Ok(arrow_schema::Field::new(name, data_type, true))
    }

    #[inline]
    fn bool_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Boolean
    }

    #[inline]
    fn int16_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Int16
    }

    #[inline]
    fn int32_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Int32
    }

    #[inline]
    fn int64_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Int64
    }

    #[inline]
    fn int256_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Decimal256(arrow_schema::DECIMAL256_MAX_PRECISION, 0)
    }

    #[inline]
    fn float32_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Float32
    }

    #[inline]
    fn float64_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Float64
    }

    #[inline]
    fn date_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Date32
    }

    #[inline]
    fn time_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond)
    }

    #[inline]
    fn timestamp_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
    }

    #[inline]
    fn timestamptz_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("+00:00".into()),
        )
    }

    #[inline]
    fn interval_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
    }

    #[inline]
    fn varchar_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Utf8
    }

    #[inline]
    fn jsonb_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".into(), "arrowudf.json".into())].into())
    }

    #[inline]
    fn bytea_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Binary
    }

    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(name, arrow_schema::DataType::Utf8, true)
            .with_metadata([("ARROW:extension:name".into(), "arrowudf.decimal".into())].into())
    }

    #[inline]
    fn serial_type_to_arrow(&self) -> arrow_schema::DataType {
        arrow_schema::DataType::Int64
    }

    #[inline]
    fn list_type_to_arrow(
        &self,
        list_type: &ListType,
    ) -> Result<arrow_schema::DataType, ArrayError> {
        Ok(arrow_schema::DataType::List(Arc::new(
            self.to_arrow_field("item", list_type.elem())?,
        )))
    }

    #[inline]
    fn struct_type_to_arrow(
        &self,
        fields: &StructType,
    ) -> Result<arrow_schema::DataType, ArrayError> {
        Ok(arrow_schema::DataType::Struct(
            fields
                .iter()
                .map(|(name, ty)| self.to_arrow_field(name, ty))
                .try_collect::<_, _, ArrayError>()?,
        ))
    }

    #[inline]
    fn map_type_to_arrow(&self, map_type: &MapType) -> Result<arrow_schema::DataType, ArrayError> {
        let sorted = false;
        // "key" is always non-null
        let key = self
            .to_arrow_field("key", map_type.key())?
            .with_nullable(false);
        let value = self.to_arrow_field("value", map_type.value())?;
        Ok(arrow_schema::DataType::Map(
            Arc::new(arrow_schema::Field::new(
                "entries",
                arrow_schema::DataType::Struct([Arc::new(key), Arc::new(value)].into()),
                // "entries" is always non-null
                false,
            )),
            sorted,
        ))
    }

    #[inline]
    fn vector_type_to_arrow(&self) -> Result<arrow_schema::DataType, ArrayError> {
        Ok(arrow_schema::DataType::List(Arc::new(
            self.to_arrow_field("item", &VECTOR_ITEM_TYPE)?,
        )))
    }
}

/// Defines how to convert Arrow arrays to RisingWave arrays.
#[allow(clippy::wrong_self_convention)]
pub trait FromArrow {
    /// Converts Arrow `RecordBatch` to RisingWave `DataChunk`.
    fn from_record_batch(&self, batch: &arrow_array::RecordBatch) -> Result<DataChunk, ArrayError> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (array, field) in batch.columns().iter().zip_eq_fast(batch.schema().fields()) {
            let column = Arc::new(self.from_array(field, array)?);
            columns.push(column);
        }
        Ok(DataChunk::new(columns, batch.num_rows()))
    }

    /// Converts Arrow `Fields` to RisingWave `StructType`.
    fn from_fields(&self, fields: &arrow_schema::Fields) -> Result<StructType, ArrayError> {
        Ok(StructType::new(
            fields
                .iter()
                .map(|f| Ok((f.name().clone(), self.from_field(f)?)))
                .try_collect::<_, Vec<_>, ArrayError>()?,
        ))
    }

    /// Converts Arrow `Field` to RisingWave `DataType`.
    fn from_field(&self, field: &arrow_schema::Field) -> Result<DataType, ArrayError> {
        use arrow_schema::DataType::*;
        use arrow_schema::IntervalUnit::*;
        use arrow_schema::TimeUnit::*;

        // extension type
        if let Some(type_name) = field.metadata().get("ARROW:extension:name") {
            return self.from_extension_type(type_name, field.data_type());
        }

        Ok(match field.data_type() {
            Boolean => DataType::Boolean,
            Int16 => DataType::Int16,
            Int32 => DataType::Int32,
            Int64 => DataType::Int64,
            Int8 => DataType::Int16,
            UInt8 => DataType::Int16,
            UInt16 => DataType::Int32,
            UInt32 => DataType::Int64,
            UInt64 => DataType::Decimal,
            Float16 => DataType::Float32,
            Float32 => DataType::Float32,
            Float64 => DataType::Float64,
            Decimal128(_, _) => DataType::Decimal,
            Decimal256(_, _) => DataType::Int256,
            Date32 => DataType::Date,
            Time64(Microsecond) => DataType::Time,
            Timestamp(Microsecond, None) => DataType::Timestamp,
            Timestamp(Microsecond, Some(_)) => DataType::Timestamptz,
            Timestamp(Second, None) => DataType::Timestamp,
            Timestamp(Second, Some(_)) => DataType::Timestamptz,
            Timestamp(Millisecond, None) => DataType::Timestamp,
            Timestamp(Millisecond, Some(_)) => DataType::Timestamptz,
            Timestamp(Nanosecond, None) => DataType::Timestamp,
            Timestamp(Nanosecond, Some(_)) => DataType::Timestamptz,
            Interval(MonthDayNano) => DataType::Interval,
            Utf8 => DataType::Varchar,
            Utf8View => DataType::Varchar,
            Binary => DataType::Bytea,
            // Iceberg `uuid` maps to `FixedSizeBinary(16)` and `fixed[L]` maps to
            // `FixedSizeBinary(L)`. Both are represented as `Bytea` in RisingWave.
            FixedSizeBinary(_) => self.from_fixed_size_binary()?,
            LargeUtf8 => self.from_large_utf8()?,
            LargeBinary => self.from_large_binary()?,
            List(field) => DataType::list(self.from_field(field)?),
            Struct(fields) => DataType::Struct(self.from_fields(fields)?),
            Map(field, _is_sorted) => {
                let entries = self.from_field(field)?;
                DataType::Map(MapType::try_from_entries(entries).map_err(|e| {
                    ArrayError::from_arrow(format!("invalid arrow map field: {field:?}, err: {e}"))
                })?)
            }
            t => {
                return Err(ArrayError::from_arrow(format!(
                    "unsupported arrow data type: {t:?}"
                )));
            }
        })
    }

    /// Converts Arrow `LargeUtf8` type to RisingWave data type.
    fn from_large_utf8(&self) -> Result<DataType, ArrayError> {
        Ok(DataType::Varchar)
    }

    /// Converts Arrow `LargeBinary` type to RisingWave data type.
    fn from_large_binary(&self) -> Result<DataType, ArrayError> {
        Ok(DataType::Bytea)
    }

    /// Converts Arrow `FixedSizeBinary` type to RisingWave data type.
    fn from_fixed_size_binary(&self) -> Result<DataType, ArrayError> {
        Ok(DataType::Bytea)
    }

    /// Converts Arrow extension type to RisingWave `DataType`.
    fn from_extension_type(
        &self,
        type_name: &str,
        physical_type: &arrow_schema::DataType,
    ) -> Result<DataType, ArrayError> {
        match (type_name, physical_type) {
            ("arrowudf.decimal", arrow_schema::DataType::Utf8) => Ok(DataType::Decimal),
            ("arrowudf.json", arrow_schema::DataType::Utf8) => Ok(DataType::Jsonb),
            _ => Err(ArrayError::from_arrow(format!(
                "unsupported extension type: {type_name:?}"
            ))),
        }
    }

    /// Converts Arrow `Array` to RisingWave `ArrayImpl`.
    ///
    /// `expected_field` is the declared-side field: it selects the extension decode and
    /// authoritatively drives the alignment of nested struct/list/map children.
    fn from_array(
        &self,
        expected_field: &arrow_schema::Field,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        use arrow_schema::DataType::*;
        use arrow_schema::IntervalUnit::*;
        use arrow_schema::TimeUnit::*;

        // extension type
        if let Some(type_name) = expected_field.metadata().get("ARROW:extension:name") {
            return self.from_extension_array(type_name, array);
        }

        match array.data_type() {
            Boolean => self.from_bool_array(array.as_any().downcast_ref().unwrap()),
            Int8 => self.from_int8_array(array.as_any().downcast_ref().unwrap()),
            Int16 => self.from_int16_array(array.as_any().downcast_ref().unwrap()),
            Int32 => self.from_int32_array(array.as_any().downcast_ref().unwrap()),
            Int64 => self.from_int64_array(array.as_any().downcast_ref().unwrap()),
            UInt8 => self.from_uint8_array(array.as_any().downcast_ref().unwrap()),
            UInt16 => self.from_uint16_array(array.as_any().downcast_ref().unwrap()),
            UInt32 => self.from_uint32_array(array.as_any().downcast_ref().unwrap()),

            UInt64 => self.from_uint64_array(array.as_any().downcast_ref().unwrap()),
            Decimal128(_, _) => self.from_decimal128_array(array.as_any().downcast_ref().unwrap()),
            Decimal256(_, _) => self.from_int256_array(array.as_any().downcast_ref().unwrap()),
            Float16 => self.from_float16_array(array.as_any().downcast_ref().unwrap()),
            Float32 => self.from_float32_array(array.as_any().downcast_ref().unwrap()),
            Float64 => self.from_float64_array(array.as_any().downcast_ref().unwrap()),
            Date32 => self.from_date32_array(array.as_any().downcast_ref().unwrap()),
            Time64(Microsecond) => self.from_time64us_array(array.as_any().downcast_ref().unwrap()),
            Timestamp(Second, None) => {
                self.from_timestampsecond_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Second, Some(_)) => {
                self.from_timestampsecond_some_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Millisecond, None) => {
                self.from_timestampms_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Millisecond, Some(_)) => {
                self.from_timestampms_some_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Microsecond, None) => {
                self.from_timestampus_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Microsecond, Some(_)) => {
                self.from_timestampus_some_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Nanosecond, None) => {
                self.from_timestampns_array(array.as_any().downcast_ref().unwrap())
            }
            Timestamp(Nanosecond, Some(_)) => {
                self.from_timestampns_some_array(array.as_any().downcast_ref().unwrap())
            }
            Interval(MonthDayNano) => {
                self.from_interval_array(array.as_any().downcast_ref().unwrap())
            }
            Utf8 => self.from_utf8_array(array.as_any().downcast_ref().unwrap()),
            Utf8View => self.from_utf8_view_array(array.as_any().downcast_ref().unwrap()),
            Binary => self.from_binary_array(array.as_any().downcast_ref().unwrap()),
            FixedSizeBinary(_) => {
                self.from_fixed_size_binary_array(array.as_any().downcast_ref().unwrap())
            }
            LargeUtf8 => self.from_large_utf8_array(array.as_any().downcast_ref().unwrap()),
            LargeBinary => self.from_large_binary_array(array.as_any().downcast_ref().unwrap()),
            List(_) => self.from_list_array(expected_field, array.as_any().downcast_ref().unwrap()),
            Struct(_) => {
                self.from_struct_array(expected_field, array.as_any().downcast_ref().unwrap())
            }
            Map(_, _) => {
                self.from_map_array(expected_field, array.as_any().downcast_ref().unwrap())
            }
            t => Err(ArrayError::from_arrow(format!(
                "unsupported arrow data type: {t:?}",
            ))),
        }
    }

    /// Converts Arrow extension array to RisingWave `ArrayImpl`.
    fn from_extension_array(
        &self,
        type_name: &str,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        match type_name {
            "arrowudf.decimal" => {
                let array: &arrow_array::StringArray =
                    array.as_any().downcast_ref().ok_or_else(|| {
                        ArrayError::from_arrow(
                            "expected string array for `arrowudf.decimal`".to_owned(),
                        )
                    })?;
                Ok(ArrayImpl::Decimal(array.try_into()?))
            }
            "arrowudf.json" => {
                let array: &arrow_array::StringArray =
                    array.as_any().downcast_ref().ok_or_else(|| {
                        ArrayError::from_arrow(
                            "expected string array for `arrowudf.json`".to_owned(),
                        )
                    })?;
                Ok(ArrayImpl::Jsonb(array.try_into()?))
            }
            _ => Err(ArrayError::from_arrow(format!(
                "unsupported extension type: {type_name:?}"
            ))),
        }
    }

    fn from_bool_array(&self, array: &arrow_array::BooleanArray) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Bool(array.into()))
    }

    fn from_int16_array(&self, array: &arrow_array::Int16Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int16(array.into()))
    }

    fn from_int8_array(&self, array: &arrow_array::Int8Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int16(array.into()))
    }

    fn from_uint8_array(&self, array: &arrow_array::UInt8Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int16(array.into()))
    }

    fn from_uint16_array(&self, array: &arrow_array::UInt16Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int32(array.into()))
    }

    fn from_uint32_array(&self, array: &arrow_array::UInt32Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int64(array.into()))
    }

    fn from_int32_array(&self, array: &arrow_array::Int32Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int32(array.into()))
    }

    fn from_int64_array(&self, array: &arrow_array::Int64Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int64(array.into()))
    }

    fn from_int256_array(
        &self,
        array: &arrow_array::Decimal256Array,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Int256(array.into()))
    }

    fn from_decimal128_array(
        &self,
        array: &arrow_array::Decimal128Array,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Decimal(array.try_into()?))
    }

    fn from_uint64_array(&self, array: &arrow_array::UInt64Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Decimal(array.try_into()?))
    }

    fn from_float16_array(
        &self,
        array: &arrow_array::Float16Array,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Float32(array.try_into()?))
    }

    fn from_float32_array(
        &self,
        array: &arrow_array::Float32Array,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Float32(array.into()))
    }

    fn from_float64_array(
        &self,
        array: &arrow_array::Float64Array,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Float64(array.into()))
    }

    fn from_date32_array(&self, array: &arrow_array::Date32Array) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Date(array.into()))
    }

    fn from_time64us_array(
        &self,
        array: &arrow_array::Time64MicrosecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Time(array.into()))
    }

    fn from_timestampsecond_array(
        &self,
        array: &arrow_array::TimestampSecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamp(array.into()))
    }
    fn from_timestampsecond_some_array(
        &self,
        array: &arrow_array::TimestampSecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamptz(array.into()))
    }

    fn from_timestampms_array(
        &self,
        array: &arrow_array::TimestampMillisecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamp(array.into()))
    }

    fn from_timestampms_some_array(
        &self,
        array: &arrow_array::TimestampMillisecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamptz(array.into()))
    }

    fn from_timestampus_array(
        &self,
        array: &arrow_array::TimestampMicrosecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamp(array.into()))
    }

    fn from_timestampus_some_array(
        &self,
        array: &arrow_array::TimestampMicrosecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamptz(array.into()))
    }

    fn from_timestampns_array(
        &self,
        array: &arrow_array::TimestampNanosecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamp(array.into()))
    }

    fn from_timestampns_some_array(
        &self,
        array: &arrow_array::TimestampNanosecondArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Timestamptz(array.into()))
    }

    fn from_interval_array(
        &self,
        array: &arrow_array::IntervalMonthDayNanoArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Interval(array.into()))
    }

    fn from_utf8_array(&self, array: &arrow_array::StringArray) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Utf8(array.into()))
    }

    fn from_utf8_view_array(
        &self,
        array: &arrow_array::StringViewArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Utf8(array.into()))
    }

    fn from_binary_array(&self, array: &arrow_array::BinaryArray) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Bytea(array.into()))
    }

    fn from_large_utf8_array(
        &self,
        array: &arrow_array::LargeStringArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Utf8(array.into()))
    }

    fn from_large_binary_array(
        &self,
        array: &arrow_array::LargeBinaryArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Bytea(array.into()))
    }

    fn from_fixed_size_binary_array(
        &self,
        array: &arrow_array::FixedSizeBinaryArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Bytea(array.iter().collect()))
    }

    /// Converts an Arrow `ListArray`, decoding elements by the expected element field so that
    /// nested decode keeps following the declared schema. Falls back to the array's own element
    /// field when the expected field does not describe a list.
    fn from_list_array(
        &self,
        expected_field: &arrow_schema::Field,
        array: &arrow_array::ListArray,
    ) -> Result<ArrayImpl, ArrayError> {
        use arrow_array::Array;
        let elem_field = match (expected_field.data_type(), array.data_type()) {
            (arrow_schema::DataType::List(elem), _) | (_, arrow_schema::DataType::List(elem)) => {
                elem
            }
            _ => unreachable!("a list array must have a list data type"),
        };
        Ok(ArrayImpl::List(ListArray {
            value: Box::new(self.from_array(elem_field, array.values())?),
            bitmap: match array.nulls() {
                Some(nulls) => nulls.iter().collect(),
                None => Bitmap::ones(array.len()),
            },
            offsets: array.offsets().iter().map(|o| *o as u32).collect(),
        }))
    }

    /// Converts an Arrow `StructArray` by the expected struct field: children align by name
    /// (first occurrence, extras dropped), positionally when a name is missing but the arity
    /// matches (an external UDF may label struct children differently, as its signature check
    /// ignores nested field names), and error otherwise. Extensions are taken from the
    /// expected side only.
    ///
    /// The result carries the expected field *names* with the *decoded* child types: the
    /// expected field may be a lossy rendering of the declared type (e.g. iceberg has no
    /// 16-bit int), so the decoded types are authoritative and any divergence from the
    /// declared type stays visible to the callers' boundary checks.
    ///
    /// Falls back to the array's own fields when the expected field does not describe a struct.
    fn from_struct_array(
        &self,
        expected_field: &arrow_schema::Field,
        array: &arrow_array::StructArray,
    ) -> Result<ArrayImpl, ArrayError> {
        use std::collections::HashMap;

        use arrow_array::Array;

        let arrow_schema::DataType::Struct(actual_fields) = array.data_type() else {
            unreachable!("a struct array must have a struct data type");
        };
        let expected_fields = match expected_field.data_type() {
            arrow_schema::DataType::Struct(fields) => fields,
            _ => actual_fields,
        };

        let len = array.len();
        let decode_positionally = |fields: &arrow_schema::Fields| {
            array
                .columns()
                .iter()
                .zip_eq_fast(fields)
                .map(|(column, field)| self.from_array(field, column).map(Arc::new))
                .try_collect()
        };
        // Children decode by the expected field either way, so aligning names in order is
        // enough for the zip fast path — no deep field comparison needed.
        let names_aligned = expected_fields.len() == actual_fields.len()
            && expected_fields
                .iter()
                .zip_eq_fast(actual_fields.iter())
                .all(|(e, a)| e.name() == a.name());
        let columns: Vec<Arc<ArrayImpl>> = if names_aligned {
            decode_positionally(expected_fields)?
        } else {
            // First occurrence wins, agreeing with the schema matcher's `find`.
            let mut actual_name_to_index = HashMap::new();
            for (idx, f) in actual_fields.iter().enumerate() {
                actual_name_to_index.entry(f.name().as_str()).or_insert(idx);
            }
            if expected_fields
                .iter()
                .all(|f| actual_name_to_index.contains_key(f.name().as_str()))
            {
                expected_fields
                    .iter()
                    .map(|expected_field| {
                        let idx = actual_name_to_index[expected_field.name().as_str()];
                        self.from_array(expected_field, &array.columns()[idx])
                            .map(Arc::new)
                    })
                    .try_collect()?
            } else if expected_fields.len() == actual_fields.len() {
                // Positional fallback for external UDFs. Unreachable on the parquet path:
                // the schema matcher requires every declared name to be present.
                decode_positionally(expected_fields)?
            } else {
                let names =
                    |fields: &arrow_schema::Fields| fields.iter().map(|f| f.name()).join(", ");
                return Err(ArrayError::from_arrow(format!(
                    "unable to align struct fields: expected [{}], actual [{}]",
                    names(expected_fields),
                    names(actual_fields),
                )));
            }
        };

        Ok(ArrayImpl::Struct(StructArray::new(
            StructType::new(
                expected_fields
                    .iter()
                    .zip_eq_fast(columns.iter())
                    .map(|(f, c)| (f.name().clone(), c.data_type())),
            ),
            columns,
            (0..len).map(|i| array.is_valid(i)).collect(),
        )))
    }

    /// Converts an Arrow `MapArray`, decoding entries by the expected entries field. Falls back
    /// to the array's own entries field when the expected field does not describe a map.
    fn from_map_array(
        &self,
        expected_field: &arrow_schema::Field,
        array: &arrow_array::MapArray,
    ) -> Result<ArrayImpl, ArrayError> {
        use arrow_array::Array;
        let expected_entries = match (expected_field.data_type(), array.data_type()) {
            (arrow_schema::DataType::Map(entries, _), _)
            | (_, arrow_schema::DataType::Map(entries, _)) => entries,
            _ => unreachable!("a map array must have a map data type"),
        };
        let struct_array = self.from_struct_array(expected_entries, array.entries())?;
        let list_array = ListArray {
            value: Box::new(struct_array),
            bitmap: match array.nulls() {
                Some(nulls) => nulls.iter().collect(),
                None => Bitmap::ones(array.len()),
            },
            offsets: array.offsets().iter().map(|o| *o as u32).collect(),
        };

        Ok(ArrayImpl::Map(MapArray { inner: list_array }))
    }
}

impl From<&Bitmap> for arrow_buffer::NullBuffer {
    fn from(bitmap: &Bitmap) -> Self {
        bitmap.iter().collect()
    }
}

/// Implement bi-directional `From` between concrete array types.
macro_rules! converts {
    ($ArrayType:ty, $ArrowType:ty) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array.iter().collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays.iter().flat_map(|a| a.iter()).collect()
            }
        }
    };
    // convert values using FromIntoArrow
    ($ArrayType:ty, $ArrowType:ty, @map) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().map(|o| o.map(|v| v.into_arrow())).collect()
            }
        }
        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array
                    .iter()
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays
                    .iter()
                    .flat_map(|a| a.iter())
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrow>::from_arrow(v)
                        })
                    })
                    .collect()
            }
        }
    };
}

/// Used to convert different types.
macro_rules! converts_with_type {
    ($ArrayType:ty, $ArrowType:ty, $FromType:ty, $ToType:ty) => {
        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                let values: Vec<Option<$ToType>> =
                    array.iter().map(|x| x.map(|v| v as $ToType)).collect();
                <$ArrowType>::from_iter(values)
            }
        }

        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                let values: Vec<Option<$FromType>> =
                    array.iter().map(|x| x.map(|v| v as $FromType)).collect();
                <$ArrayType>::from_iter(values)
            }
        }

        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                let values: Vec<Option<$FromType>> = arrays
                    .iter()
                    .flat_map(|a| a.iter().map(|x| x.map(|v| v as $FromType)))
                    .collect();
                <$ArrayType>::from_iter(values)
            }
        }
    };
}

macro_rules! converts_with_timeunit {
    ($ArrayType:ty, $ArrowType:ty, $time_unit:expr, @map) => {

        impl From<&$ArrayType> for $ArrowType {
            fn from(array: &$ArrayType) -> Self {
                array.iter().map(|o| o.map(|v| v.into_arrow_with_unit($time_unit))).collect()
            }
        }

        impl From<&$ArrowType> for $ArrayType {
            fn from(array: &$ArrowType) -> Self {
                array.iter().map(|o| {
                    o.map(|v| {
                        let timestamp = <<$ArrayType as Array>::RefItem<'_> as FromIntoArrowWithUnit>::from_arrow_with_unit(v, $time_unit);
                        timestamp
                    })
                }).collect()
            }
        }

        impl From<&[$ArrowType]> for $ArrayType {
            fn from(arrays: &[$ArrowType]) -> Self {
                arrays
                    .iter()
                    .flat_map(|a| a.iter())
                    .map(|o| {
                        o.map(|v| {
                            <<$ArrayType as Array>::RefItem<'_> as FromIntoArrowWithUnit>::from_arrow_with_unit(v, $time_unit)
                        })
                    })
                    .collect()
            }
        }

    };
}

converts!(BoolArray, arrow_array::BooleanArray);
converts!(I16Array, arrow_array::Int16Array);
converts!(I32Array, arrow_array::Int32Array);
converts!(I64Array, arrow_array::Int64Array);
converts!(F32Array, arrow_array::Float32Array, @map);
converts!(F64Array, arrow_array::Float64Array, @map);
converts!(BytesArray, arrow_array::BinaryArray);
converts!(BytesArray, arrow_array::LargeBinaryArray);
converts!(Utf8Array, arrow_array::StringArray);
converts!(Utf8Array, arrow_array::LargeStringArray);
converts!(Utf8Array, arrow_array::StringViewArray);
converts!(DateArray, arrow_array::Date32Array, @map);
converts!(TimeArray, arrow_array::Time64MicrosecondArray, @map);
converts!(IntervalArray, arrow_array::IntervalMonthDayNanoArray, @map);
converts!(SerialArray, arrow_array::Int64Array, @map);

converts_with_type!(I16Array, arrow_array::Int8Array, i16, i8);
converts_with_type!(I16Array, arrow_array::UInt8Array, i16, u8);
converts_with_type!(I32Array, arrow_array::UInt16Array, i32, u16);
converts_with_type!(I64Array, arrow_array::UInt32Array, i64, u32);

converts_with_timeunit!(TimestampArray, arrow_array::TimestampSecondArray, TimeUnit::Second, @map);
converts_with_timeunit!(TimestampArray, arrow_array::TimestampMillisecondArray, TimeUnit::Millisecond, @map);
converts_with_timeunit!(TimestampArray, arrow_array::TimestampMicrosecondArray, TimeUnit::Microsecond, @map);
converts_with_timeunit!(TimestampArray, arrow_array::TimestampNanosecondArray, TimeUnit::Nanosecond, @map);

converts_with_timeunit!(TimestamptzArray, arrow_array::TimestampSecondArray, TimeUnit::Second, @map);
converts_with_timeunit!(TimestamptzArray, arrow_array::TimestampMillisecondArray,TimeUnit::Millisecond, @map);
converts_with_timeunit!(TimestamptzArray, arrow_array::TimestampMicrosecondArray, TimeUnit::Microsecond, @map);
converts_with_timeunit!(TimestamptzArray, arrow_array::TimestampNanosecondArray, TimeUnit::Nanosecond, @map);

/// Converts RisingWave value from and into Arrow value.
trait FromIntoArrow {
    /// The corresponding element type in the Arrow array.
    type ArrowType;
    fn from_arrow(value: Self::ArrowType) -> Self;
    fn into_arrow(self) -> Self::ArrowType;
}

/// Converts RisingWave value from and into Arrow value.
/// Specifically used for converting timestamp types according to timeunit.
trait FromIntoArrowWithUnit {
    type ArrowType;
    /// The timestamp type used to distinguish different time units, only utilized when the Arrow type is a timestamp.
    type TimestampType;
    fn from_arrow_with_unit(value: Self::ArrowType, time_unit: Self::TimestampType) -> Self;
    fn into_arrow_with_unit(self, time_unit: Self::TimestampType) -> Self::ArrowType;
}

impl FromIntoArrow for Serial {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for F32 {
    type ArrowType = f32;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for F64 {
    type ArrowType = f64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        value.into()
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.into()
    }
}

impl FromIntoArrow for Date {
    type ArrowType = i32;

    #[allow(deprecated)]
    fn from_arrow(value: Self::ArrowType) -> Self {
        Date(arrow_array::types::Date32Type::to_naive_date(value))
    }

    fn into_arrow(self) -> Self::ArrowType {
        arrow_array::types::Date32Type::from_naive_date(self.0)
    }
}

impl FromIntoArrow for Time {
    type ArrowType = i64;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Time(
            NaiveTime::from_num_seconds_from_midnight_opt(
                (value / 1_000_000) as _,
                (value % 1_000_000 * 1000) as _,
            )
            .unwrap(),
        )
    }

    fn into_arrow(self) -> Self::ArrowType {
        self.0
            .signed_duration_since(NaiveTime::default())
            .num_microseconds()
            .unwrap()
    }
}

impl FromIntoArrowWithUnit for Timestamp {
    type ArrowType = i64;
    type TimestampType = TimeUnit;

    fn from_arrow_with_unit(value: Self::ArrowType, time_unit: Self::TimestampType) -> Self {
        match time_unit {
            TimeUnit::Second => {
                Timestamp(DateTime::from_timestamp(value as _, 0).unwrap().naive_utc())
            }
            TimeUnit::Millisecond => {
                Timestamp(DateTime::from_timestamp_millis(value).unwrap().naive_utc())
            }
            TimeUnit::Microsecond => {
                Timestamp(DateTime::from_timestamp_micros(value).unwrap().naive_utc())
            }
            TimeUnit::Nanosecond => Timestamp(DateTime::from_timestamp_nanos(value).naive_utc()),
        }
    }

    fn into_arrow_with_unit(self, time_unit: Self::TimestampType) -> Self::ArrowType {
        match time_unit {
            TimeUnit::Second => self.0.and_utc().timestamp(),
            TimeUnit::Millisecond => self.0.and_utc().timestamp_millis(),
            TimeUnit::Microsecond => self.0.and_utc().timestamp_micros(),
            TimeUnit::Nanosecond => self.0.and_utc().timestamp_nanos_opt().unwrap(),
        }
    }
}

impl FromIntoArrowWithUnit for Timestamptz {
    type ArrowType = i64;
    type TimestampType = TimeUnit;

    fn from_arrow_with_unit(value: Self::ArrowType, time_unit: Self::TimestampType) -> Self {
        match time_unit {
            TimeUnit::Second => Timestamptz::from_secs(value).unwrap_or_default(),
            TimeUnit::Millisecond => Timestamptz::from_millis(value).unwrap_or_default(),
            TimeUnit::Microsecond => Timestamptz::from_micros(value),
            TimeUnit::Nanosecond => Timestamptz::from_nanos(value).unwrap_or_default(),
        }
    }

    fn into_arrow_with_unit(self, time_unit: Self::TimestampType) -> Self::ArrowType {
        match time_unit {
            TimeUnit::Second => self.timestamp(),
            TimeUnit::Millisecond => self.timestamp_millis(),
            TimeUnit::Microsecond => self.timestamp_micros(),
            TimeUnit::Nanosecond => self.timestamp_nanos().unwrap(),
        }
    }
}

impl FromIntoArrow for Interval {
    type ArrowType = ArrowIntervalType;

    fn from_arrow(value: Self::ArrowType) -> Self {
        Interval::from_month_day_usec(value.months, value.days, value.nanoseconds / 1000)
    }

    fn into_arrow(self) -> Self::ArrowType {
        ArrowIntervalType {
            months: self.months(),
            days: self.days(),
            // TODO: this may overflow and we need `try_into`
            nanoseconds: self.usecs() * 1000,
        }
    }
}

impl From<&DecimalArray> for arrow_array::LargeBinaryArray {
    fn from(array: &DecimalArray) -> Self {
        let mut builder =
            arrow_array::builder::LargeBinaryBuilder::with_capacity(array.len(), array.len() * 8);
        for value in array.iter() {
            builder.append_option(value.map(|d| d.to_string()));
        }
        builder.finish()
    }
}

impl From<&DecimalArray> for arrow_array::StringArray {
    fn from(array: &DecimalArray) -> Self {
        let mut builder =
            arrow_array::builder::StringBuilder::with_capacity(array.len(), array.len() * 8);
        for value in array.iter() {
            builder.append_option(value.map(|d| d.to_string()));
        }
        builder.finish()
    }
}

// This arrow decimal type is used by iceberg source to read iceberg decimal into RW decimal.
impl TryFrom<&arrow_array::Decimal128Array> for DecimalArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::Decimal128Array) -> Result<Self, Self::Error> {
        if array.scale() < 0 {
            bail!("support negative scale for arrow decimal")
        }

        // Calculate the max value based on the Arrow decimal's precision
        // When writing Inf to Arrow Decimal128(precision, scale), we use 10^precision - 1
        let precision = array.precision();
        let max_value = 10_i128.pow(precision as u32) - 1;

        let from_arrow = |value| {
            const NAN: i128 = i128::MIN + 1;
            let res = match value {
                // Check for special values using Arrow Decimal's max value, not i128::MAX
                NAN => Decimal::NaN,
                v if v == max_value => Decimal::PositiveInf,
                v if v == -max_value => Decimal::NegativeInf,
                i128::MAX => Decimal::PositiveInf, // Fallback for old data
                i128::MIN => Decimal::NegativeInf, // Fallback for old data
                _ => Decimal::truncated_i128_and_scale(value, array.scale() as u32)
                    .ok_or_else(|| ArrayError::from_arrow("decimal overflow"))?,
            };
            Ok(res)
        };
        array
            .iter()
            .map(|o| o.map(from_arrow).transpose())
            .collect::<Result<Self, Self::Error>>()
    }
}

// Since RisingWave does not support UInt type, convert UInt64Array to Decimal.
impl TryFrom<&arrow_array::UInt64Array> for DecimalArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::UInt64Array) -> Result<Self, Self::Error> {
        let from_arrow = |value| {
            // Convert the value to a Decimal with scale 0
            let res = Decimal::from(value);
            Ok(res)
        };

        // Map over the array and convert each value
        array
            .iter()
            .map(|o| o.map(from_arrow).transpose())
            .collect::<Result<Self, Self::Error>>()
    }
}

impl TryFrom<&arrow_array::Float16Array> for F32Array {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::Float16Array) -> Result<Self, Self::Error> {
        let from_arrow = |value| Ok(f32::from(value));

        array
            .iter()
            .map(|o| o.map(from_arrow).transpose())
            .collect::<Result<Self, Self::Error>>()
    }
}

impl TryFrom<&arrow_array::LargeBinaryArray> for DecimalArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::LargeBinaryArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    let s = std::str::from_utf8(s)
                        .map_err(|_| ArrayError::from_arrow(format!("invalid decimal: {s:?}")))?;
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid decimal: {s:?}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl TryFrom<&arrow_array::StringArray> for DecimalArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::StringArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid decimal: {s:?}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<&JsonbArray> for arrow_array::StringArray {
    fn from(array: &JsonbArray) -> Self {
        let mut builder =
            arrow_array::builder::StringBuilder::with_capacity(array.len(), array.len() * 16);
        for value in array.iter() {
            match value {
                Some(jsonb) => {
                    write!(&mut builder, "{}", jsonb).unwrap();
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }
        builder.finish()
    }
}

impl TryFrom<&arrow_array::StringArray> for JsonbArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::StringArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid json: {s}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<&IntervalArray> for arrow_array::StringArray {
    fn from(array: &IntervalArray) -> Self {
        let mut builder =
            arrow_array::builder::StringBuilder::with_capacity(array.len(), array.len() * 16);
        for value in array.iter() {
            match value {
                Some(interval) => {
                    write!(&mut builder, "{}", interval).unwrap();
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }
        builder.finish()
    }
}

impl From<&JsonbArray> for arrow_array::LargeStringArray {
    fn from(array: &JsonbArray) -> Self {
        let mut builder =
            arrow_array::builder::LargeStringBuilder::with_capacity(array.len(), array.len() * 16);
        for value in array.iter() {
            match value {
                Some(jsonb) => {
                    write!(&mut builder, "{}", jsonb).unwrap();
                    builder.append_value("");
                }
                None => builder.append_null(),
            }
        }
        builder.finish()
    }
}

impl TryFrom<&arrow_array::LargeStringArray> for JsonbArray {
    type Error = ArrayError;

    fn try_from(array: &arrow_array::LargeStringArray) -> Result<Self, Self::Error> {
        array
            .iter()
            .map(|o| {
                o.map(|s| {
                    s.parse()
                        .map_err(|_| ArrayError::from_arrow(format!("invalid json: {s}")))
                })
                .transpose()
            })
            .try_collect()
    }
}

impl From<arrow_buffer::i256> for Int256 {
    fn from(value: arrow_buffer::i256) -> Self {
        let buffer = value.to_be_bytes();
        Int256::from_be_bytes(buffer)
    }
}

impl<'a> From<Int256Ref<'a>> for arrow_buffer::i256 {
    fn from(val: Int256Ref<'a>) -> Self {
        let buffer = val.to_be_bytes();
        arrow_buffer::i256::from_be_bytes(buffer)
    }
}

impl From<&Int256Array> for arrow_array::Decimal256Array {
    fn from(array: &Int256Array) -> Self {
        array
            .iter()
            .map(|o| o.map(arrow_buffer::i256::from))
            .collect()
    }
}

impl From<&arrow_array::Decimal256Array> for Int256Array {
    fn from(array: &arrow_array::Decimal256Array) -> Self {
        let values = array.iter().map(|o| o.map(Int256::from)).collect_vec();

        values
            .iter()
            .map(|i| i.as_ref().map(|v| v.as_scalar_ref()))
            .collect()
    }
}

/// This function checks whether the schema of a Parquet file matches the user-defined schema in RisingWave.
/// It handles the following special cases:
/// - Arrow's `timestamp(_, None)` types (all four time units) match with RisingWave's `Timestamp` type.
/// - Arrow's `timestamp(_, Some)` matches with RisingWave's `Timestamptz` type.
/// - Since RisingWave does not have an `UInt` type:
///   - Arrow's `UInt8` matches with RisingWave's `Int16`.
///   - Arrow's `UInt16` matches with RisingWave's `Int32`.
///   - Arrow's `UInt32` matches with RisingWave's `Int64`.
///   - Arrow's `UInt64` matches with RisingWave's `Decimal`.
/// - Arrow's `Float16` matches with RisingWave's `Float32`.
///
/// Nested data type matching:
/// - Struct: Arrow's `Struct` type matches with RisingWave's `Struct` type recursively, requiring that all expected fields exist and match by name and type. Extra Arrow fields are allowed, but a declared name matching multiple Arrow siblings is ambiguous and rejected.
/// - List: Arrow's `List` type matches with RisingWave's `List` type recursively, requiring the same element type.
/// - Map: Arrow's `Map` type matches with RisingWave's `Map` type recursively, requiring the key and value types to match, and the inner struct must have exactly two fields named "key" and "value".
pub fn is_parquet_schema_match_source_schema(
    arrow_data_type: &arrow_schema::DataType,
    rw_data_type: &crate::types::DataType,
) -> bool {
    use arrow_schema::DataType as ArrowType;

    use crate::types::{DataType as RwType, MapType, StructType};

    match (arrow_data_type, rw_data_type) {
        // Primitive type matching and special cases
        (ArrowType::Boolean, RwType::Boolean)
        | (ArrowType::Int8 | ArrowType::Int16 | ArrowType::UInt8, RwType::Int16)
        | (ArrowType::Int32 | ArrowType::UInt16, RwType::Int32)
        | (ArrowType::Int64 | ArrowType::UInt32, RwType::Int64)
        | (ArrowType::UInt64 | ArrowType::Decimal128(_, _), RwType::Decimal)
        | (ArrowType::Decimal256(_, _), RwType::Int256)
        | (ArrowType::Float16 | ArrowType::Float32, RwType::Float32)
        | (ArrowType::Float64, RwType::Float64)
        | (ArrowType::Timestamp(_, None), RwType::Timestamp)
        | (ArrowType::Timestamp(_, Some(_)), RwType::Timestamptz)
        | (ArrowType::Date32, RwType::Date)
        | (ArrowType::Time32(_) | ArrowType::Time64(_), RwType::Time)
        | (ArrowType::Interval(arrow_schema::IntervalUnit::MonthDayNano), RwType::Interval)
        | (ArrowType::Utf8 | ArrowType::LargeUtf8, RwType::Varchar)
        | (
            ArrowType::Binary | ArrowType::LargeBinary | ArrowType::FixedSizeBinary(_),
            RwType::Bytea,
        ) => true,

        // Struct type recursive matching
        // Arrow's Struct matches RisingWave's Struct if all expected field names exist and types
        // match recursively. Extra Arrow fields are allowed and field order is ignored.
        (ArrowType::Struct(arrow_fields), RwType::Struct(rw_struct)) => {
            if arrow_fields.len() < rw_struct.len() {
                return false;
            }
            for (rw_name, rw_ty) in rw_struct.iter() {
                let mut candidates = arrow_fields.iter().filter(|f| f.name() == rw_name);
                let Some(arrow_field) = candidates.next() else {
                    return false;
                };
                // Parquet permits duplicate sibling names; which one holds the data is
                // ambiguous, so reject the match.
                if candidates.next().is_some() {
                    return false;
                }
                if !is_parquet_schema_match_source_schema(arrow_field.data_type(), rw_ty) {
                    return false;
                }
            }
            true
        }
        // List type recursive matching
        // Arrow's List matches RisingWave's List if the element type matches recursively
        (ArrowType::List(arrow_field), RwType::List(rw_list_ty)) => {
            is_parquet_schema_match_source_schema(arrow_field.data_type(), rw_list_ty.elem())
        }
        // Map type recursive matching
        // Arrow's Map matches RisingWave's Map if the key and value types match recursively,
        // and the inner struct has exactly two fields named "key" and "value"
        (ArrowType::Map(arrow_field, _), RwType::Map(rw_map_ty)) => {
            if let ArrowType::Struct(fields) = arrow_field.data_type() {
                if fields.len() != 2 {
                    return false;
                }
                let key_field = &fields[0];
                let value_field = &fields[1];
                if key_field.name() != "key" || value_field.name() != "value" {
                    return false;
                }
                let (rw_key_ty, rw_value_ty) = (rw_map_ty.key(), rw_map_ty.value());
                is_parquet_schema_match_source_schema(key_field.data_type(), rw_key_ty)
                    && is_parquet_schema_match_source_schema(value_field.data_type(), rw_value_ty)
            } else {
                false
            }
        }
        // Fallback: types do not match
        _ => false,
    }
}
#[cfg(test)]
mod tests {

    use arrow_schema::{DataType as ArrowType, Field as ArrowField};

    use super::*;
    use crate::types::{DataType as RwType, MapType, StructType};

    /// A default-only `FromArrow` for exercising the shared decode logic.
    struct Dummy;
    impl FromArrow for Dummy {}

    #[test]
    fn test_struct_schema_match() {
        // Arrow: struct<f1: Double, f2: Utf8>

        let arrow_struct = ArrowType::Struct(
            vec![
                ArrowField::new("f1", ArrowType::Float64, true),
                ArrowField::new("f2", ArrowType::Utf8, true),
            ]
            .into(),
        );
        // RW: struct<f1 Double, f2 Varchar>
        let rw_struct = RwType::Struct(StructType::new(vec![
            ("f1".to_owned(), RwType::Float64),
            ("f2".to_owned(), RwType::Varchar),
        ]));
        assert!(is_parquet_schema_match_source_schema(
            &arrow_struct,
            &rw_struct
        ));

        // Arrow is a superset of RW struct fields.
        let arrow_struct_superset = ArrowType::Struct(
            vec![
                ArrowField::new("f1", ArrowType::Float64, true),
                ArrowField::new("f2", ArrowType::Utf8, true),
                ArrowField::new("f3", ArrowType::Int32, true),
            ]
            .into(),
        );
        assert!(is_parquet_schema_match_source_schema(
            &arrow_struct_superset,
            &rw_struct
        ));

        // Field order is ignored for struct matching.
        let arrow_struct_reordered = ArrowType::Struct(
            vec![
                ArrowField::new("f2", ArrowType::Utf8, true),
                ArrowField::new("f1", ArrowType::Float64, true),
            ]
            .into(),
        );
        assert!(is_parquet_schema_match_source_schema(
            &arrow_struct_reordered,
            &rw_struct
        ));

        // Field names do not match
        let arrow_struct2 = ArrowType::Struct(
            vec![
                ArrowField::new("f1", ArrowType::Float64, true),
                ArrowField::new("f3", ArrowType::Utf8, true),
            ]
            .into(),
        );
        assert!(!is_parquet_schema_match_source_schema(
            &arrow_struct2,
            &rw_struct
        ));
    }

    #[test]
    fn test_struct_duplicate_sibling_names_reject_match() {
        let rw_struct = RwType::Struct(StructType::new(vec![("f1".to_owned(), RwType::Float64)]));

        // A declared name matching multiple Arrow siblings is ambiguous, even when the
        // duplicates carry the same type.
        for dup_type in [ArrowType::Float64, ArrowType::Utf8] {
            let arrow_struct = ArrowType::Struct(
                vec![
                    ArrowField::new("f1", ArrowType::Float64, true),
                    ArrowField::new("f1", dup_type, true),
                ]
                .into(),
            );
            assert!(!is_parquet_schema_match_source_schema(
                &arrow_struct,
                &rw_struct
            ));
        }

        // Duplicates among extra (undeclared) fields are irrelevant: they are dropped anyway.
        let arrow_struct = ArrowType::Struct(
            vec![
                ArrowField::new("f1", ArrowType::Float64, true),
                ArrowField::new("extra", ArrowType::Int32, true),
                ArrowField::new("extra", ArrowType::Utf8, true),
            ]
            .into(),
        );
        assert!(is_parquet_schema_match_source_schema(
            &arrow_struct,
            &rw_struct
        ));
    }

    #[test]
    fn test_struct_projection_from_arrow() {
        use itertools::Itertools;

        // Actual Arrow struct: struct<foo:int32, bar:utf8, baz:int32>
        let actual_fields: arrow_schema::Fields = vec![
            ArrowField::new("foo", ArrowType::Int32, true),
            ArrowField::new("bar", ArrowType::Utf8, true),
            ArrowField::new("baz", ArrowType::Int32, true),
        ]
        .into();
        let foo: arrow_array::ArrayRef =
            Arc::new(arrow_array::Int32Array::from(vec![Some(10), Some(20)]));
        let bar: arrow_array::ArrayRef =
            Arc::new(arrow_array::StringArray::from(vec![Some("a"), Some("b")]));
        let baz: arrow_array::ArrayRef =
            Arc::new(arrow_array::Int32Array::from(vec![Some(100), Some(200)]));
        let actual_struct = arrow_array::StructArray::new(actual_fields, vec![foo, bar, baz], None);
        let actual_struct_ref: arrow_array::ArrayRef = Arc::new(actual_struct);

        // Expected struct in RW schema (via to_arrow_field): struct<foo:int32, bar:utf8>
        let expected_field = ArrowField::new(
            "s",
            ArrowType::Struct(
                vec![
                    ArrowField::new("foo", ArrowType::Int32, true),
                    ArrowField::new("bar", ArrowType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let array_impl = Dummy
            .from_array(&expected_field, &actual_struct_ref)
            .unwrap();

        let ArrayImpl::Struct(s) = array_impl else {
            panic!("expected RW StructArray");
        };

        let DataType::Struct(st) = s.data_type() else {
            panic!("expected RW struct type");
        };
        assert_eq!(st.len(), 2);
        assert_eq!(st.iter().map(|(n, _)| n).collect_vec(), vec!["foo", "bar"]);

        let v0 = s.value_at(0).unwrap().to_owned_scalar();
        let v1 = s.value_at(1).unwrap().to_owned_scalar();
        assert_eq!(
            v0,
            StructValue::new(vec![
                Some(ScalarImpl::Int32(10)),
                Some(ScalarImpl::Utf8("a".into()))
            ])
        );
        assert_eq!(
            v1,
            StructValue::new(vec![
                Some(ScalarImpl::Int32(20)),
                Some(ScalarImpl::Utf8("b".into()))
            ])
        );
    }

    /// Builds a two-element `list<struct>` array from the given element fields and columns.
    fn build_list_of_struct(
        elem_fields: arrow_schema::Fields,
        columns: Vec<arrow_array::ArrayRef>,
    ) -> arrow_array::ArrayRef {
        use std::sync::Arc;
        let elem_struct = arrow_array::StructArray::new(elem_fields.clone(), columns, None);
        Arc::new(arrow_array::ListArray::new(
            Arc::new(ArrowField::new(
                "element",
                ArrowType::Struct(elem_fields),
                true,
            )),
            arrow_buffer::OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(elem_struct),
            None,
        ))
    }

    #[test]
    fn test_list_element_struct_decodes_by_declared_field() {
        // File: list<struct<b utf8, a int32, extra int32>> — reordered and a superset of the
        // declared element struct.
        let file_array = build_list_of_struct(
            vec![
                ArrowField::new("b", ArrowType::Utf8, true),
                ArrowField::new("a", ArrowType::Int32, true),
                ArrowField::new("extra", ArrowType::Int32, true),
            ]
            .into(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec![Some("x"), Some("y")])),
                Arc::new(arrow_array::Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(arrow_array::Int32Array::from(vec![Some(9), Some(8)])),
            ],
        );
        // Declared: list<struct<a int, b varchar>>.
        let declared_elem: arrow_schema::Fields = vec![
            ArrowField::new("a", ArrowType::Int32, true),
            ArrowField::new("b", ArrowType::Utf8, true),
        ]
        .into();
        let declared_field = ArrowField::new(
            "l",
            ArrowType::List(Arc::new(ArrowField::new(
                "element",
                ArrowType::Struct(declared_elem),
                true,
            ))),
            true,
        );

        let converted = Dummy.from_array(&declared_field, &file_array).unwrap();
        assert_eq!(
            converted.data_type(),
            RwType::list(RwType::Struct(StructType::new(vec![
                ("a", RwType::Int32),
                ("b", RwType::Varchar),
            ])))
        );
        let ArrayImpl::List(list) = &converted else {
            panic!("expected list array");
        };
        let ArrayImpl::Struct(elems) = list.values() else {
            panic!("expected struct elements");
        };
        assert_eq!(
            elems.value_at(0).unwrap().to_owned_scalar(),
            StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Utf8("x".into())),
            ])
        );
        assert_eq!(
            elems.value_at(1).unwrap().to_owned_scalar(),
            StructValue::new(vec![
                Some(ScalarImpl::Int32(2)),
                Some(ScalarImpl::Utf8("y".into())),
            ])
        );
    }

    #[test]
    fn test_struct_name_mismatch_same_arity_decodes_positionally() {
        // An external UDF may label struct children differently from the declared return
        // type; the correspondence defined by the signature check is positional.
        let actual_fields: arrow_schema::Fields = vec![
            ArrowField::new("total", ArrowType::Int32, true),
            ArrowField::new("count", ArrowType::Utf8, true),
        ]
        .into();
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::StructArray::new(
            actual_fields,
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![Some(42)])),
                Arc::new(arrow_array::StringArray::from(vec![Some("x")])),
            ],
            None,
        ));
        let declared_field = ArrowField::new(
            "s",
            ArrowType::Struct(
                vec![
                    ArrowField::new("sum", ArrowType::Int32, true),
                    ArrowField::new("cnt", ArrowType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let converted = Dummy.from_array(&declared_field, &array).unwrap();
        assert_eq!(
            converted.data_type(),
            RwType::Struct(StructType::new(vec![
                ("sum", RwType::Int32),
                ("cnt", RwType::Varchar),
            ]))
        );
        let ArrayImpl::Struct(structs) = &converted else {
            panic!("expected struct array");
        };
        assert_eq!(
            structs.value_at(0).unwrap().to_owned_scalar(),
            StructValue::new(vec![
                Some(ScalarImpl::Int32(42)),
                Some(ScalarImpl::Utf8("x".into())),
            ])
        );
    }

    #[test]
    fn test_struct_unalignable_fields_error() {
        // A declared name is missing and the arity differs: neither by-name nor positional
        // alignment applies.
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::StructArray::new(
            vec![ArrowField::new("a", ArrowType::Int32, true)].into(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![Some(1)]))],
            None,
        ));
        let declared_field = ArrowField::new(
            "s",
            ArrowType::Struct(
                vec![
                    ArrowField::new("a", ArrowType::Int32, true),
                    ArrowField::new("b", ArrowType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let err = Dummy.from_array(&declared_field, &array).unwrap_err();
        assert!(
            err.to_string()
                .contains("unable to align struct fields: expected [a, b], actual [a]"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_struct_child_type_divergence_stamped_honestly() {
        // Same child name, different type: the result must report the decoded child type,
        // not the expected one, so callers' boundary checks can see the divergence.
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::StructArray::new(
            vec![ArrowField::new("a", ArrowType::Utf8, true)].into(),
            vec![Arc::new(arrow_array::StringArray::from(vec![Some("oops")]))],
            None,
        ));
        let declared_field = ArrowField::new(
            "s",
            ArrowType::Struct(vec![ArrowField::new("a", ArrowType::Int64, true)].into()),
            true,
        );

        let converted = Dummy.from_array(&declared_field, &array).unwrap();
        assert_eq!(
            converted.data_type(),
            RwType::Struct(StructType::new(vec![("a", RwType::Varchar)]))
        );
    }

    #[test]
    fn test_struct_child_decodes_despite_lossy_expected_field() {
        // The parquet path renders a declared `struct<a smallint>` through the iceberg-lossy
        // to_arrow_field as struct<a: Int32>. A foreign file storing a genuine Int16 child
        // must still decode to Int16 with correct values instead of erroring.
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::StructArray::new(
            vec![ArrowField::new("a", ArrowType::Int16, true)].into(),
            vec![Arc::new(arrow_array::Int16Array::from(vec![
                Some(7),
                Some(-3),
            ]))],
            None,
        ));
        let lossy_expected = ArrowField::new(
            "s",
            ArrowType::Struct(vec![ArrowField::new("a", ArrowType::Int32, true)].into()),
            true,
        );

        let converted = Dummy.from_array(&lossy_expected, &array).unwrap();
        assert_eq!(
            converted.data_type(),
            RwType::Struct(StructType::new(vec![("a", RwType::Int16)]))
        );
        let ArrayImpl::Struct(structs) = &converted else {
            panic!("expected struct array");
        };
        assert_eq!(
            structs.value_at(0).unwrap().to_owned_scalar(),
            StructValue::new(vec![Some(ScalarImpl::Int16(7))])
        );
    }

    #[test]
    fn test_struct_duplicate_sibling_names_decode_first_occurrence() {
        // The by-name decode must consult the same child as the schema matcher (the first
        // occurrence), never a later duplicate of a different type.
        let actual_fields: arrow_schema::Fields = vec![
            ArrowField::new("a", ArrowType::Int32, true),
            ArrowField::new("a", ArrowType::Utf8, true),
            ArrowField::new("b", ArrowType::Utf8, true),
        ]
        .into();
        let array: arrow_array::ArrayRef = Arc::new(arrow_array::StructArray::new(
            actual_fields,
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![Some(1)])),
                Arc::new(arrow_array::StringArray::from(vec![Some("dup")])),
                Arc::new(arrow_array::StringArray::from(vec![Some("x")])),
            ],
            None,
        ));
        let declared_field = ArrowField::new(
            "s",
            ArrowType::Struct(
                vec![
                    ArrowField::new("a", ArrowType::Int32, true),
                    ArrowField::new("b", ArrowType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        let converted = Dummy.from_array(&declared_field, &array).unwrap();
        let ArrayImpl::Struct(structs) = &converted else {
            panic!("expected struct array");
        };
        assert_eq!(
            structs.value_at(0).unwrap().to_owned_scalar(),
            StructValue::new(vec![
                Some(ScalarImpl::Int32(1)),
                Some(ScalarImpl::Utf8("x".into())),
            ])
        );
    }

    #[test]
    fn test_extension_decode_follows_declared_field_under_list() {
        let json_meta: std::collections::HashMap<String, String> = [(
            "ARROW:extension:name".to_owned(),
            "arrowudf.json".to_owned(),
        )]
        .into();
        let strings: arrow_array::ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
            Some(r#"{"k":1}"#),
            Some("2"),
        ]));
        let make_list = |elem_field: ArrowField, values: arrow_array::ArrayRef| {
            Arc::new(arrow_array::ListArray::new(
                Arc::new(elem_field),
                arrow_buffer::OffsetBuffer::new(vec![0, 2].into()),
                values,
                None,
            )) as arrow_array::ArrayRef
        };
        let plain_elem = ArrowField::new("element", ArrowType::Utf8, true);
        let json_elem = plain_elem.clone().with_metadata(json_meta);

        // A file-side extension is ignored when the declared element is plain varchar.
        let file_json = make_list(json_elem.clone(), strings.clone());
        let declared_plain =
            ArrowField::new("l", ArrowType::List(Arc::new(plain_elem.clone())), true);
        let converted = Dummy.from_array(&declared_plain, &file_json).unwrap();
        assert_eq!(converted.data_type(), RwType::list(RwType::Varchar));

        // A declared-side extension drives the decode even when the file element is plain.
        let file_plain = make_list(plain_elem, strings);
        let declared_json = ArrowField::new("l", ArrowType::List(Arc::new(json_elem)), true);
        let converted = Dummy.from_array(&declared_json, &file_plain).unwrap();
        assert_eq!(converted.data_type(), RwType::list(RwType::Jsonb));
    }

    #[test]
    fn test_map_value_struct_decodes_by_declared_field() {
        // File: map<utf8, struct<y int32, x int32>>; declared value struct is the subset
        // struct<x int32>.
        let value_fields: arrow_schema::Fields = vec![
            ArrowField::new("y", ArrowType::Int32, true),
            ArrowField::new("x", ArrowType::Int32, true),
        ]
        .into();
        let entries_fields: arrow_schema::Fields = vec![
            ArrowField::new("key", ArrowType::Utf8, false),
            ArrowField::new("value", ArrowType::Struct(value_fields.clone()), true),
        ]
        .into();
        let value_struct = arrow_array::StructArray::new(
            value_fields,
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![Some(7)])),
                Arc::new(arrow_array::Int32Array::from(vec![Some(42)])),
            ],
            None,
        );
        let entries = arrow_array::StructArray::new(
            entries_fields.clone(),
            vec![
                Arc::new(arrow_array::StringArray::from(vec![Some("k")])),
                Arc::new(value_struct),
            ],
            None,
        );
        let file_map: arrow_array::ArrayRef = Arc::new(arrow_array::MapArray::new(
            Arc::new(ArrowField::new(
                "entries",
                ArrowType::Struct(entries_fields),
                false,
            )),
            arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));

        let declared_value =
            ArrowType::Struct(vec![ArrowField::new("x", ArrowType::Int32, true)].into());
        let declared_field = ArrowField::new(
            "m",
            ArrowType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowType::Struct(
                        vec![
                            ArrowField::new("key", ArrowType::Utf8, false),
                            ArrowField::new("value", declared_value, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );

        let converted = Dummy.from_array(&declared_field, &file_map).unwrap();
        assert_eq!(
            converted.data_type(),
            RwType::Map(MapType::from_kv(
                RwType::Varchar,
                RwType::Struct(StructType::new(vec![("x", RwType::Int32)])),
            ))
        );
        let ArrayImpl::Map(map) = &converted else {
            panic!("expected map array");
        };
        let ArrayImpl::Struct(entries) = map.inner.values() else {
            panic!("expected struct entries");
        };
        assert_eq!(
            entries.value_at(0).unwrap().to_owned_scalar(),
            StructValue::new(vec![
                Some(ScalarImpl::Utf8("k".into())),
                Some(ScalarImpl::Struct(StructValue::new(vec![Some(
                    ScalarImpl::Int32(42)
                )]))),
            ])
        );
    }

    #[test]
    fn test_list_schema_match() {
        // Arrow: list<double>
        let arrow_list =
            ArrowType::List(Box::new(ArrowField::new("item", ArrowType::Float64, true)).into());
        // RW: list<double>
        let rw_list = RwType::Float64.list();
        assert!(is_parquet_schema_match_source_schema(&arrow_list, &rw_list));

        let rw_list2 = RwType::Int32.list();
        assert!(!is_parquet_schema_match_source_schema(
            &arrow_list,
            &rw_list2
        ));
    }

    #[test]
    fn test_map_schema_match() {
        // Arrow: map<utf8, int32>
        let arrow_map = ArrowType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowType::Struct(
                    vec![
                        ArrowField::new("key", ArrowType::Utf8, false),
                        ArrowField::new("value", ArrowType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        // RW: map<varchar, int32>
        let rw_map = RwType::Map(MapType::from_kv(RwType::Varchar, RwType::Int32));
        assert!(is_parquet_schema_match_source_schema(&arrow_map, &rw_map));

        // Key type does not match
        let rw_map2 = RwType::Map(MapType::from_kv(RwType::Int32, RwType::Int32));
        assert!(!is_parquet_schema_match_source_schema(&arrow_map, &rw_map2));

        // Value type does not match
        let rw_map3 = RwType::Map(MapType::from_kv(RwType::Varchar, RwType::Float64));
        assert!(!is_parquet_schema_match_source_schema(&arrow_map, &rw_map3));

        // Arrow inner struct field name does not match
        let arrow_map2 = ArrowType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowType::Struct(
                    vec![
                        ArrowField::new("k", ArrowType::Utf8, false),
                        ArrowField::new("value", ArrowType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(!is_parquet_schema_match_source_schema(&arrow_map2, &rw_map));
    }

    #[test]
    fn bool() {
        let array = BoolArray::from_iter([None, Some(false), Some(true)]);
        let arrow = arrow_array::BooleanArray::from(&array);
        assert_eq!(BoolArray::from(&arrow), array);
    }

    #[test]
    fn i16() {
        let array = I16Array::from_iter([None, Some(-7), Some(25)]);
        let arrow = arrow_array::Int16Array::from(&array);
        assert_eq!(I16Array::from(&arrow), array);
    }

    #[test]
    fn i32() {
        let array = I32Array::from_iter([None, Some(-7), Some(25)]);
        let arrow = arrow_array::Int32Array::from(&array);
        assert_eq!(I32Array::from(&arrow), array);
    }

    #[test]
    fn i64() {
        let array = I64Array::from_iter([None, Some(-7), Some(25)]);
        let arrow = arrow_array::Int64Array::from(&array);
        assert_eq!(I64Array::from(&arrow), array);
    }

    #[test]
    fn f32() {
        let array = F32Array::from_iter([None, Some(-7.0), Some(25.0)]);
        let arrow = arrow_array::Float32Array::from(&array);
        assert_eq!(F32Array::from(&arrow), array);
    }

    #[test]
    fn f64() {
        let array = F64Array::from_iter([None, Some(-7.0), Some(25.0)]);
        let arrow = arrow_array::Float64Array::from(&array);
        assert_eq!(F64Array::from(&arrow), array);
    }

    #[test]
    fn int8() {
        let array: PrimitiveArray<i16> = I16Array::from_iter([None, Some(-128), Some(127)]);
        let arr = arrow_array::Int8Array::from(vec![None, Some(-128), Some(127)]);
        let converted: PrimitiveArray<i16> = (&arr).into();
        assert_eq!(converted, array);
    }

    #[test]
    fn uint8() {
        let array: PrimitiveArray<i16> = I16Array::from_iter([None, Some(7), Some(25)]);
        let arr = arrow_array::UInt8Array::from(vec![None, Some(7), Some(25)]);
        let converted: PrimitiveArray<i16> = (&arr).into();
        assert_eq!(converted, array);
    }

    #[test]
    fn uint16() {
        let array: PrimitiveArray<i32> = I32Array::from_iter([None, Some(7), Some(65535)]);
        let arr = arrow_array::UInt16Array::from(vec![None, Some(7), Some(65535)]);
        let converted: PrimitiveArray<i32> = (&arr).into();
        assert_eq!(converted, array);
    }

    #[test]
    fn uint32() {
        let array: PrimitiveArray<i64> = I64Array::from_iter([None, Some(7), Some(4294967295)]);
        let arr = arrow_array::UInt32Array::from(vec![None, Some(7), Some(4294967295)]);
        let converted: PrimitiveArray<i64> = (&arr).into();
        assert_eq!(converted, array);
    }

    #[test]
    fn uint64() {
        let array: PrimitiveArray<Decimal> = DecimalArray::from_iter([
            None,
            Some(Decimal::Normalized("7".parse().unwrap())),
            Some(Decimal::Normalized("18446744073709551615".parse().unwrap())),
        ]);
        let arr = arrow_array::UInt64Array::from(vec![None, Some(7), Some(18446744073709551615)]);
        let converted: PrimitiveArray<Decimal> = (&arr).try_into().unwrap();
        assert_eq!(converted, array);
    }

    #[test]
    fn date() {
        let array = DateArray::from_iter([
            None,
            Date::with_days_since_ce(12345).ok(),
            Date::with_days_since_ce(-12345).ok(),
        ]);
        let arrow = arrow_array::Date32Array::from(&array);
        assert_eq!(DateArray::from(&arrow), array);
    }

    #[test]
    fn time() {
        let array = TimeArray::from_iter([None, Time::with_micro(24 * 3600 * 1_000_000 - 1).ok()]);
        let arrow = arrow_array::Time64MicrosecondArray::from(&array);
        assert_eq!(TimeArray::from(&arrow), array);
    }

    #[test]
    fn timestamp() {
        let array =
            TimestampArray::from_iter([None, Timestamp::with_micros(123456789012345678).ok()]);
        let arrow = arrow_array::TimestampMicrosecondArray::from(&array);
        assert_eq!(TimestampArray::from(&arrow), array);
    }

    #[test]
    fn interval() {
        let array = IntervalArray::from_iter([
            None,
            Some(Interval::from_month_day_usec(
                1_000_000,
                1_000,
                1_000_000_000,
            )),
            Some(Interval::from_month_day_usec(
                -1_000_000,
                -1_000,
                -1_000_000_000,
            )),
        ]);
        let arrow = arrow_array::IntervalMonthDayNanoArray::from(&array);
        assert_eq!(IntervalArray::from(&arrow), array);
    }

    #[test]
    fn string() {
        let array = Utf8Array::from_iter([None, Some("array"), Some("arrow")]);
        let arrow = arrow_array::StringArray::from(&array);
        assert_eq!(Utf8Array::from(&arrow), array);
    }

    #[test]
    fn binary() {
        let array = BytesArray::from_iter([None, Some("array".as_bytes())]);
        let arrow = arrow_array::BinaryArray::from(&array);
        assert_eq!(BytesArray::from(&arrow), array);
    }

    #[test]
    fn fixed_size_binary() {
        let uuid = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
            0xde, 0xf0,
        ];
        let arrow_array = arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [None, Some(uuid)].into_iter(),
            16,
        )
        .unwrap();
        let field =
            arrow_schema::Field::new("u", arrow_schema::DataType::FixedSizeBinary(16), true);

        assert_eq!(Dummy.from_field(&field).unwrap(), DataType::Bytea);

        let rw_array = Dummy
            .from_array(&field, &(Arc::new(arrow_array) as arrow_array::ArrayRef))
            .unwrap();
        let expected = BytesArray::from_iter([None, Some(uuid.as_slice())]);
        assert_eq!(rw_array.as_bytea(), &expected);
    }

    #[test]
    fn decimal() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let arrow = arrow_array::LargeBinaryArray::from(&array);
        assert_eq!(DecimalArray::try_from(&arrow).unwrap(), array);

        let arrow = arrow_array::StringArray::from(&array);
        assert_eq!(DecimalArray::try_from(&arrow).unwrap(), array);
    }

    #[test]
    fn jsonb() {
        let array = JsonbArray::from_iter([
            None,
            Some("null".parse().unwrap()),
            Some("false".parse().unwrap()),
            Some("1".parse().unwrap()),
            Some("[1, 2, 3]".parse().unwrap()),
            Some(r#"{ "a": 1, "b": null }"#.parse().unwrap()),
        ]);
        let arrow = arrow_array::LargeStringArray::from(&array);
        assert_eq!(JsonbArray::try_from(&arrow).unwrap(), array);

        let arrow = arrow_array::StringArray::from(&array);
        assert_eq!(JsonbArray::try_from(&arrow).unwrap(), array);
    }

    #[test]
    fn int256() {
        let values = [
            None,
            Some(Int256::from(1)),
            Some(Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(Int256::from(i64::MAX) * Int256::from(i64::MAX) * Int256::from(i64::MAX)),
            Some(
                Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX)
                    * Int256::from(i64::MAX),
            ),
            Some(Int256::min_value()),
            Some(Int256::max_value()),
        ];

        let array =
            Int256Array::from_iter(values.iter().map(|r| r.as_ref().map(|x| x.as_scalar_ref())));
        let arrow = arrow_array::Decimal256Array::from(&array);
        assert_eq!(Int256Array::from(&arrow), array);
    }
}
