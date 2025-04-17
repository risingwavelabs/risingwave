// Copyright 2025 RisingWave Labs
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
use super::DataType;
use crate::row::OwnedRow;
use crate::util::chunk_coalesce::DataChunkBuilder;

/// A struct can implements `Fields` when if can be represented as a relational Row.
///
/// # Derivable
///
/// This trait can be automatically derived with [`#[derive(Fields)]`](derive@super::Fields).
/// Type of the fields must implement [`WithDataType`](super::WithDataType) and [`ToOwnedDatum`](super::ToOwnedDatum).
///
/// ```
/// # use risingwave_common::types::Fields;
///
/// #[derive(Fields)]
/// struct Data {
///     v1: i16,
///     v2: i32,
/// }
/// ```
///
/// You can add `#[primary_key]` attribute to one of the fields to specify the primary key of the table.
///
/// ```
/// # use risingwave_common::types::Fields;
///
/// #[derive(Fields)]
/// struct Data {
///     #[primary_key]
///     v1: i16,
///     v2: i32,
/// }
/// ```
///
/// If the primary key is composite, you can add `#[primary_key(...)]` attribute to the struct to specify the order of the fields.
///
/// ```
/// # use risingwave_common::types::Fields;
///
/// #[derive(Fields)]
/// #[primary_key(v2, v1)]
/// struct Data {
///     v1: i16,
///     v2: i32,
/// }
/// ```
pub trait Fields {
    /// The primary key of the table.
    ///
    /// - `None` if the primary key is not applicable.
    /// - `Some(&[])` if the primary key is empty, i.e., there'll be at most one row in the table.
    const PRIMARY_KEY: Option<&'static [usize]>;

    /// Return the schema of the struct.
    fn fields() -> Vec<(&'static str, DataType)>;

    /// Convert the struct to an `OwnedRow`.
    fn into_owned_row(self) -> OwnedRow;

    /// Create a [`DataChunkBuilder`](crate::util::chunk_coalesce::DataChunkBuilder) with the schema of the struct.
    fn data_chunk_builder(capacity: usize) -> DataChunkBuilder {
        DataChunkBuilder::new(
            Self::fields().into_iter().map(|(_, ty)| ty).collect(),
            capacity,
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::types::{F32, Fields, StructType, Timestamp, Timestamptz};

    #[test]
    #[allow(dead_code)]
    fn test_macro() {
        #[derive(Fields)]
        struct Sub {
            v12: Timestamptz,
            v13: Bytes,
        }

        #[derive(Fields)]
        struct Data {
            v1: i16,
            v2: std::primitive::i32,
            v3: bool,
            v4: f32,
            v5: F32,
            v6: Option<f64>,
            v7: Vec<u8>,
            v8: std::vec::Vec<i16>,
            v9: Option<Vec<i64>>,
            v10: std::option::Option<Vec<Option<F32>>>,
            v11: Timestamp,
            v14: Sub,
        }

        assert_eq!(
            Data::fields(),
            vec![
                ("v1", DataType::Int16),
                ("v2", DataType::Int32),
                ("v3", DataType::Boolean),
                ("v4", DataType::Float32),
                ("v5", DataType::Float32),
                ("v6", DataType::Float64),
                ("v7", DataType::Bytea),
                ("v8", DataType::List(Box::new(DataType::Int16))),
                ("v9", DataType::List(Box::new(DataType::Int64))),
                ("v10", DataType::List(Box::new(DataType::Float32))),
                ("v11", DataType::Timestamp),
                (
                    "v14",
                    DataType::Struct(StructType::new(vec![
                        ("v12", DataType::Timestamptz),
                        ("v13", DataType::Bytea)
                    ]))
                )
            ]
        )
    }
}
