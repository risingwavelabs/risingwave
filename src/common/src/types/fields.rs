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
use super::DataType;

/// A struct can implements `Fields` when if can be represented as a relational Row.
///
/// Can be automatically derived with [`#[derive(Fields)]`](derive@super::Fields).
pub trait Fields {
    /// When the struct being converted to an [`Row`](crate::row::Row) or a [`DataChunk`](crate::array::DataChunk), it schema must be consistent with the `fields` call.
    fn fields() -> Vec<(&'static str, DataType)>;
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::types::{Fields, StructType, Timestamp, Timestamptz, F32};

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
            v10: std::option::Option<Vec<Option<f32>>>,
            v11: Box<Timestamp>,
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
