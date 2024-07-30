// Copyright 2024 RisingWave Labs
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

use std::fmt::Formatter;

use anyhow::Context;

use super::*;

/// Refer to [`super::super::array::MapArray`] for the invariants of a map value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MapType(Box<(DataType, DataType)>);

impl From<MapType> for DataType {
    fn from(value: MapType) -> Self {
        DataType::Map(value)
    }
}

impl MapType {
    /// # Panics
    /// Panics if the key type is not valid for a map.
    pub fn from_kv(key: DataType, value: DataType) -> Self {
        Self::check_key_type_valid(&key).unwrap();
        Self(Box::new((key, value)))
    }

    pub fn try_from_kv(key: DataType, value: DataType) -> Result<Self, anyhow::Error> {
        Self::check_key_type_valid(&key)?;
        Ok(Self(Box::new((key, value))))
    }

    /// # Panics
    /// Panics if the key type is not valid for a map, or the
    /// entries type is not a valid struct type.
    pub fn from_list_entries(list_entries_type: DataType) -> Self {
        let struct_type = list_entries_type.as_struct();
        let (k, v) = struct_type
            .iter()
            .collect_tuple()
            .expect("the underlying struct for map must have exactly two fields");
        if cfg!(debug_assertions) {
            // the field names are not strictly enforced
            itertools::assert_equal(struct_type.names(), ["key", "value"]);
        }
        Self::from_kv(k.1.clone(), v.1.clone())
    }

    /// # Panics
    /// Panics if the key type is not valid for a map.
    pub fn struct_type_for_map(key_type: DataType, value_type: DataType) -> StructType {
        MapType::check_key_type_valid(&key_type).unwrap();
        StructType::new(vec![("key", key_type), ("value", value_type)])
    }

    pub fn key(&self) -> &DataType {
        &self.0 .0
    }

    pub fn value(&self) -> &DataType {
        &self.0 .1
    }

    pub fn into_struct(self) -> StructType {
        let (key, value) = *self.0;
        Self::struct_type_for_map(key, value)
    }

    pub fn into_list(self) -> DataType {
        DataType::List(Box::new(DataType::Struct(self.into_struct())))
    }

    /// String and integral types are allowed.
    ///
    /// This is similar to [Protobuf](https://protobuf.dev/programming-guides/proto3/#maps)'s
    /// decision.
    ///
    /// Note that this isn't definitive.
    /// Just be conservative at the beginning, but not too restrictive (like only allowing strings).
    pub fn check_key_type_valid(data_type: &DataType) -> anyhow::Result<()> {
        let ok = match data_type {
            DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
            DataType::Varchar => true,
            DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Timestamptz
            | DataType::Interval
            | DataType::Struct(_)
            | DataType::List(_)
            | DataType::Bytea
            | DataType::Jsonb
            | DataType::Serial
            | DataType::Int256
            | DataType::Map(_) => false,
        };
        if !ok {
            Err(anyhow::anyhow!("invalid map key type: {data_type}"))
        } else {
            Ok(())
        }
    }
}

impl FromStr for MapType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !(s.starts_with("map(") && s.ends_with(')')) {
            return Err(anyhow::anyhow!("expect map(...,...)"));
        };
        if let Some((key, value)) = s[4..s.len() - 1].split(',').collect_tuple() {
            let key = key.parse().context("failed to parse map key type")?;
            let value = value.parse().context("failed to parse map value type")?;
            MapType::try_from_kv(key, value)
        } else {
            Err(anyhow::anyhow!("expect map(...,...)"))
        }
    }
}

impl std::fmt::Display for MapType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "map({},{})", self.key(), self.value())
    }
}
