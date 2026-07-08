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

//! Encoding and decoding between external data formats and RisingWave datum (i.e., type mappings).

#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(error_generic_member_access)]
#![feature(register_tool)]
#![register_tool(rw)]
#![recursion_limit = "256"]

pub mod common;
/// Converts JSON/AVRO/Protobuf data to RisingWave datum.
/// The core API is [`decoder::Access`].
pub mod decoder;

pub use apache_avro::schema::Schema as AvroSchema;
pub use apache_avro::types::{Value as AvroValue, ValueKind as AvroValueKind};
pub use risingwave_pb::plan_common::ColumnDesc;
pub struct JsonSchema(pub serde_json::Value);
impl JsonSchema {
    pub fn parse_str(schema: &str) -> anyhow::Result<Self> {
        use anyhow::Context;

        let value = serde_json::from_str(schema).context("failed to parse json schema")?;
        Ok(Self(value))
    }

    pub fn parse_bytes(schema: &[u8]) -> anyhow::Result<Self> {
        use anyhow::Context;

        let value = serde_json::from_slice(schema).context("failed to parse json schema")?;
        Ok(Self(value))
    }
}
