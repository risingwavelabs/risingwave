// Copyright 2026 RisingWave Labs
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

use anyhow::Context;
use apache_avro::Schema;
use risingwave_common::bail;
use serde::Deserialize;

use crate::error::ConnectorResult;
use crate::source::pulsar::topic::{Topic, parse_topic};

#[derive(Debug, Deserialize)]
pub struct PulsarSchemaInfo {
    pub version: i64,
    pub r#type: String,
    pub data: String,
}

/// A Pulsar schema by type. Pulsar stores both `AVRO` and `JSON` schemas as Avro record
/// definitions, but their payloads are Avro binary and plain JSON respectively.
#[derive(Debug)]
pub enum PulsarSchema {
    Avro(Schema),
    Json(Schema),
}

impl PulsarSchemaInfo {
    /// Parses the schema definition according to its type.
    pub fn parse(&self) -> ConnectorResult<PulsarSchema> {
        let wrap = match self.r#type.to_ascii_uppercase().as_str() {
            "AVRO" => PulsarSchema::Avro,
            "JSON" => PulsarSchema::Json,
            _ => bail!("unsupported Pulsar schema type {}", self.r#type),
        };
        let schema = Schema::parse_str(&self.data)
            .with_context(|| format!("failed to parse Pulsar {} schema", self.r#type))?;
        Ok(wrap(schema))
    }
}

/// Returns the partitioned topic, i.e. the topic without partition suffix, as schemas are shared
/// by all partitions.
pub fn parse_partitioned_topic(topic: &str) -> ConnectorResult<Topic> {
    let mut topic = parse_topic(topic)?;
    topic.topic = topic.topic_str_without_partition()?;
    topic.partition_index = None;
    Ok(topic)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PulsarSchemaVersion(pub i64);

impl TryFrom<&[u8]> for PulsarSchemaVersion {
    type Error = crate::error::ConnectorError;

    fn try_from(version: &[u8]) -> Result<Self, Self::Error> {
        let bytes: [u8; 8] = version.try_into().with_context(|| {
            format!(
                "expected 8-byte Pulsar LongSchemaVersion, got {} bytes",
                version.len()
            )
        })?;
        Ok(Self(i64::from_be_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pulsar_schema_info_deserializes_type() {
        let schema: PulsarSchemaInfo =
            serde_json::from_str(r#"{"version":0,"type":"AVRO","data":"{}"}"#).unwrap();
        assert_eq!(schema.r#type, "AVRO");
    }

    #[test]
    fn json_schema_definition_is_parsed_as_avro() {
        let schema: PulsarSchemaInfo = serde_json::from_value(serde_json::json!({
            "version": 0,
            "type": "JSON",
            "data": r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}"#,
        }))
        .unwrap();
        assert!(matches!(
            schema.parse().unwrap(),
            PulsarSchema::Json(Schema::Record(_))
        ));
    }

    #[test]
    fn unsupported_schema_type_is_rejected() {
        let schema: PulsarSchemaInfo =
            serde_json::from_str(r#"{"version":0,"type":"PROTOBUF","data":"{}"}"#).unwrap();
        assert_eq!(
            schema.parse().unwrap_err().to_string(),
            "unsupported Pulsar schema type PROTOBUF"
        );
    }

    #[test]
    fn pulsar_schema_version_is_signed_big_endian() {
        assert_eq!(
            PulsarSchemaVersion::try_from(1_i64.to_be_bytes().as_slice())
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            PulsarSchemaVersion::try_from((-1_i64).to_be_bytes().as_slice())
                .unwrap()
                .0,
            -1
        );
    }

    #[test]
    fn pulsar_schema_version_requires_exactly_eight_bytes() {
        assert!(PulsarSchemaVersion::try_from([].as_slice()).is_err());
        assert!(PulsarSchemaVersion::try_from([1, 2, 3].as_slice()).is_err());
        assert!(PulsarSchemaVersion::try_from([0; 9].as_slice()).is_err());
    }
}
