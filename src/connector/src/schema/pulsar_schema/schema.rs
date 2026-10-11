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

impl PulsarSchemaInfo {
    /// Parses the schema definition, which Pulsar stores as an Avro record for both `AVRO` and
    /// `JSON` schemas.
    pub fn parse_avro_definition(&self, expected_type: &str) -> ConnectorResult<Schema> {
        if !self.r#type.eq_ignore_ascii_case(expected_type) {
            bail!(
                "expected Pulsar {expected_type} schema, got {}",
                self.r#type
            );
        }
        let schema = Schema::parse_str(&self.data)
            .with_context(|| format!("failed to parse Pulsar {expected_type} schema"))?;
        Ok(schema)
    }
}

/// Returns the topic without partition suffix, as schemas are shared by all partitions.
pub fn parse_non_partitioned_topic(topic: &str) -> ConnectorResult<Topic> {
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
            schema.parse_avro_definition("JSON").unwrap(),
            Schema::Record(_)
        ));
        assert_eq!(
            schema
                .parse_avro_definition("AVRO")
                .unwrap_err()
                .to_string(),
            "expected Pulsar AVRO schema, got JSON"
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
