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

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PulsarSchema {
    pub version: i64,
    #[serde(rename = "type")]
    pub r#type: String,
    pub data: String,
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
    fn test_pulsar_schema_version_from_bytes() {
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
        assert!(PulsarSchemaVersion::try_from([1, 2, 3].as_slice()).is_err());
    }
}
