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

//! Data file naming for pk-index sinks that do not use the legacy file path layout.
//!
//! Every data file written by the sink writer or the pk-index compaction is placed at
//! `{data_location}/{uuid}.parquet`, so the PK index only needs to store the 16-byte UUID and
//! can rebuild the full path from it.

use anyhow::anyhow;
use iceberg::spec::{DataFileFormat, PartitionKey, TableMetadata};
use iceberg::writer::file_writer::location_generator::{
    DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use uuid::Uuid;

use crate::sink::{Result, SinkError};

const PK_INDEX_DATA_FILE_SUFFIX: &str = ".parquet";

/// Maps between data file UUIDs and data file paths of a pk-index sink table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkIndexDataFileLocator {
    data_location: String,
}

impl PkIndexDataFileLocator {
    pub fn new(table_metadata: &TableMetadata) -> Result<Self> {
        // Resolve the data location exactly as the default generator does, so the iceberg-compaction
        // writer, which places files with `DefaultLocationGenerator`, agrees with this locator.
        let location = DefaultLocationGenerator::new(table_metadata)
            .map_err(|err| SinkError::Iceberg(anyhow!(err)))?
            .generate_location(None, "");
        Ok(Self::with_data_location(location))
    }

    pub fn with_data_location(data_location: impl Into<String>) -> Self {
        let mut data_location = data_location.into();
        while data_location.ends_with('/') {
            data_location.pop();
        }
        Self { data_location }
    }

    pub fn file_path(&self, uuid: Uuid) -> String {
        format!(
            "{}/{}{}",
            self.data_location,
            uuid.hyphenated(),
            PK_INDEX_DATA_FILE_SUFFIX
        )
    }

    pub fn file_path_from_bytes(&self, uuid: &[u8]) -> Result<String> {
        let uuid = Uuid::from_slice(uuid).map_err(|err| {
            SinkError::Iceberg(anyhow!(err).context("invalid pk-index data file uuid"))
        })?;
        Ok(self.file_path(uuid))
    }

    /// Extracts the UUID from a data file path. Fails if the path is not exactly the one
    /// [`Self::file_path`] would produce, since the full path could not be rebuilt otherwise.
    pub fn file_uuid(&self, path: &str) -> Result<Uuid> {
        path.strip_prefix(self.data_location.as_str())
            .and_then(|name| name.strip_prefix('/'))
            .and_then(|name| name.strip_suffix(PK_INDEX_DATA_FILE_SUFFIX))
            .and_then(|uuid| Uuid::try_parse(uuid).ok())
            .filter(|uuid| self.file_path(*uuid) == path)
            .ok_or_else(|| {
                SinkError::Iceberg(anyhow!(
                    "data file path {path} does not match the pk-index layout `{}/{{uuid}}{}`",
                    self.data_location,
                    PK_INDEX_DATA_FILE_SUFFIX
                ))
            })
    }
}

/// Places every data file directly under the data location, without partition directories.
impl LocationGenerator for PkIndexDataFileLocator {
    fn generate_location(&self, _partition_key: Option<&PartitionKey>, file_name: &str) -> String {
        format!("{}/{}", self.data_location, file_name)
    }
}

/// Names every data file `{uuid}.parquet` with a fresh UUID v7.
#[derive(Clone, Debug, Default)]
pub struct PkIndexDataFileNameGenerator;

impl FileNameGenerator for PkIndexDataFileNameGenerator {
    fn generate_file_name(&self) -> String {
        debug_assert_eq!(
            PK_INDEX_DATA_FILE_SUFFIX,
            format!(".{}", DataFileFormat::Parquet)
        );
        format!(
            "{}{}",
            Uuid::now_v7().hyphenated(),
            PK_INDEX_DATA_FILE_SUFFIX
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_path_round_trip() {
        let locator = PkIndexDataFileLocator::with_data_location("s3://bucket/db/t/data/");
        let file_name = PkIndexDataFileNameGenerator.generate_file_name();
        let path = locator.generate_location(None, &file_name);
        assert!(path.starts_with("s3://bucket/db/t/data/"));
        assert!(path.ends_with(".parquet"));

        let uuid = locator.file_uuid(&path).unwrap();
        assert_eq!(uuid.get_version_num(), 7);
        assert_eq!(locator.file_path(uuid), path);
        assert_eq!(locator.file_path_from_bytes(uuid.as_bytes()).unwrap(), path);
    }

    #[test]
    fn test_file_uuid_rejects_other_layouts() {
        let locator = PkIndexDataFileLocator::with_data_location("s3://bucket/t/data");
        let uuid = Uuid::now_v7();
        for path in [
            format!("s3://bucket/t/data/p=1/{uuid}.parquet"),
            format!("s3://bucket/other/data/{uuid}.parquet"),
            format!("s3://bucket/t/data/{}.parquet", uuid.simple()),
            format!("s3://bucket/t/data/{uuid}.orc"),
            "s3://bucket/t/data/1-00000-0193.parquet".to_owned(),
        ] {
            assert!(locator.file_uuid(&path).is_err(), "{path}");
        }
        assert!(locator.file_path_from_bytes(&[1, 2, 3]).is_err());
    }
}
