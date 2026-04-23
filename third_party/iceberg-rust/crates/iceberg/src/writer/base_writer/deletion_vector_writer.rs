// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Deletion vector writer for Iceberg V3.

use std::collections::HashMap;

use crate::delete_vector::{
    DELETION_VECTOR_PROPERTY_CARDINALITY, DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE,
    DeleteVector,
};
use crate::io::FileIO;
use crate::puffin::{CompressionCodec, PuffinWriter};
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey, Struct,
};
use crate::writer::base_writer::position_delete_file_writer::PositionDeleteInput;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `DeletionVectorWriter`.
#[derive(Clone)]
pub struct DeletionVectorWriterBuilder<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
}

impl<L: LocationGenerator, F: FileNameGenerator> DeletionVectorWriterBuilder<L, F> {
    /// Create a new `DeletionVectorWriterBuilder`.
    pub fn new(file_io: FileIO, location_generator: L, file_name_generator: F) -> Self {
        Self {
            file_io,
            location_generator,
            file_name_generator,
        }
    }
}

#[async_trait::async_trait]
impl<L, F> IcebergWriterBuilder<Vec<PositionDeleteInput>> for DeletionVectorWriterBuilder<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = DeletionVectorWriter<L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(DeletionVectorWriter {
            file_io: self.file_io.clone(),
            location_generator: self.location_generator.clone(),
            file_name_generator: self.file_name_generator.clone(),
            partition_key,
            delete_vectors: HashMap::new(),
        })
    }
}

/// Writer that collects position deletes and emits deletion vectors as puffin files.
pub struct DeletionVectorWriter<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
    partition_key: Option<PartitionKey>,
    delete_vectors: HashMap<String, DeleteVector>,
}

#[async_trait::async_trait]
impl<L, F> IcebergWriter<Vec<PositionDeleteInput>> for DeletionVectorWriter<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> Result<()> {
        for delete in input {
            let pos = u64::try_from(delete.pos).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector position must be non-negative".to_string(),
                )
            })?;
            let entry = self
                .delete_vectors
                .entry(delete.path.as_ref().to_string())
                .or_default();
            entry.insert(pos);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if self.delete_vectors.is_empty() {
            return Ok(Vec::new());
        }

        let partition_key = self.partition_key.as_ref();
        let delete_vectors = std::mem::take(&mut self.delete_vectors);
        let mut data_files = Vec::with_capacity(delete_vectors.len());

        for (data_file_path, delete_vector) in delete_vectors {
            if delete_vector.is_empty() {
                continue;
            }
            let file_name = self.file_name_generator.generate_file_name();
            let location = self
                .location_generator
                .generate_location(partition_key, &file_name);
            let output_file = self.file_io.new_output(&location)?;
            let mut writer = PuffinWriter::new(&output_file, HashMap::new(), false).await?;

            let cardinality = delete_vector.len();
            let properties = HashMap::from([
                (
                    DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                    cardinality.to_string(),
                ),
                (
                    DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                    data_file_path.clone(),
                ),
            ]);
            let blob = delete_vector.to_puffin_blob(properties)?;
            writer.add(blob, CompressionCodec::None).await?;

            let result = writer.close_with_metadata().await?;
            let blob_metadata = result
                .blobs_metadata
                .first()
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "puffin metadata is empty"))?;

            let mut builder = DataFileBuilder::default();
            builder
                .content(DataContentType::PositionDeletes)
                .file_path(location)
                .file_format(DataFileFormat::Puffin)
                .partition(partition_key.map_or_else(Struct::empty, |pk| pk.data().clone()))
                .partition_spec_id(partition_key.map_or(0, |pk| pk.spec().spec_id()))
                .record_count(cardinality)
                .file_size_in_bytes(result.file_size_in_bytes)
                .referenced_data_file(Some(data_file_path))
                .content_offset(Some(blob_metadata.offset() as i64))
                .content_size_in_bytes(Some(blob_metadata.length() as i64));

            data_files.push(builder.build().map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to build deletion vector file: {err}"),
                )
            })?);
        }

        Ok(data_files)
    }
}
