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

//! High-level Iceberg writer that coordinates partition routing for `RecordBatch` input.
//!
//! `TaskWriter` sits on top of the generic writer abstractions and provides a convenient entry
//! point for users that start from Arrow `RecordBatch` values. It lazily constructs the
//! appropriate partitioning writer (unpartitioned, fanout, or clustered) and routes batches to it.
//!
//! # Example
//!
//! ```rust,ignore
//! use iceberg::spec::{PartitionSpec, Schema};
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
//! use iceberg::writer::task_writer::TaskWriter;
//! use parquet::file::properties::WriterProperties;
//!
//! # async fn build_task_writer(
//! #     data_file_writer_builder:
//! #         DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>,
//! #     schema: Schema,
//! #     partition_spec: PartitionSpec,
//! # ) -> iceberg::Result<()> {
//! let mut task_writer = TaskWriter::new(
//!     data_file_writer_builder,
//!     false, // fanout_enabled
//!     Schema::from(schema).into(),
//!     PartitionSpec::from(partition_spec).into(),
//! );
//!
//! // task_writer.write(record_batch).await?;
//! // let data_files = task_writer.close().await?;
//! # Ok(())
//! # }
//! ```

use arrow_array::RecordBatch;

use crate::Result;
use crate::arrow::RecordBatchPartitionSplitter;
use crate::spec::{DataFile, PartitionSpecRef, SchemaRef};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::partitioning::clustered_writer::ClusteredWriter;
use crate::writer::partitioning::fanout_writer::FanoutWriter;
use crate::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
use crate::writer::{IcebergWriter, IcebergWriterBuilder};

/// High-level writer that handles partitioning and routing of `RecordBatch` data to Iceberg tables.
pub struct TaskWriter<B: IcebergWriterBuilder> {
    /// The underlying writer (unpartitioned, fanout, or clustered)
    writer: Option<SupportedWriter<B>>,
    /// Lazily initialized partition splitter for partitioned tables
    partition_splitter: Option<RecordBatchPartitionSplitter>,
    /// Iceberg schema reference used for partition splitting
    schema: SchemaRef,
    /// Partition specification reference used for partition splitting
    partition_spec: PartitionSpecRef,
}

/// Internal enum holding the writer implementation for each partitioning strategy.
enum SupportedWriter<B: IcebergWriterBuilder> {
    /// Writer for unpartitioned tables
    Unpartitioned(UnpartitionedWriter<B>),
    /// Writer for partitioned tables with unsorted data (maintains multiple active writers)
    Fanout(FanoutWriter<B>),
    /// Writer for partitioned tables with sorted data (maintains a single active writer)
    Clustered(ClusteredWriter<B>),
}

impl<B: IcebergWriterBuilder> TaskWriter<B> {
    /// Create a new `TaskWriter`.
    ///
    /// * `writer_builder` - The writer builder used to create underlying writers
    /// * `fanout_enabled` - Controls whether `FanoutWriter` is used for partitioned tables; when
    ///   `false` the `ClusteredWriter` is selected instead
    /// * `schema` - The Iceberg schema reference for the incoming `RecordBatch`
    /// * `partition_spec` - The partition specification reference for the target table
    pub fn new(
        writer_builder: B,
        fanout_enabled: bool,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Self {
        Self::new_with_partition_splitter(
            writer_builder,
            fanout_enabled,
            schema,
            partition_spec,
            None,
        )
    }

    /// Create a new `TaskWriter` with a pre-configured partition splitter.
    ///
    /// This allows callers to provide a custom [`RecordBatchPartitionSplitter`], enabling use cases
    /// such as computing partition values at runtime rather than expecting a pre-computed
    /// `_partition` column in incoming batches.
    pub fn new_with_partition_splitter(
        writer_builder: B,
        fanout_enabled: bool,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
        partition_splitter: Option<RecordBatchPartitionSplitter>,
    ) -> Self {
        let writer = if partition_spec.is_unpartitioned() {
            SupportedWriter::Unpartitioned(UnpartitionedWriter::new(writer_builder))
        } else if fanout_enabled {
            SupportedWriter::Fanout(FanoutWriter::new(writer_builder))
        } else {
            SupportedWriter::Clustered(ClusteredWriter::new(writer_builder))
        };

        Self {
            writer: Some(writer),
            partition_splitter,
            schema,
            partition_spec,
        }
    }

    /// Write a `RecordBatch` to the `TaskWriter`.
    ///
    /// For the first write against a partitioned table, the partition splitter is initialised
    /// lazily. Unpartitioned tables bypass the splitter entirely.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            crate::Error::new(
                crate::ErrorKind::Unexpected,
                "TaskWriter has been closed and cannot be used",
            )
        })?;

        match writer {
            SupportedWriter::Unpartitioned(writer) => writer.write(batch).await,
            SupportedWriter::Fanout(writer) => {
                if self.partition_splitter.is_none() {
                    self.partition_splitter = Some(
                        RecordBatchPartitionSplitter::try_new_with_precomputed_values(
                            self.schema.clone(),
                            self.partition_spec.clone(),
                        )?,
                    );
                }

                Self::write_partitioned_batches(writer, &self.partition_splitter, &batch).await
            }
            SupportedWriter::Clustered(writer) => {
                if self.partition_splitter.is_none() {
                    self.partition_splitter = Some(
                        RecordBatchPartitionSplitter::try_new_with_precomputed_values(
                            self.schema.clone(),
                            self.partition_spec.clone(),
                        )?,
                    );
                }

                Self::write_partitioned_batches(writer, &self.partition_splitter, &batch).await
            }
        }
    }

    /// Close the `TaskWriter` and return all written data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.writer {
            match writer {
                SupportedWriter::Unpartitioned(writer) => writer.close().await,
                SupportedWriter::Fanout(writer) => writer.close().await,
                SupportedWriter::Clustered(writer) => writer.close().await,
            }
        } else {
            Err(crate::Error::new(
                crate::ErrorKind::Unexpected,
                "TaskWriter has already been closed",
            ))
        }
    }

    async fn write_partitioned_batches<W: PartitioningWriter>(
        writer: &mut W,
        partition_splitter: &Option<RecordBatchPartitionSplitter>,
        batch: &RecordBatch,
    ) -> Result<()> {
        let splitter = partition_splitter
            .as_ref()
            .expect("partition splitter must be initialised before use");
        let partitioned_batches = splitter.split(batch)?;

        for (partition_key, partition_batch) in partitioned_batches {
            writer.write(partition_key, partition_batch).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for TaskWriter<B> {
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        self.write(input).await
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.writer.take() {
            match writer {
                SupportedWriter::Unpartitioned(writer) => writer.close().await,
                SupportedWriter::Fanout(writer) => writer.close().await,
                SupportedWriter::Clustered(writer) => writer.close().await,
            }
        } else {
            Err(crate::Error::new(
                crate::ErrorKind::Unexpected,
                "TaskWriter has already been closed",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::Result;
    use crate::arrow::{PROJECTED_PARTITION_VALUE_COLUMN, RecordBatchPartitionSplitter};
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataFile, DataFileFormat, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::task_writer::TaskWriter;

    fn create_test_schema() -> Result<Arc<crate::spec::Schema>> {
        Ok(Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        ))
    }

    fn create_arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]))
    }

    fn create_arrow_schema_with_partition() -> Arc<Schema> {
        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_struct_field = Field::new(
            PROJECTED_PARTITION_VALUE_COLUMN,
            DataType::Struct(vec![partition_field.clone()].into()),
            false,
        );

        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            partition_struct_field,
        ]))
    }

    fn create_writer_builder(
        temp_dir: &TempDir,
        schema: Arc<crate::spec::Schema>,
    ) -> Result<
        DataFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
    > {
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_string_lossy().into_owned(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );
        Ok(DataFileWriterBuilder::new(rolling_writer_builder))
    }

    #[tokio::test]
    async fn test_task_writer_unpartitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema();

        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::new(writer_builder, false, schema, partition_spec);

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["US", "EU", "US"])) as ArrayRef,
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        assert!(!data_files.is_empty());
        assert_eq!(data_files[0].record_count(), 3);

        Ok(())
    }

    fn verify_partition_files(
        data_files: &[DataFile],
        expected_total: u64,
    ) -> HashMap<String, u64> {
        let total_records: u64 = data_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total_records, expected_total, "total record count mismatch");

        let mut partition_counts = HashMap::new();
        for data_file in data_files {
            let partition_value = data_file.partition();
            let region_literal = partition_value.fields()[0]
                .as_ref()
                .expect("partition value should not be null");
            let region = match region_literal
                .as_primitive_literal()
                .expect("expected primitive literal")
            {
                PrimitiveLiteral::String(s) => s.clone(),
                _ => panic!("expected string partition value"),
            };

            *partition_counts.entry(region.clone()).or_insert(0) += data_file.record_count();

            assert!(
                data_file.file_path().contains("region="),
                "file path should encode partition info"
            );
        }
        partition_counts
    }

    #[tokio::test]
    async fn test_task_writer_partitioned_with_computed_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema();

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", crate::spec::Transform::Identity)?
                .build()?,
        );
        let partition_splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            schema.clone(),
            partition_spec.clone(),
        )?;

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::new_with_partition_splitter(
            writer_builder,
            true,
            schema,
            partition_spec,
            Some(partition_splitter),
        );

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["US", "EU", "US", "EU"])) as ArrayRef,
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        let partition_counts = verify_partition_files(&data_files, 4);
        assert_eq!(partition_counts.get("US"), Some(&2));
        assert_eq!(partition_counts.get("EU"), Some(&2));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_writer_partitioned_fanout() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema_with_partition();

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", crate::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::new(writer_builder, true, schema, partition_spec);

        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_values = StringArray::from(vec!["US", "EU", "US", "EU"]);
        let partition_struct = StructArray::from(vec![(
            Arc::new(partition_field),
            Arc::new(partition_values) as ArrayRef,
        )]);

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["US", "EU", "US", "EU"])) as ArrayRef,
            Arc::new(partition_struct) as ArrayRef,
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        let partition_counts = verify_partition_files(&data_files, 4);
        assert_eq!(partition_counts.get("US"), Some(&2));
        assert_eq!(partition_counts.get("EU"), Some(&2));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_writer_partitioned_clustered() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema_with_partition();

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", crate::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::new(writer_builder, false, schema, partition_spec);

        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_values = StringArray::from(vec!["ASIA", "ASIA", "EU", "EU"]);
        let partition_struct = StructArray::from(vec![(
            Arc::new(partition_field),
            Arc::new(partition_values) as ArrayRef,
        )]);

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ASIA", "ASIA", "EU", "EU"])) as ArrayRef,
            Arc::new(partition_struct) as ArrayRef,
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        let partition_counts = verify_partition_files(&data_files, 4);
        assert_eq!(partition_counts.get("ASIA"), Some(&2));
        assert_eq!(partition_counts.get("EU"), Some(&2));

        Ok(())
    }
}
