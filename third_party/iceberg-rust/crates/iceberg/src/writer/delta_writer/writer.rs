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

//! Delta writer that produces data files, position delete files and equality delete files
//! in a single pass.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_select::filter::filter_record_batch;
use itertools::Itertools;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::arrow::record_batch_projector::RecordBatchProjector;
use crate::arrow::schema_to_arrow_schema;
use crate::spec::{DataFile, PartitionKey, SchemaRef};
use crate::writer::base_writer::position_delete_file_writer::PositionDeleteInput;
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Insert operation marker.
pub const INSERT_OP: i32 = 1;
/// Delete operation marker.
pub const DELETE_OP: i32 = 2;

/// Builder for [`DeltaWriter`].
#[derive(Clone)]
pub struct DeltaWriterBuilder<DB, PDB, EDB> {
    data_writer_builder: DB,
    position_delete_writer_builder: PDB,
    equality_delete_writer_builder: EDB,
    unique_column_ids: Vec<i32>,
    schema: SchemaRef,
}

impl<DB, PDB, EDB> DeltaWriterBuilder<DB, PDB, EDB> {
    /// Create a new `DeltaWriterBuilder`.
    pub fn new(
        data_writer_builder: DB,
        position_delete_writer_builder: PDB,
        equality_delete_writer_builder: EDB,
        unique_column_ids: Vec<i32>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            data_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl<DB, PDB, EDB> IcebergWriterBuilder for DeltaWriterBuilder<DB, PDB, EDB>
where
    DB: IcebergWriterBuilder,
    PDB: IcebergWriterBuilder<Vec<PositionDeleteInput>>,
    EDB: IcebergWriterBuilder,
    DB::R: CurrentFileStatus,
{
    type R = DeltaWriter<DB::R, PDB::R, EDB::R>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Self::R::try_new(
            self.data_writer_builder
                .build(partition_key.clone())
                .await?,
            self.position_delete_writer_builder
                .build(partition_key.clone())
                .await?,
            self.equality_delete_writer_builder
                .build(partition_key)
                .await?,
            self.schema.clone(),
            self.unique_column_ids.clone(),
        )
    }
}

/// Writer that handles insert and delete operations in a single stream.
pub struct DeltaWriter<D, PD, ED> {
    data_writer: D,
    position_delete_writer: PD,
    equality_delete_writer: ED,
    projector: RecordBatchProjector,
    inserted_row: HashMap<OwnedRow, PositionDeleteInput>,
    row_converter: RowConverter,
}

impl<D, PD, ED> DeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<Vec<PositionDeleteInput>>,
    ED: IcebergWriter,
{
    pub(crate) fn try_new(
        data_writer: D,
        position_delete_writer: PD,
        equality_delete_writer: ED,
        schema: SchemaRef,
        unique_column_ids: Vec<i32>,
    ) -> Result<Self> {
        let projector = RecordBatchProjector::new(
            Arc::new(schema_to_arrow_schema(&schema)?),
            &unique_column_ids,
            |field| {
                if field.data_type().is_nested() {
                    return Ok(None);
                }
                field
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY)
                    .map(|s| {
                        s.parse::<i64>()
                            .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))
                    })
                    .transpose()
            },
            |_| true,
        )?;
        let row_converter = RowConverter::new(
            projector
                .projected_schema_ref()
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        Ok(Self {
            data_writer,
            position_delete_writer,
            equality_delete_writer,
            projector,
            inserted_row: HashMap::new(),
            row_converter,
        })
    }

    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let row_count = batch.num_rows();

        // Initialise the writer by writing the batch first; offsets are derived from the end state.
        self.data_writer.write(batch).await?;

        let end_offset = self.data_writer.current_row_num();
        let start_offset = end_offset.saturating_sub(row_count);
        let current_file_path: Arc<str> = Arc::from(self.data_writer.current_file_path());

        let mut position_deletes = Vec::new();
        for (idx, row) in rows.iter().enumerate() {
            let previous_input = self.inserted_row.insert(
                row.owned(),
                PositionDeleteInput::new(current_file_path.clone(), (start_offset + idx) as i64),
            );
            if let Some(previous_input) = previous_input {
                position_deletes.push(previous_input);
            }
        }

        self.write_position_deletes(position_deletes).await
    }

    async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::with_capacity(rows.num_rows());
        let mut position_deletes = Vec::new();
        for row in rows.iter() {
            if let Some(previous_input) = self.inserted_row.remove(&row.owned()) {
                position_deletes.push(previous_input);
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }

        self.write_position_deletes(position_deletes).await?;

        let delete_mask = delete_row.finish();
        if delete_mask.null_count() == delete_mask.len() {
            return Ok(());
        }

        let delete_batch = filter_record_batch(&batch, &delete_mask).map_err(|err| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Failed to filter record batch, error: {err}"),
            )
        })?;
        if delete_batch.num_rows() == 0 {
            return Ok(());
        }
        self.equality_delete_writer.write(delete_batch).await
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.projector.project_column(batch.columns())?)
            .map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to convert columns, error: {err}"),
                )
            })
    }

    async fn write_position_deletes(
        &mut self,
        mut deletes: Vec<PositionDeleteInput>,
    ) -> Result<()> {
        if deletes.is_empty() {
            return Ok(());
        }
        deletes.sort_by(|a, b| {
            let path_cmp = a.path.as_ref().cmp(b.path.as_ref());
            if path_cmp == std::cmp::Ordering::Equal {
                a.pos.cmp(&b.pos)
            } else {
                path_cmp
            }
        });
        self.position_delete_writer.write(deletes).await
    }
}

#[async_trait::async_trait]
impl<D, PD, ED> IcebergWriter for DeltaWriter<D, PD, ED>
where
    D: IcebergWriter + CurrentFileStatus,
    PD: IcebergWriter<Vec<PositionDeleteInput>>,
    ED: IcebergWriter,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_columns() == 0 {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Delta writer expects at least one column for operation markers",
            ));
        }

        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(
                ErrorKind::DataInvalid,
                "The last column must be an Int32Array of operation markers",
            ))?;

        let partitions =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to partition ops, error: {err}"),
                )
            })?;
        for range in partitions.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .unwrap()
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                INSERT_OP => self.insert(batch).await?,
                DELETE_OP => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid ops: {op}"),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let data_files = self.data_writer.close().await?;
        let position_delete_files = self.position_delete_writer.close().await?;
        let equality_delete_files = self.equality_delete_writer.close().await?;
        Ok(data_files
            .into_iter()
            .chain(position_delete_files)
            .chain(equality_delete_files)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use arrow_select::concat::concat_batches;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::Result;
    use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
    use crate::io::FileIOBuilder;
    use crate::spec::{DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::base_writer::equality_delete_writer::{
        EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
    };
    use crate::writer::base_writer::position_delete_file_writer::PositionDeleteFileWriterBuilder;
    use crate::writer::delta_writer::{DELETE_OP, DeltaWriterBuilder, INSERT_OP};
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::parquet_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    type WriterBuildersResult = (
        DataFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
        PositionDeleteFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
        EqualityDeleteFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
        EqualityDeleteWriterConfig,
    );

    fn position_delete_arrow_schema() -> ArrowSchema {
        schema_to_arrow_schema(
            &Schema::builder()
                .with_fields(vec![
                    NestedField::required(
                        2147483546,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )
                    .into(),
                    NestedField::required(2147483545, "pos", Type::Primitive(PrimitiveType::Long))
                        .into(),
                ])
                .build()
                .unwrap(),
        )
        .unwrap()
    }

    fn create_writer_builders(
        data_schema: Arc<Schema>,
        file_io: &crate::io::FileIO,
        location_gen: DefaultLocationGenerator,
        file_name_gen: DefaultFileNameGenerator,
    ) -> Result<WriterBuildersResult> {
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), data_schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen.clone(),
            file_name_gen.clone(),
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let position_delete_parquet_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(
                            2147483546,
                            "file_path",
                            Type::Primitive(PrimitiveType::String),
                        )
                        .into(),
                        NestedField::required(
                            2147483545,
                            "pos",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into(),
                    ])
                    .build()
                    .unwrap(),
            ),
        );
        let position_delete_writer_builder = PositionDeleteFileWriterBuilder::new(
            RollingFileWriterBuilder::new_with_default_file_size(
                position_delete_parquet_builder,
                file_io.clone(),
                location_gen.clone(),
                file_name_gen.clone(),
            ),
        );

        let equality_config = EqualityDeleteWriterConfig::new(vec![1, 2], data_schema.clone())?;
        let equality_delete_writer_builder = {
            let schema =
                arrow_schema_to_schema(equality_config.projected_arrow_schema_ref())?.into();
            let parquet_writer_builder =
                ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
            let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
                parquet_writer_builder,
                file_io.clone(),
                location_gen,
                file_name_gen,
            );
            EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, equality_config.clone())
        };

        Ok((
            data_file_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            equality_config,
        ))
    }

    #[tokio::test]
    async fn test_delta_writer() -> Result<()> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id".to_string(),
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        2,
                        "name".to_string(),
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap(),
        );
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen =
            DefaultLocationGenerator::with_data_location(temp_dir.path().to_string_lossy().into());
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let (
            data_file_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            equality_config,
        ) = create_writer_builders(
            schema.clone(),
            &file_io,
            location_gen.clone(),
            file_name_gen.clone(),
        )?;

        let mut delta_writer = DeltaWriterBuilder::new(
            data_file_writer_builder,
            position_delete_writer_builder,
            equality_delete_writer_builder,
            vec![1, 2],
            schema.clone(),
        )
        .build(None)
        .await?;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("data", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("op", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]));

        let batch_one = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 3, 1])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g"])),
            Arc::new(Int32Array::from(vec![INSERT_OP; 7])),
        ])?;
        delta_writer.write(batch_one).await?;

        let batch_two = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "k", "l"])),
            Arc::new(Int32Array::from(vec![
                DELETE_OP, DELETE_OP, DELETE_OP, INSERT_OP,
            ])),
        ])?;
        delta_writer.write(batch_two).await?;

        let data_files = delta_writer.close().await?;
        assert_eq!(data_files.len(), 3);

        let data_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("data", DataType::Utf8, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ]));

        let data_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::Data)
            .unwrap();
        let data_file_path = data_file.file_path().to_string();
        let input_content = file_io.new_input(data_file_path.clone())?.read().await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(input_content)?
            .build()
            .unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&data_schema, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(data_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 3, 1, 4])),
            Arc::new(StringArray::from(vec![
                "a", "b", "c", "d", "e", "f", "g", "l",
            ])),
        ])?;
        assert_eq!(expected_batches, res);

        let position_delete_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::PositionDeletes)
            .unwrap();
        let position_input = file_io
            .new_input(position_delete_file.file_path.clone())?
            .read()
            .await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(position_input)?
            .build()
            .unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let position_schema = Arc::new(position_delete_arrow_schema());
        let res = concat_batches(&position_schema, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(position_schema.clone(), vec![
            Arc::new(StringArray::from(vec![
                data_file_path.clone(),
                data_file_path.clone(),
            ])),
            Arc::new(Int64Array::from(vec![0, 1])),
        ])?;
        assert_eq!(expected_batches, res);

        let equality_delete_file = data_files
            .iter()
            .find(|file| file.content == DataContentType::EqualityDeletes)
            .unwrap();
        let equality_input = file_io
            .new_input(equality_delete_file.file_path.clone())?
            .read()
            .await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(equality_input)?
            .build()
            .unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let equality_schema = Arc::new(arrow_schema_to_schema(
            equality_config.projected_arrow_schema_ref(),
        )?);
        let equality_arrow_schema = Arc::new(schema_to_arrow_schema(&equality_schema)?);
        let res = concat_batches(&equality_arrow_schema, &batches).unwrap();
        let expected_batches = RecordBatch::try_new(equality_arrow_schema.clone(), vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(StringArray::from(vec!["k"])),
        ])?;
        assert_eq!(expected_batches, res);

        Ok(())
    }
}
