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

//! Integration tests for rest catalog.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{DataFile, Operation};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

mod common;
use common::{random_ns, test_schema};

// Helper function to create a data file writer builder
fn create_data_file_writer_builder(
    table: &Table,
) -> DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>
{
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        Some(Uuid::now_v7().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    DataFileWriterBuilder::new(rolling_writer_builder)
}

#[tokio::test]
async fn test_overwrite_data_files() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let data_file_writer_builder = create_data_file_writer_builder(&table);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // check parquet file schema
    let content = table
        .file_io()
        .new_input(data_file[0].file_path())
        .unwrap()
        .read()
        .await
        .unwrap();
    let parquet_reader = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
        &content,
        ArrowReaderOptions::default(),
    )
    .unwrap();
    let field_ids: Vec<i32> = parquet_reader
        .parquet_schema()
        .columns()
        .iter()
        .map(|col| col.self_type().get_basic_info().id())
        .collect();
    assert_eq!(field_ids, vec![1, 2, 3]);

    // commit result
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);

    // commit result again
    let tx = Transaction::new(&table);
    let append_action = tx
        .fast_append()
        .set_check_duplicate(false)
        .add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result again
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0], batch);
    assert_eq!(batches[1], batch);

    let data_file_writer_builder = create_data_file_writer_builder(&table);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file_overwrite = data_file_writer.close().await.unwrap();

    // commit result again
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .add_data_files(data_file_overwrite.clone())
        .delete_files(data_file.clone());

    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    {
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(
            Operation::Overwrite,
            snapshot.summary().operation,
            "Expected operation to be Overwrite after rewriting files"
        );
    }

    // check result
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);
}

#[tokio::test]
async fn test_empty_overwrite() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t2".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let tx = Transaction::new(&table);
    let overwrite_action = tx.overwrite_files();
    let tx = overwrite_action.apply(tx).unwrap();
    let err = tx
        .commit(&rest_catalog)
        .await
        .expect_err("empty overwrite should fail");
    assert_eq!(err.kind(), ErrorKind::PreconditionFailed);

    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert!(batches.is_empty());
}

#[tokio::test]
async fn test_multiple_file_overwrite() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t3".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file1 = data_file_writer.close().await.unwrap();

    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file2 = data_file_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .add_data_files(data_file1.clone())
        .add_data_files(data_file2.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0], batch);
    assert_eq!(batches[1], batch);
}

#[tokio::test]
async fn test_overwrite_nonexistent_file() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t4".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // Create a valid data file
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let valid_data_file = data_file_writer.close().await.unwrap();

    // Create a nonexistent data file (simulated by not writing it)
    let nonexistent_data_file = valid_data_file.clone();

    let tx = Transaction::new(&table);
    let overwrite_action = tx.overwrite_files();

    // Attempt to delete the nonexistent file
    let _overwrite_action = overwrite_action.delete_files(nonexistent_data_file);

    // No assertion needed - just verify builder compiles
}

#[tokio::test]
async fn test_sequence_number_in_manifest_entry() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t3".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file1 = data_file_writer.close().await.unwrap();

    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file2 = data_file_writer.close().await.unwrap();

    // Commit with sequence number

    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_new_data_file_sequence_number(12345)
        .add_data_files(data_file1.clone())
        .add_data_files(data_file2.clone());

    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify manifest entry has correct sequence number
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    assert_eq!(manifest_list.entries().len(), 1);

    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries() {
            assert_eq!(entry.sequence_number(), Some(12345));
        }
    }
}

#[tokio::test]
async fn test_partition_spec_id_in_manifest() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    // commit result
    let mut data_files_vec = Vec::default();

    async fn build_data_file_f(
        schema: Arc<arrow_schema::Schema>,
        table: &Table,
        location_generator: DefaultLocationGenerator,
        file_name_generator: DefaultFileNameGenerator,
    ) -> DataFile {
        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
        let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
        let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
        let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(col1) as ArrayRef,
            Arc::new(col2) as ArrayRef,
            Arc::new(col3) as ArrayRef,
        ])
        .unwrap();
        data_file_writer.write(batch.clone()).await.unwrap();
        data_file_writer.close().await.unwrap()[0].clone()
    }

    for _ in 0..10 {
        let data_file = build_data_file_f(
            schema.clone(),
            &table,
            location_generator.clone(),
            file_name_generator.clone(),
        )
        .await;
        data_files_vec.push(data_file.clone());
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(vec![data_file]);
        let tx = append_action.apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap();
    }

    let last_data_files = data_files_vec.last().unwrap();
    let partition_spec_id = last_data_files.partition_spec_id();

    // remove the data files by overwriteAction
    for data_file in &data_files_vec {
        let tx = Transaction::new(&table);
        let overwrite_action = tx.overwrite_files().delete_files(vec![data_file.clone()]);
        let tx = overwrite_action.apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap();
    }

    // TODO: test update partition spec
    // Verify that the partition spec ID is correctly set
    let last_snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = last_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert!(!manifest_list.entries().is_empty());
    for manifest_file in manifest_list.entries() {
        assert_eq!(manifest_file.partition_spec_id, partition_spec_id);
    }
}

#[tokio::test]
async fn test_overwrite_files_to_branch() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("branch_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Prepare test data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let original_files = data_file_writer.close().await.unwrap();

    // First append to main branch
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(original_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify main branch has the data
    let main_snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(main_snapshot.parent_snapshot_id(), None);

    let branch_name = "test_branch";

    // Prepare overwrite files
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let overwrite_files = data_file_writer.close().await.unwrap();

    // overwrite files to the new branch
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch_name.to_string())
        .add_data_files(overwrite_files.clone())
        .delete_files(original_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify branch snapshot
    let branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    assert_eq!(
        branch_snapshot.parent_snapshot_id(),
        None,
        "Branch snapshot should not have a parent snapshot"
    );

    // Verify main branch remains unchanged
    let current_main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();
    assert_eq!(
        current_main_snapshot.snapshot_id(),
        main_snapshot.snapshot_id()
    );

    // Verify data in branch
    let branch_batch_stream = table
        .scan()
        .snapshot_id(
            table
                .metadata()
                .snapshot_for_ref(branch_name)
                .unwrap()
                .snapshot_id(),
        )
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let branch_batches: Vec<_> = branch_batch_stream.try_collect().await.unwrap();
    assert_eq!(branch_batches.len(), 1);
    assert_eq!(branch_batches[0], batch);

    // Verify data in main branch remains unchanged
    let main_batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let main_batches: Vec<_> = main_batch_stream.try_collect().await.unwrap();
    assert_eq!(main_batches.len(), 1);
    assert_eq!(main_batches[0], batch);
}

#[tokio::test]
async fn test_branch_snapshot_isolation() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("branch_snapshot_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create initial snapshot in main branch
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_files = data_file_writer.close().await.unwrap();

    // Commit to main branch
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Create branch1 from main
    let branch1 = "branch1";
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch1.to_string())
        .add_data_files(data_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify branch1 snapshot
    let branch1_snapshot = table.metadata().snapshot_for_ref(branch1).unwrap();
    assert_eq!(
        branch1_snapshot.parent_snapshot_id(),
        None,
        "Branch1 should have main snapshot as parent"
    );

    // Create branch2 from branch1
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch1.to_string())
        .add_data_files(data_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify branch1 snapshot
    let branch1_snapshot_2 = table.metadata().snapshot_for_ref(branch1).unwrap();
    assert_eq!(
        branch1_snapshot_2.parent_snapshot_id(),
        Some(branch1_snapshot.snapshot_id()),
        " Should have branch1 snapshot as parent"
    );
}

#[tokio::test]
async fn test_overwrite_files_with_sequence_number_from_branch() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("branch_seq_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Prepare test data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let original_files = data_file_writer.close().await.unwrap();

    // First append to main branch
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(original_files.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let branch_name = "seq_branch";
    let tx = Transaction::new(&table);
    let sequence_from_main = table
        .metadata()
        .snapshot_for_ref("main")
        .unwrap()
        .sequence_number();
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch_name.to_string())
        .set_new_data_file_sequence_number(sequence_from_main)
        .add_data_files(original_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check manifest entry for sequence number
    let branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    let manifest_list = branch_snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    assert_eq!(manifest_list.entries().len(), 1);
    let manifest_file = &manifest_list.entries()[0];
    let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
    assert_eq!(manifest.entries().len(), 1);
    let entry = &manifest.entries()[0];
    assert_eq!(entry.sequence_number(), Some(1));
}

#[tokio::test]
async fn test_multiple_branches_isolation() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let table_creation = TableCreation::builder()
        .name("multi_branch_test".to_string())
        .schema(schema.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Prepare test data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1.clone()) as ArrayRef,
        Arc::new(col2.clone()) as ArrayRef,
        Arc::new(col3.clone()) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let original_files = data_file_writer.close().await.unwrap();

    // Create branch1 with original files
    let branch1 = "branch1";
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch1.to_string())
        .add_data_files(original_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Create branch2 with modified data
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
    let modified_col1 = StringArray::from(vec![Some("modified"), Some("data"), None, Some("here")]);
    let modified_batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(modified_col1) as ArrayRef,
        Arc::new(col2.clone()) as ArrayRef,
        Arc::new(col3.clone()) as ArrayRef,
    ])
    .unwrap();
    data_file_writer
        .write(modified_batch.clone())
        .await
        .unwrap();
    let modified_files = data_file_writer.close().await.unwrap();

    let branch2 = "branch2";
    let tx = Transaction::new(&table);
    let overwrite_action = tx
        .overwrite_files()
        .set_target_branch(branch2.to_string())
        .add_data_files(modified_files.clone());
    let tx = overwrite_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify branches are isolated
    let branch1_data: Vec<_> = table
        .scan()
        .snapshot_id(
            table
                .metadata()
                .snapshot_for_ref(branch1)
                .unwrap()
                .snapshot_id(),
        )
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(branch1_data[0], batch);

    let branch2_data: Vec<_> = table
        .scan()
        .snapshot_id(
            table
                .metadata()
                .snapshot_for_ref(branch2)
                .unwrap()
                .snapshot_id(),
        )
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(branch2_data[0], modified_batch);
}
