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

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use common::{random_ns, test_schema};
use futures::TryStreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use parquet::file::properties::WriterProperties;

#[tokio::test]
async fn test_append_data_file_conflict() {
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
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
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

    // start two transaction and commit one of them
    let tx1 = Transaction::new(&table);
    let append_action = tx1.fast_append().add_data_files(data_file.clone());
    let tx1 = append_action.apply(tx1).unwrap();

    let tx2 = Transaction::new(&table);
    let append_action = tx2.fast_append().add_data_files(data_file.clone());
    let tx2 = append_action.apply(tx2).unwrap();
    let table = tx2
        .commit(&rest_catalog)
        .await
        .expect("The first commit should not fail.");

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

    // another commit should fail
    assert!(tx1.commit(&rest_catalog).await.is_err());
}

#[tokio::test]
async fn test_append_data_file_target_branch() {
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
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
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

    // Test 1: Append to main branch (default behavior)
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify main branch has the data
    assert!(table.metadata().current_snapshot().is_some());
    let main_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

    // Verify main branch ref points to the snapshot using snapshot_for_ref
    let main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();
    // First commit should have no parent snapshot
    assert_eq!(main_snapshot.parent_snapshot_id(), None);

    let main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();

    // Verify main branch data
    let main_batch_stream = table
        .scan()
        .snapshot_id(main_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let main_batches: Vec<_> = main_batch_stream.try_collect().await.unwrap();
    assert_eq!(main_batches.len(), 1);
    assert_eq!(main_batches[0], batch);
    assert_eq!(
        main_batches[0].schema(),
        batch.schema(),
        "Main branch schema mismatch"
    );

    // Test 2: Append to a custom branch
    let branch_name = "test-branch";
    let second_location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let second_file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        Some(uuid::Uuid::now_v7().to_string()),
        iceberg::spec::DataFileFormat::Parquet,
    );
    let second_parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let second_rolling_writer = RollingFileWriterBuilder::new_with_default_file_size(
        second_parquet_writer_builder,
        table.file_io().clone(),
        second_location_generator,
        second_file_name_generator,
    );
    let second_writer_builder = DataFileWriterBuilder::new(second_rolling_writer);
    let mut second_writer = second_writer_builder.build(None).await.unwrap();
    second_writer.write(batch.clone()).await.unwrap();
    let second_data_file = second_writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx
        .fast_append()
        .set_target_branch(branch_name.to_string())
        .add_data_files(second_data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the custom branch was created and points to a new snapshot
    let branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    assert_ne!(branch_snapshot.snapshot_id(), main_snapshot_id);
    // New branch should have no parent snapshot
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(branch_name)
            .unwrap()
            .parent_snapshot_id(),
        None
    );

    // Verify test-branch data
    let branch_batch_stream = table
        .scan()
        .snapshot_id(branch_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let branch_batches: Vec<_> = branch_batch_stream.try_collect().await.unwrap();
    assert_eq!(branch_batches.len(), 1);
    assert_eq!(branch_batches[0], batch);
    assert_eq!(
        branch_batches[0].schema(),
        batch.schema(),
        "Test branch schema mismatch"
    );

    // Verify the main branch is unchanged
    let main_snapshot_after = table.metadata().snapshot_for_ref("main").unwrap();
    assert_eq!(main_snapshot_after.snapshot_id(), main_snapshot_id);

    // Test 3: Append to the same custom branch again
    let tx = Transaction::new(&table);
    let append_action = tx
        .fast_append()
        .set_target_branch(branch_name.to_string())
        .add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the custom branch now points to a newer snapshot
    let branch_snapshot_final = table.metadata().snapshot_for_ref(branch_name).unwrap();
    assert_ne!(
        branch_snapshot_final.snapshot_id(),
        branch_snapshot.snapshot_id()
    );
    assert_ne!(branch_snapshot_final.snapshot_id(), main_snapshot_id);
    // Second append should have previous branch snapshot as parent
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(branch_name)
            .unwrap()
            .parent_snapshot_id(),
        Some(branch_snapshot.snapshot_id())
    );

    // Verify test-branch data after second append
    let branch_batch_stream = table
        .scan()
        .snapshot_id(branch_snapshot_final.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let branch_batches: Vec<_> = branch_batch_stream.try_collect().await.unwrap();
    assert_eq!(branch_batches.len(), 2);
    assert_eq!(branch_batches[0], batch);
    assert_eq!(branch_batches[1], batch);
    assert_eq!(
        branch_batches[0].schema(),
        batch.schema(),
        "Test branch schema mismatch after second append"
    );

    // Verify we have 3 snapshots total (1 main + 2 branch)
    assert_eq!(table.metadata().snapshots().count(), 3);

    // Test 4: Test merge append to branch
    let another_branch = "merge-branch";
    let tx = Transaction::new(&table);
    let merge_append_action = tx
        .merge_append()
        .set_target_branch(another_branch.to_string())
        .add_data_files(data_file.clone());
    let tx = merge_append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify the merge branch was created
    let merge_branch_snapshot = table.metadata().snapshot_for_ref(another_branch).unwrap();
    assert_ne!(merge_branch_snapshot.snapshot_id(), main_snapshot_id);
    assert_ne!(
        merge_branch_snapshot.snapshot_id(),
        branch_snapshot_final.snapshot_id()
    );
    // Merge branch should have no parent snapshot
    assert_eq!(
        table
            .metadata()
            .snapshot_for_ref(another_branch)
            .unwrap()
            .parent_snapshot_id(),
        None
    );

    // Verify we now have 4 snapshots total
    assert_eq!(table.metadata().snapshots().count(), 4);

    // Verify merge-branch data
    let merge_batch_stream = table
        .scan()
        .snapshot_id(merge_branch_snapshot.snapshot_id())
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let merge_batches: Vec<_> = merge_batch_stream.try_collect().await.unwrap();
    assert_eq!(merge_batches.len(), 1);
    assert_eq!(merge_batches[0], batch);
    assert_eq!(
        merge_batches[0].schema(),
        batch.schema(),
        "Merge branch schema mismatch"
    );

    // Verify all branches exist and can be accessed via snapshot_for_ref
    assert!(table.metadata().snapshot_for_ref("main").is_some());
    assert!(table.metadata().snapshot_for_ref(branch_name).is_some());
    assert!(table.metadata().snapshot_for_ref(another_branch).is_some());

    // Verify each branch points to different snapshots
    let final_main_snapshot = table.metadata().snapshot_for_ref("main").unwrap();
    let final_branch_snapshot = table.metadata().snapshot_for_ref(branch_name).unwrap();
    let final_merge_snapshot = table.metadata().snapshot_for_ref(another_branch).unwrap();

    assert_ne!(
        final_main_snapshot.snapshot_id(),
        final_branch_snapshot.snapshot_id()
    );
    assert_ne!(
        final_main_snapshot.snapshot_id(),
        final_merge_snapshot.snapshot_id()
    );
    assert_ne!(
        final_branch_snapshot.snapshot_id(),
        final_merge_snapshot.snapshot_id()
    );
}
