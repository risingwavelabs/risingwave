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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use common::{random_ns, test_schema};
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{
    ApplyTransactionAction, MANIFEST_MERGE_ENABLED, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_TARGET_SIZE_BYTES, Transaction,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder};
use iceberg_integration_tests::get_test_fixture;
use parquet::file::properties::WriterProperties;

static FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn write_new_data_file(table: &Table) -> Vec<DataFile> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    // Use a short unique prefix each time so repeated calls don't produce identical paths
    // while keeping file name length small to mimic original manifest sizing for merge logic.
    let unique_prefix = format!("test{:04}", FILE_COUNTER.fetch_add(1, Ordering::Relaxed));
    let file_name_generator =
        DefaultFileNameGenerator::new(unique_prefix, None, iceberg::spec::DataFileFormat::Parquet);
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
    let col1 = StringArray::from(vec![Some("foo"); 100]);
    let col2 = Int32Array::from(vec![Some(1); 100]);
    let col3 = BooleanArray::from(vec![Some(true); 100]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    data_file_writer.close().await.unwrap()
}

// Helper: create table with 3 single-entry manifests produced by fast append
async fn create_table_with_three_manifests() -> (RestCatalog, Table) {
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
    for _ in 0..3 {
        let data_file = write_new_data_file(&table).await;
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_file.clone());
        let tx = append_action.apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap();
    }
    (rest_catalog, table)
}

// Scenario 1: Every manifest is packed into its own bin so merge is skipped (covers `manifest_bin.len()==1` branch)
#[tokio::test]
async fn test_merge_append_no_merge_each_single_bin() {
    let (rest_catalog, mut table) = create_table_with_three_manifests().await;
    let manifest_list_before = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list_before.entries().len(), 3);

    let snapshot_properties = HashMap::from([
        (MANIFEST_MERGE_ENABLED.to_string(), "true".to_string()),
        (MANIFEST_MIN_MERGE_COUNT.to_string(), "4".to_string()), // original setting
        (MANIFEST_TARGET_SIZE_BYTES.to_string(), "7000".to_string()), // small target -> each manifest isolated
    ]);

    let data_file = write_new_data_file(&table).await;
    let tx = Transaction::new(&table);
    let merge_action = tx
        .merge_append()
        .set_snapshot_properties(snapshot_properties)
        .add_data_files(data_file);
    let tx = merge_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let manifest_list_after = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    // No merge => 3 (existing) + 1 (new) = 4
    assert_eq!(manifest_list_after.entries().len(), 4);
    for entry in manifest_list_after.entries() {
        let m = entry.load_manifest(table.file_io()).await.unwrap();
        assert_eq!(
            m.entries().len(),
            1,
            "each manifest should still have exactly one data file"
        );
    }
}

// Scenario 2: All manifests fit into one bin but merge is skipped because bin contains the first manifest and count < min_count_to_merge
#[tokio::test]
async fn test_merge_append_no_merge_min_count_protects() {
    let (rest_catalog, mut table) = create_table_with_three_manifests().await;
    let manifest_list_before = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list_before.entries().len(), 3);

    let snapshot_properties = HashMap::from([
        (MANIFEST_MERGE_ENABLED.to_string(), "true".to_string()),
        (MANIFEST_MIN_MERGE_COUNT.to_string(), "10".to_string()), // larger than manifest count (4)
        (MANIFEST_TARGET_SIZE_BYTES.to_string(), "500000".to_string()), // large target -> all in one bin
    ]);

    let data_file = write_new_data_file(&table).await;
    let tx = Transaction::new(&table);
    let merge_action = tx
        .merge_append()
        .set_snapshot_properties(snapshot_properties)
        .add_data_files(data_file);
    let tx = merge_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let manifest_list_after = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    // len(4) < min_count(10) and bin contains first manifest => skip merge
    assert_eq!(manifest_list_after.entries().len(), 4);
}

// Scenario 3: Merge all manifests into a single manifest
#[tokio::test]
async fn test_merge_append_full_merge() {
    let (rest_catalog, mut table) = create_table_with_three_manifests().await;
    let manifest_list_before = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list_before.entries().len(), 3);

    let snapshot_properties = HashMap::from([
        (MANIFEST_MERGE_ENABLED.to_string(), "true".to_string()),
        (MANIFEST_MIN_MERGE_COUNT.to_string(), "2".to_string()), // low threshold allows merge
        (MANIFEST_TARGET_SIZE_BYTES.to_string(), "500000".to_string()), // large -> all in one bin
    ]);

    let data_file = write_new_data_file(&table).await;
    let tx = Transaction::new(&table);
    let merge_action = tx
        .merge_append()
        .set_snapshot_properties(snapshot_properties)
        .add_data_files(data_file);
    let tx = merge_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let manifest_list_after = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    // Expect all 4 single-file manifests merged into 1 manifest
    assert_eq!(
        manifest_list_after.entries().len(),
        1,
        "should merge into a single manifest file"
    );
    let merged_manifest = manifest_list_after.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert_eq!(
        merged_manifest.entries().len(),
        4,
        "merged manifest should contain 4 data file entries"
    );
}
