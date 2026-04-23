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

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use common::{random_ns, test_schema};
use iceberg::actions::RemoveOrphanFilesAction;
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

async fn write_data_files(
    table: &mut iceberg::table::Table,
    catalog: &impl Catalog,
    prefix: &str,
) -> Vec<String> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_gen = DefaultFileNameGenerator::new(
        prefix.to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(None)
        .await
        .unwrap();

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
    ])
    .unwrap();

    writer.write(batch).await.unwrap();
    let data_files = writer.close().await.unwrap();
    let paths: Vec<String> = data_files
        .iter()
        .map(|f| f.file_path().to_string())
        .collect();

    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .unwrap();
    *table = tx.commit(catalog).await.unwrap();

    paths
}

fn future_timestamp() -> i64 {
    current_timestamp_millis() + 3_600_000
}

fn current_timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Creates an orphan file at the given path.
async fn create_orphan_file(table: &iceberg::table::Table, path: &str) {
    table
        .file_io()
        .new_output(path)
        .unwrap()
        .write("orphan".into())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_dry_run_and_delete() {
    let fixture = get_test_fixture();
    let catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let mut table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t1".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();

    let valid_files = write_data_files(&mut table, &catalog, "valid").await;

    let orphan_path = format!("{}/data/orphan.parquet", table.metadata().location());
    create_orphan_file(&table, &orphan_path).await;

    // Dry run: find but don't delete
    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert!(result.contains(&orphan_path), "Should find orphan file");
    assert!(
        !result.iter().any(|p| valid_files.contains(p)),
        "Valid files not orphan"
    );
    assert!(
        table.file_io().exists(&orphan_path).await.unwrap(),
        "Orphan still exists"
    );

    // Actual delete
    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(false)
        .execute()
        .await
        .unwrap();

    assert!(result.contains(&orphan_path), "Should have deleted orphan");
    assert!(
        !table.file_io().exists(&orphan_path).await.unwrap(),
        "Orphan deleted"
    );
    for f in &valid_files {
        assert!(
            table.file_io().exists(f).await.unwrap(),
            "Valid file preserved"
        );
    }
}

#[tokio::test]
async fn test_older_than_threshold() {
    let fixture = get_test_fixture();
    let catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let mut table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t2".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();

    write_data_files(&mut table, &catalog, "data").await;

    let orphan_path = format!("{}/data/orphan.parquet", table.metadata().location());
    create_orphan_file(&table, &orphan_path).await;

    // Past threshold: should NOT find orphan (file is newer)
    let past_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 3_600_000;

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(past_ts)
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert!(
        !result.contains(&orphan_path),
        "Should not find new orphan with past threshold"
    );

    // Future threshold: should find orphan
    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    assert!(
        result.contains(&orphan_path),
        "Should find orphan with future threshold"
    );
}

#[tokio::test]
async fn test_preserves_metadata_files() {
    let fixture = get_test_fixture();
    let catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let mut table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t3".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();

    // Create multiple snapshots
    let mut all_data_files = Vec::new();
    for i in 0..3 {
        all_data_files.extend(write_data_files(&mut table, &catalog, &format!("s{i}")).await);
    }

    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            ns.name().clone(),
            "t3".to_string(),
        ))
        .await
        .unwrap();

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    let orphan_set: HashSet<_> = result.into_iter().collect();

    // Verify metadata files NOT orphan
    if let Some(loc) = table.metadata_location() {
        assert!(!orphan_set.contains(loc), "Current metadata not orphan");
    }
    for log in table.metadata().metadata_log() {
        assert!(
            !orphan_set.contains(&log.metadata_file),
            "Historical metadata not orphan"
        );
    }

    // Verify manifest lists NOT orphan
    for snap in table.metadata().snapshots() {
        assert!(
            !orphan_set.contains(snap.manifest_list()),
            "Manifest list not orphan"
        );
    }

    // Verify data files NOT orphan
    for f in &all_data_files {
        assert!(!orphan_set.contains(f), "Data file not orphan: {f}");
    }
}

#[tokio::test]
async fn test_after_expire_snapshots() {
    let fixture = get_test_fixture();
    let catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let mut table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t4".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();

    // Create 3 snapshots
    let mut manifest_lists = Vec::new();
    for i in 0..3 {
        write_data_files(&mut table, &catalog, &format!("b{i}")).await;
        if let Some(snap) = table.metadata().current_snapshot() {
            manifest_lists.push(snap.manifest_list().to_string());
        }
    }

    let current_ml = manifest_lists.last().unwrap().clone();
    let expired_mls: Vec<_> = manifest_lists[..2].to_vec();

    // Expire old snapshots
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshot()
        .retain_last(1)
        .expire_older_than(current_timestamp_millis())
        .apply(tx)
        .unwrap();
    table = tx.commit(&catalog).await.unwrap();

    assert_eq!(table.metadata().snapshots().count(), 1);

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    let orphan_set: HashSet<_> = result.into_iter().collect();

    assert!(
        !orphan_set.contains(&current_ml),
        "Current manifest list not orphan"
    );
    for ml in &expired_mls {
        assert!(
            orphan_set.contains(ml),
            "Expired manifest list should be orphan: {ml}"
        );
    }
}

#[tokio::test]
async fn test_after_rewrite() {
    let fixture = get_test_fixture();
    let catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let mut table = catalog
        .create_table(
            ns.name(),
            TableCreation::builder()
                .name("t5".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .unwrap();

    write_data_files(&mut table, &catalog, "initial").await;

    let first_ml = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .manifest_list()
        .to_string();
    let first_manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    let first_manifests: Vec<_> = first_manifest_list
        .entries()
        .iter()
        .map(|e| e.manifest_path.clone())
        .collect();

    // Load original data files for rewrite
    let mut original_data_files = Vec::new();
    for entry in first_manifest_list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await.unwrap();
        for e in manifest.entries() {
            if e.is_alive() {
                original_data_files.push(e.data_file().clone());
            }
        }
    }

    // Write new files and rewrite
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_gen = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_gen = DefaultFileNameGenerator::new(
        "rewritten".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling_builder)
        .build(None)
        .await
        .unwrap();

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef,
        Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
    ])
    .unwrap();

    writer.write(batch).await.unwrap();
    let new_data_files = writer.close().await.unwrap();

    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files()
        .delete_files(original_data_files)
        .add_data_files(new_data_files)
        .apply(tx)
        .unwrap();
    table = tx.commit(&catalog).await.unwrap();

    // Expire old snapshot
    let tx = Transaction::new(&table);
    let tx = tx
        .expire_snapshot()
        .retain_last(1)
        .expire_older_than(current_timestamp_millis())
        .apply(tx)
        .unwrap();
    table = tx.commit(&catalog).await.unwrap();

    let result = RemoveOrphanFilesAction::new(table.clone())
        .older_than_ms(future_timestamp())
        .dry_run(true)
        .execute()
        .await
        .unwrap();

    let orphan_set: HashSet<_> = result.into_iter().collect();

    // First manifest-list should be orphan
    assert!(
        orphan_set.contains(&first_ml),
        "First manifest-list should be orphan"
    );

    // First manifests should be orphan
    for m in &first_manifests {
        assert!(
            orphan_set.contains(m),
            "First manifest should be orphan: {m}"
        );
    }
}
