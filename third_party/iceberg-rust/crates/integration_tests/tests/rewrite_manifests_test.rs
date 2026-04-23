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

//! Integration tests for rewrite_manifests transaction action.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use common::{random_ns, test_schema};
use futures::TryStreamExt;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
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

/// Write a single data file to the given table and return the DataFile metadata.
async fn write_data_file(table: &Table) -> Vec<DataFile> {
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let unique_prefix = format!(
        "rwm_test{:04}",
        FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
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
    let col1 = StringArray::from(vec![Some("foo"), Some("bar"), None, Some("baz")]);
    let col2 = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false), None, Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch).await.unwrap();
    data_file_writer.close().await.unwrap()
}

/// Create a table with N manifests by performing N separate fast_append commits.
async fn create_table_with_manifests(
    num_manifests: usize,
    table_name: &str,
) -> (RestCatalog, Table) {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();
    let table_creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(schema.clone())
        .build();
    let mut table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();
    for _ in 0..num_manifests {
        let data_file = write_data_file(&table).await;
        let tx = Transaction::new(&table);
        let append_action = tx.fast_append().add_data_files(data_file);
        let tx = append_action.apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap();
    }
    (rest_catalog, table)
}

/// Test: rewrite manifests with cluster_by consolidates multiple manifests into one.
///
/// 1. Create 3 separate manifests via fast_append.
/// 2. Use rewrite_manifests with cluster_by to consolidate all entries.
/// 3. Verify that the resulting snapshot has fewer manifests.
/// 4. Verify data is unchanged via scan.
#[tokio::test]
async fn test_rewrite_manifests_cluster_by() {
    let (rest_catalog, table) = create_table_with_manifests(3, "t_rwm_cluster").await;

    // Verify we start with 3 manifests
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);

    // Count total rows before rewrite
    let batches_before: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows_before: usize = batches_before.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows_before, 3 * 4); // 3 files, 4 rows each

    // Rewrite manifests: cluster everything into one group
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_data_file| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Verify manifests were consolidated
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    // All entries clustered by same key -> 1 manifest
    assert_eq!(
        manifest_list.entries().len(),
        1,
        "all manifests should be consolidated into one"
    );

    // Verify the single manifest contains all 3 data file entries
    let manifest = manifest_list.entries()[0]
        .load_manifest(table.file_io())
        .await
        .unwrap();
    assert_eq!(
        manifest.entries().len(),
        3,
        "consolidated manifest should contain all 3 data file entries"
    );

    // Verify data is unchanged
    let batches_after: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows_after: usize = batches_after.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows_before, total_rows_after);

    // Verify snapshot summary counts
    let summary = &snapshot.summary().additional_properties;
    assert_eq!(summary.get("manifests-created").unwrap(), "1");
    assert_eq!(summary.get("manifests-kept").unwrap(), "0");
    assert_eq!(summary.get("manifests-replaced").unwrap(), "3");
    assert_eq!(summary.get("entries-processed").unwrap(), "3");
}

/// Test: rewrite manifests with rewrite_if predicate only rewrites matching manifests.
///
/// 1. Create 3 manifests via fast_append.
/// 2. Use rewrite_manifests with cluster_by and rewrite_if that only matches
///    manifests with a specific manifest length threshold.
/// 3. Since no manifests match the predicate, the action is a no-op and no
///    new snapshot is created.
#[tokio::test]
async fn test_rewrite_manifests_with_predicate_no_match() {
    let (rest_catalog, table) = create_table_with_manifests(3, "t_rwm_pred").await;

    let snapshot_before = table.metadata().current_snapshot().unwrap();
    let snapshot_id_before = snapshot_before.snapshot_id();
    let manifest_list = snapshot_before
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);

    // Use a predicate that matches NO manifests (impossibly high length threshold).
    // Nothing is rewritten → no-op, no new snapshot.
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_data_file| "all".to_string()))
        .rewrite_if(Box::new(|manifest| manifest.manifest_length > 999_999_999));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Snapshot should be unchanged — no new snapshot created
    let snapshot_after = table.metadata().current_snapshot().unwrap();
    assert_eq!(
        snapshot_after.snapshot_id(),
        snapshot_id_before,
        "no-op rewrite should not create a new snapshot"
    );
}

/// Test: rewrite manifests with a predicate that matches ALL manifests.
///
/// 1. Create 3 manifests via fast_append.
/// 2. Use rewrite_manifests with cluster_by and rewrite_if that matches all manifests.
/// 3. Verify that all manifests are replaced and data is preserved.
#[tokio::test]
async fn test_rewrite_manifests_predicate_matches_all() {
    let (rest_catalog, table) = create_table_with_manifests(3, "t_rwm_pred_all").await;

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);

    // Predicate matches ALL manifests (length > 0 is always true)
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_data_file| "all".to_string()))
        .rewrite_if(Box::new(|manifest| manifest.manifest_length > 0));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(
        manifest_list.entries().len(),
        1,
        "all manifests should be consolidated"
    );

    let summary = &snapshot.summary().additional_properties;
    assert_eq!(summary.get("manifests-created").unwrap(), "1");
    assert_eq!(summary.get("manifests-kept").unwrap(), "0");
    assert_eq!(summary.get("manifests-replaced").unwrap(), "3");

    // Verify data still scannable
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3 * 4);
}

/// Test: rewrite manifests with multi-key clustering produces multiple output manifests.
///
/// 1. Create 4 manifests with fast_append.
/// 2. Rewrite with a cluster_by function that groups by even/odd file index.
/// 3. Verify that the output has the expected number of manifests.
#[tokio::test]
async fn test_rewrite_manifests_multi_cluster_keys() {
    let (rest_catalog, table) = create_table_with_manifests(4, "t_rwm_multi").await;

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 4);

    // Cluster by the last character of the file path (gives some variety)
    // In practice this partitions files into different groups based on their path.
    // We use a simple even/odd of the record_count to split into 2 groups.
    let tx = Transaction::new(&table);
    let action = tx.rewrite_manifests().cluster_by(Box::new(|data_file| {
        // Use the file path hash mod 2 to create two clusters
        let hash: usize = data_file.file_path().bytes().map(|b| b as usize).sum();
        format!("group_{}", hash % 2)
    }));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    // Should have at most 2 manifests (two cluster keys), at least 1
    let num_manifests = manifest_list.entries().len();
    assert!(
        (1..=2).contains(&num_manifests),
        "expected 1 or 2 manifests from 2 cluster keys, got {num_manifests}",
    );

    // Total data file entries across all manifests should be 4
    let mut total_entries = 0;
    for entry in manifest_list.entries() {
        let manifest = entry.load_manifest(table.file_io()).await.unwrap();
        total_entries += manifest.entries().len();
    }
    assert_eq!(total_entries, 4);

    // Verify data is intact
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4 * 4);
}

/// Test: delete a nonexistent manifest returns an error.
///
/// Construct a fake ManifestFile and try to delete it via rewrite_manifests.
/// The commit should fail with a DataInvalid error.
#[tokio::test]
async fn test_rewrite_manifests_delete_nonexistent_manifest() {
    let (rest_catalog, table) = create_table_with_manifests(1, "t_rwm_noexist").await;

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    let real_manifest = manifest_list.entries()[0].clone();

    // Construct a fake manifest by modifying the path
    let mut fake_manifest = real_manifest;
    fake_manifest.manifest_path = "s3://fake-bucket/nonexistent-manifest.avro".to_string();

    let tx = Transaction::new(&table);
    let action = tx.rewrite_manifests().delete_manifest(fake_manifest);
    let tx = action.apply(tx).unwrap();

    // The validation happens at commit time when the current snapshot's
    // manifest list is loaded and the deleted manifest path is checked.
    let commit_result = tx.commit(&rest_catalog).await;
    let err_msg = format!("{}", commit_result.unwrap_err());
    assert!(
        err_msg.contains("does not exist in the current snapshot"),
        "unexpected error: {err_msg}",
    );
}

/// Test: cluster_by combined with delete_manifest.
///
/// `delete_manifest` excludes a manifest from clustering so it's not reprocessed.
/// This is useful when a manifest has already been rewritten externally and must
/// be swapped: delete the old version and add the new one, while cluster_by
/// handles the rest.
///
/// 1. Create 4 manifests via fast_append, then consolidate into 1 via cluster_by.
///    The resulting manifest has `added_files_count=0, existing_files_count=4`.
/// 2. Use `delete_manifest` on the consolidated manifest and `add_manifest` to
///    re-add it (swap), while `cluster_by` processes nothing extra.
/// 3. Verify the table still has 1 manifest with all 4 entries.
#[tokio::test]
async fn test_rewrite_manifests_cluster_by_with_delete() {
    let (rest_catalog, mut table) = create_table_with_manifests(4, "t_rwm_cluster_del").await;

    // Step 1: consolidate 4 manifests into 1
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);

    let consolidated = manifest_list.entries()[0].clone();
    assert_eq!(consolidated.added_files_count, Some(0));
    assert_eq!(consolidated.existing_files_count, Some(4));

    // Step 2: delete + add the same manifest (swap), cluster_by is present
    // but has nothing to rewrite since the only manifest is deleted.
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .delete_manifest(consolidated.clone())
        .add_manifest(consolidated.clone())
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    // Verify: 1 manifest, 4 entries, all data intact
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(
        manifest_list.entries().len(),
        1,
        "should still have exactly 1 manifest after swap"
    );

    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4 * 4);
}

/// Test: rewrite manifests preserves data across multiple rewrite operations.
///
/// 1. Create 3 manifests.
/// 2. Rewrite all into one.
/// 3. Append a new file (creating 2 manifests).
/// 4. Rewrite again into one.
/// 5. Verify all data is present.
#[tokio::test]
async fn test_rewrite_manifests_multiple_rounds() {
    let (rest_catalog, mut table) = create_table_with_manifests(3, "t_rwm_rounds").await;

    // Round 1: consolidate 3 -> 1
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);

    // Append another file -> 2 manifests
    let data_file = write_data_file(&table).await;
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file);
    let tx = append_action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 2);

    // Round 2: consolidate 2 -> 1
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);

    // Verify all data: 3 original + 1 new = 4 files, 4 rows each = 16 rows
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4 * 4);
}

/// Test: rewrite manifests on a table with no snapshots (empty table).
///
/// When there are no manifests to rewrite, the action is a no-op and no new
/// snapshot is created.
#[tokio::test]
async fn test_rewrite_manifests_empty_table() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();
    let table_creation = TableCreation::builder()
        .name("t_rwm_empty".to_string())
        .schema(schema.clone())
        .build();
    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // No snapshots — nothing to rewrite, should be a no-op
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // No snapshot should have been created
    assert!(
        table.metadata().current_snapshot().is_none(),
        "no-op rewrite on empty table should not create a snapshot"
    );
}

/// Test: snapshot operation type is Replace after rewrite_manifests.
#[tokio::test]
async fn test_rewrite_manifests_snapshot_operation() {
    let (rest_catalog, table) = create_table_with_manifests(2, "t_rwm_op").await;

    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    assert_eq!(
        snapshot.summary().operation,
        iceberg::spec::Operation::Replace
    );
}

/// Test: rewrite manifests with custom snapshot properties.
///
/// Set custom properties on the rewrite action and verify they appear in the snapshot summary.
#[tokio::test]
async fn test_rewrite_manifests_snapshot_properties() {
    let (rest_catalog, table) = create_table_with_manifests(2, "t_rwm_props").await;

    let mut custom_props = std::collections::HashMap::new();
    custom_props.insert("custom-key".to_string(), "custom-value".to_string());
    custom_props.insert("rewrite-reason".to_string(), "compaction".to_string());

    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()))
        .set_snapshot_properties(custom_props);
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let summary = &snapshot.summary().additional_properties;
    assert_eq!(summary.get("custom-key").unwrap(), "custom-value");
    assert_eq!(summary.get("rewrite-reason").unwrap(), "compaction");
}

/// Test: rewrite manifests with no clustering and no add/delete is a no-op.
///
/// Without cluster_by or any manual add/delete, there is nothing to change.
/// The action should skip creating a new snapshot.
#[tokio::test]
async fn test_rewrite_manifests_no_clustering_noop() {
    let (rest_catalog, table) = create_table_with_manifests(3, "t_rwm_nocluster").await;

    let snapshot_before = table.metadata().current_snapshot().unwrap();
    let snapshot_id_before = snapshot_before.snapshot_id();
    let manifest_list = snapshot_before
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 3);

    // Without cluster_by or any add/delete, this is a no-op
    let tx = Transaction::new(&table);
    let action = tx.rewrite_manifests();
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // Snapshot should be unchanged
    let snapshot_after = table.metadata().current_snapshot().unwrap();
    assert_eq!(
        snapshot_after.snapshot_id(),
        snapshot_id_before,
        "no-op rewrite should not create a new snapshot"
    );

    // Verify data is intact
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3 * 4);
}

/// Test: delete_manifest matches by manifest_path, not full structural equality.
///
/// 1. Create a table with 3 manifests, then consolidate into 1 via cluster_by.
///    The consolidated manifest has added_files_count=0, existing_files_count=3.
/// 2. Clone the consolidated manifest, mutate optional fields (rows counts,
///    key_metadata, first_row_id), and pass it to delete_manifest().
/// 3. Add back the original (unmodified) consolidated manifest via add_manifest().
/// 4. The operation should succeed because identity matching uses manifest_path.
#[tokio::test]
async fn test_rewrite_manifests_delete_manifest_by_path() {
    let (rest_catalog, mut table) = create_table_with_manifests(3, "t_rwm_bypath").await;

    // Step 1: consolidate 3 manifests into 1. The resulting manifest has
    // added_files_count=0, existing_files_count=3, so it's valid for add_manifest.
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);

    let original_manifest = manifest_list.entries()[0].clone();
    assert_eq!(original_manifest.added_files_count, Some(0));
    assert_eq!(original_manifest.existing_files_count, Some(3));

    // Step 2: mutate optional fields to simulate a ManifestFile constructed
    // with incomplete metadata.
    let mut manifest_to_delete = original_manifest.clone();
    manifest_to_delete.added_rows_count = None;
    manifest_to_delete.existing_rows_count = None;
    manifest_to_delete.deleted_rows_count = Some(9999);
    manifest_to_delete.key_metadata = Some(vec![0xDE, 0xAD]);
    manifest_to_delete.first_row_id = None;

    // Step 3: delete with mutated copy, add back the original.
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .delete_manifest(manifest_to_delete)
        .add_manifest(original_manifest);
    let tx = action.apply(tx).unwrap();
    table = tx.commit(&rest_catalog).await.unwrap();

    // Verify table still has 1 manifest and data is intact
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    assert_eq!(manifest_list.entries().len(), 1);

    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3 * 4);
}

/// Test: partition spec id is preserved correctly in rewritten manifests.
///
/// After rewriting manifests with cluster_by, verify that the partition_spec_id
/// on the new manifest files matches the original.
#[tokio::test]
async fn test_rewrite_manifests_preserves_partition_spec_id() {
    let (rest_catalog, table) = create_table_with_manifests(3, "t_rwm_specid").await;

    // Capture the original partition spec IDs
    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    let original_spec_ids: Vec<i32> = manifest_list
        .entries()
        .iter()
        .map(|m| m.partition_spec_id)
        .collect();
    // All should be 0 for unpartitioned table
    for id in &original_spec_ids {
        assert_eq!(*id, 0);
    }

    // Rewrite manifests
    let tx = Transaction::new(&table);
    let action = tx
        .rewrite_manifests()
        .cluster_by(Box::new(|_| "all".to_string()));
    let tx = action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    let snapshot = table.metadata().current_snapshot().unwrap();
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();
    for manifest_entry in manifest_list.entries() {
        assert_eq!(
            manifest_entry.partition_spec_id, 0,
            "partition spec id should be preserved"
        );
    }
}

// NOTE: A test for partitioned tables is omitted because the current test
// infrastructure (write_data_file) creates unpartitioned data files with empty
// partition structs, which fast_append correctly rejects for partitioned tables.
// Partition spec ID preservation is already tested by
// test_rewrite_manifests_preserves_partition_spec_id (with spec id 0).
