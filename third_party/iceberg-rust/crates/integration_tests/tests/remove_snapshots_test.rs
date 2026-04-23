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
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::properties::WriterProperties;

#[tokio::test]
async fn test_expire_snapshots_by_count() {
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
    for i in 0..10 {
        // Create a new data file writer for each iteration
        let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();
        // Create different data for each iteration
        let col1 = StringArray::from(vec![
            Some(format!("foo_{i}")),
            Some(format!("bar_{i}")),
            None,
            Some(format!("baz_{i}")),
        ]);
        let col2 = Int32Array::from(vec![Some(i), Some(i + 1), Some(i + 2), Some(i + 3)]);
        let col3 = BooleanArray::from(vec![
            Some(i % 2 == 0),
            Some(i % 2 == 1),
            None,
            Some(i % 3 == 0),
        ]);
        let batch = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(col1) as ArrayRef,
            Arc::new(col2) as ArrayRef,
            Arc::new(col3) as ArrayRef,
        ])
        .unwrap();

        // Write the unique data and get the data file
        data_file_writer.write(batch.clone()).await.unwrap();
        let data_file = data_file_writer.close().await.unwrap();

        let tx = Transaction::new(&table);
        let append_action = tx.fast_append();
        let tx = append_action.add_data_files(data_file).apply(tx).unwrap();
        table = tx.commit(&rest_catalog).await.unwrap();
    }

    // check snapshot count
    let snapshot_counts = table.metadata().snapshots().count();
    assert_eq!(10, snapshot_counts);

    let tx = Transaction::new(&table);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let remove_action = tx.expire_snapshot().retain_last(5).expire_older_than(now);
    let tx = remove_action.apply(tx).unwrap();
    let t = tx.commit(&rest_catalog).await.unwrap();
    assert_eq!(5, t.metadata().snapshots().count());
}
