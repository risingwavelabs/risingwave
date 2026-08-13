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

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use iceberg::delete_vector::DeleteVector;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFileBuilder, DataFileFormat, FormatVersion, Literal, NestedField, PrimitiveType, Schema,
    SortOrder, Struct, TableMetadataBuilder, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::{Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent};
use risingwave_connector::sink::iceberg::{
    read_position_deletes_from_file, write_dv_puffin_file, write_parquet_position_delete_file,
};
use risingwave_pb::connector_service::SinkMetadata;
use tempfile::TempDir;

use super::*;

fn report(
    role: PbIcebergPkIndexSinkRole,
    metadata: Option<SinkMetadata>,
) -> PbIcebergPkIndexSinkMetadata {
    PbIcebergPkIndexSinkMetadata {
        role: role as i32,
        metadata,
        ..Default::default()
    }
}

fn serialized_file(path: &str) -> SerializedDataFile {
    serde_json::from_value(serde_json::json!({
        "content": 0,
        "file_path": path,
        "file_format": "PARQUET",
        "partition": {},
        "record_count": 1,
        "file_size_in_bytes": 1
    }))
    .unwrap()
}

fn writer_metadata(
    schema_id: i32,
    partition_spec_id: i32,
    data_files: Vec<SerializedDataFile>,
) -> SinkMetadata {
    SinkMetadata::try_from(&IcebergCommitResult {
        schema_id,
        partition_spec_id,
        data_files,
    })
    .unwrap()
}

fn merger_metadata(
    schema_id: i32,
    partition_spec_id: i32,
    delete_files: Vec<SerializedDataFile>,
    overwrite_files: Vec<SerializedDataFile>,
) -> SinkMetadata {
    SinkMetadata::try_from(&IcebergPositionDeleteCommitResult {
        schema_id,
        partition_spec_id,
        delete_files,
        overwrite_files,
    })
    .unwrap()
}

fn coalesce_test_config() -> IcebergConfig {
    IcebergConfig::from_btreemap(BTreeMap::from([
        ("connector".to_owned(), "iceberg".to_owned()),
        ("type".to_owned(), "upsert".to_owned()),
        ("primary_key".to_owned(), "id".to_owned()),
        ("warehouse.path".to_owned(), "memory://coalesce".to_owned()),
        ("catalog.type".to_owned(), "storage".to_owned()),
        ("database.name".to_owned(), "db".to_owned()),
        ("table.name".to_owned(), "table".to_owned()),
    ]))
    .unwrap()
}

fn coalesce_test_table(temp_dir: &TempDir, format_version: FormatVersion) -> Result<Table> {
    coalesce_test_table_with_partitioning(temp_dir, format_version, false)
}

fn coalesce_test_table_with_partitioning(
    temp_dir: &TempDir,
    format_version: FormatVersion,
    partitioned: bool,
) -> Result<Table> {
    let location = format!("file://{}", temp_dir.path().display());
    coalesce_test_table_at_location(&location, format_version, partitioned)
}

fn coalesce_test_table_at_location(
    location: &str,
    format_version: FormatVersion,
    partitioned: bool,
) -> Result<Table> {
    let location = location.to_owned();
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?;
    let partition_spec = if partitioned {
        UnboundPartitionSpec::builder()
            .with_spec_id(1)
            .add_partition_fields(vec![UnboundPartitionField {
                source_id: 1,
                field_id: Some(1000),
                name: "id".to_owned(),
                transform: Transform::Identity,
            }])?
            .build()
    } else {
        UnboundPartitionSpec::builder().build()
    };
    let metadata = TableMetadataBuilder::new(
        schema,
        partition_spec,
        SortOrder::unsorted_order(),
        location.clone(),
        format_version,
        HashMap::new(),
    )?
    .build()?
    .metadata;
    Ok(Table::builder()
        .identifier(TableIdent::new(
            NamespaceIdent::new("db".to_owned()),
            "table".to_owned(),
        ))
        .file_io(FileIO::from_path(&location)?.build()?)
        .metadata(metadata)
        .build()?)
}

fn output_data_file(table: &Table, name: &str) -> Result<DataFile> {
    output_data_file_with_partition(table, name, Struct::empty())
}

fn output_data_file_with_partition(
    table: &Table,
    name: &str,
    partition: Struct,
) -> Result<DataFile> {
    Ok(DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/{name}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .partition(partition)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .record_count(10)
        .file_size_in_bytes(1)
        .build()?)
}

async fn write_test_position_delete(
    table: &Table,
    config: &IcebergConfig,
    prefix: &str,
    referenced_path: &str,
    positions: impl IntoIterator<Item = u64>,
) -> Result<DataFile> {
    write_test_position_delete_with_partition(
        table,
        config,
        prefix,
        referenced_path,
        positions,
        None,
    )
    .await
}

async fn write_test_position_delete_with_partition(
    table: &Table,
    config: &IcebergConfig,
    prefix: &str,
    referenced_path: &str,
    positions: impl IntoIterator<Item = u64>,
    partition_key: Option<&PartitionKey>,
) -> Result<DataFile> {
    let format = if table.metadata().format_version() >= FormatVersion::V3 {
        DataFileFormat::Puffin
    } else {
        DataFileFormat::Parquet
    };
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(prefix.to_owned(), None, format);
    let delete_vector = DeleteVector::from_iter(positions);
    if format == DataFileFormat::Puffin {
        write_dv_puffin_file(
            table,
            &location_generator,
            &file_name_generator,
            referenced_path.to_owned(),
            &delete_vector,
            partition_key,
        )
        .await
    } else {
        write_parquet_position_delete_file(
            table,
            &location_generator,
            &file_name_generator,
            config,
            referenced_path.to_owned(),
            &delete_vector,
            partition_key,
        )
        .await
    }
}

#[derive(Debug)]
struct ReloadingTestCatalog {
    table: Mutex<Table>,
}

impl ReloadingTestCatalog {
    fn new(table: Table) -> Self {
        Self {
            table: Mutex::new(table),
        }
    }

    fn set_table(&self, table: Table) {
        *self.table.lock().unwrap() = table;
    }
}

#[async_trait]
impl Catalog for ReloadingTestCatalog {
    async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
        Ok(self.table.lock().unwrap().clone())
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        unreachable!("test only loads a table")
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        unreachable!("test only loads a table")
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        unreachable!("test only loads a table")
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        unreachable!("test only loads a table")
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        unreachable!("test only loads a table")
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }

    async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
        unreachable!("test only loads a table")
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
        unreachable!("test only loads a table")
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }

    async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
        unreachable!("test only loads a table")
    }
}

fn test_coordinator(
    table: Table,
    catalog: Arc<dyn Catalog>,
    config: IcebergConfig,
    db: DatabaseConnection,
) -> IcebergPkIndexSinkCoordinator {
    IcebergPkIndexSinkCoordinator {
        sink_id: SinkId::new(42),
        db,
        catalog,
        table,
        iceberg_config: config,
        target_branch: "main".to_owned(),
        retry_num: 0,
        waiting_commit: None,
        prev_committed_epoch: None,
    }
}

#[tokio::test]
async fn reload_table_for_pre_commit_observes_v2_to_v3_upgrade() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let location = format!("file://{}", temp_dir.path().display());
    let stale_v2 = coalesce_test_table_at_location(&location, FormatVersion::V2, false)?;
    let current_v3 = coalesce_test_table_at_location(&location, FormatVersion::V3, false)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(stale_v2.clone()));
    let mut coordinator = test_coordinator(
        stale_v2,
        catalog.clone(),
        coalesce_test_config(),
        DatabaseConnection::Disconnected,
    );

    catalog.set_table(current_v3);
    coordinator.reload_table_for_pre_commit().await?;

    assert_eq!(
        coordinator.table.metadata().format_version(),
        FormatVersion::V3
    );
    Ok(())
}

#[tokio::test]
async fn compactor_output_ids_are_validated_before_physical_rewrite() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = coalesce_test_table(&temp_dir, FormatVersion::V3)?;
    let catalog = Arc::new(ReloadingTestCatalog::new(table.clone()));
    let mut coordinator = test_coordinator(
        table,
        catalog,
        coalesce_test_config(),
        DatabaseConnection::Disconnected,
    );

    let error = coordinator
        .pre_commit(
            1,
            vec![],
            Some(CompactionOverwrite {
                sink_id: SinkId::new(42),
                epoch: 1,
                schema_id: 99,
                partition_spec_id: 0,
                output_files: vec![],
                input_file_paths: vec![],
                read_snapshot_id: 1,
            }),
        )
        .await
        .expect_err("compactor schema mismatch should fail");

    assert!(error.to_string().contains("compactor output"));
    assert!(coordinator.waiting_commit.is_none());
    Ok(())
}

#[test]
fn compactor_output_ids_match_current_table() -> Result<()> {
    validate_compactor_output_ids(3, 4, 3, 4)
}

#[test]
fn compactor_output_partition_spec_mismatch_is_rejected() {
    let error = validate_compactor_output_ids(3, 5, 3, 4)
        .expect_err("compactor partition spec mismatch should fail");
    assert!(error.to_string().contains("partition_spec_id 5"));
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_unions_v2_and_v3_artifacts() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let resolver =
            write_test_position_delete(&table, &config, "resolver", output.file_path(), [1, 3])
                .await?;
        let merger =
            write_test_position_delete(&table, &config, "merger", output.file_path(), [3, 5])
                .await?;
        let source_paths = HashSet::from([
            resolver.file_path().to_owned(),
            merger.file_path().to_owned(),
        ]);
        let mut added_delete_files = vec![resolver, merger];

        let result = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            std::slice::from_ref(&output),
            &mut added_delete_files,
        )
        .await?;

        assert_eq!(added_delete_files.len(), 1);
        let replacement = &added_delete_files[0];
        assert_eq!(
            replacement.referenced_data_file().as_deref(),
            Some(output.file_path())
        );
        assert_eq!(replacement.partition(), output.partition());
        assert_eq!(
            read_position_deletes_from_file(table.file_io(), replacement).await?,
            DeleteVector::from([1, 3, 5])
        );
        assert_eq!(HashSet::from_iter(result.discarded_paths), source_paths);
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_preserves_singleton_and_unrelated() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let singleton =
            write_test_position_delete(&table, &config, "singleton", output.file_path(), [7])
                .await?;
        let unrelated = write_test_position_delete(
            &table,
            &config,
            "unrelated",
            "file:///unrelated-data.parquet",
            [9],
        )
        .await?;
        let expected_paths = vec![
            singleton.file_path().to_owned(),
            unrelated.file_path().to_owned(),
        ];
        let mut delete_files = vec![singleton, unrelated];

        let result = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut delete_files,
        )
        .await?;

        assert!(result.discarded_paths.is_empty());
        assert_eq!(
            delete_files
                .iter()
                .map(|file| file.file_path())
                .collect::<Vec<_>>(),
            expected_paths
        );
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_groups_multiple_outputs() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let table = coalesce_test_table(&temp_dir, FormatVersion::V3)?;
    let config = coalesce_test_config();
    let outputs = vec![
        output_data_file(&table, "output-a")?,
        output_data_file(&table, "output-b")?,
    ];
    let mut delete_files = Vec::new();
    for (prefix, output, positions) in [
        ("a1", &outputs[0], [1, 3]),
        ("a2", &outputs[0], [3, 5]),
        ("b1", &outputs[1], [2, 4]),
        ("b2", &outputs[1], [4, 6]),
    ] {
        delete_files.push(
            write_test_position_delete(&table, &config, prefix, output.file_path(), positions)
                .await?,
        );
    }

    let result = coalesce_position_delete_files(
        &table,
        &config,
        SinkId::new(42),
        99,
        &outputs,
        &mut delete_files,
    )
    .await?;

    assert_eq!(result.discarded_paths.len(), 4);
    assert_eq!(delete_files.len(), 2);
    for (output, expected) in [
        (&outputs[0], DeleteVector::from([1, 3, 5])),
        (&outputs[1], DeleteVector::from([2, 4, 6])),
    ] {
        let replacement = delete_files
            .iter()
            .find(|file| file.referenced_data_file().as_deref() == Some(output.file_path()))
            .expect("each output should have one replacement");
        assert_eq!(
            read_position_deletes_from_file(table.file_io(), replacement).await?,
            expected
        );
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_preserves_partition() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table_with_partitioning(&temp_dir, format_version, true)?;
        let config = coalesce_test_config();
        let partition = Struct::from_iter([Some(Literal::long(11))]);
        let output = output_data_file_with_partition(&table, "output", partition.clone())?;
        let partition_key = PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            partition.clone(),
        );
        let mut delete_files = vec![
            write_test_position_delete_with_partition(
                &table,
                &config,
                "resolver",
                output.file_path(),
                [1],
                Some(&partition_key),
            )
            .await?,
            write_test_position_delete_with_partition(
                &table,
                &config,
                "merger",
                output.file_path(),
                [2],
                Some(&partition_key),
            )
            .await?,
        ];

        coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut delete_files,
        )
        .await?;

        assert_eq!(delete_files[0].partition(), &partition);
        assert_eq!(
            delete_files[0].partition_spec_id(),
            table.metadata().default_partition_spec_id()
        );
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_retries_same_path_without_deleting_sources() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let sources = vec![
            write_test_position_delete(&table, &config, "resolver", output.file_path(), [1, 3])
                .await?,
            write_test_position_delete(&table, &config, "merger", output.file_path(), [3, 5])
                .await?,
        ];
        let source_paths = sources
            .iter()
            .map(|file| file.file_path().to_owned())
            .collect::<Vec<_>>();
        let mut first_attempt = sources.clone();
        coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            std::slice::from_ref(&output),
            &mut first_attempt,
        )
        .await?;
        let first_path = first_attempt[0].file_path().to_owned();
        let mut second_attempt = sources;
        coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut second_attempt,
        )
        .await?;

        assert_eq!(second_attempt[0].file_path(), first_path);
        for path in source_paths {
            assert!(table.file_io().exists(path).await?);
        }
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_rejects_replacement_path_collision_before_io() -> Result<()>
{
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let sources = vec![
            write_test_position_delete(&table, &config, "resolver", output.file_path(), [1])
                .await?,
            write_test_position_delete(&table, &config, "merger", output.file_path(), [2]).await?,
        ];
        let mut first_attempt = sources.clone();
        coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            std::slice::from_ref(&output),
            &mut first_attempt,
        )
        .await?;
        let replacement = &first_attempt[0];
        let replacement_path = replacement.file_path().to_owned();
        let retained_with_replacement_path = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(replacement_path.clone())
            .file_format(replacement.file_format())
            .partition(replacement.partition().clone())
            .partition_spec_id(replacement.partition_spec_id())
            .record_count(1)
            .file_size_in_bytes(1)
            .referenced_data_file(Some("file:///unrelated-data.parquet".to_owned()))
            .build()?;
        let mut second_attempt = sources;
        second_attempt.push(retained_with_replacement_path);

        let error = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut second_attempt,
        )
        .await
        .err()
        .expect("replacement path collision should fail");

        assert!(error.to_string().contains("replacement path"));
        assert!(table.file_io().exists(replacement_path).await?);
    }
    Ok(())
}

#[cfg(not(madsim))]
#[tokio::test]
async fn coalesce_position_delete_files_rejects_invalid_artifacts() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let source =
            write_test_position_delete(&table, &config, "duplicate", output.file_path(), [1])
                .await?;
        let mut delete_files = vec![source.clone(), source];
        let error = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            std::slice::from_ref(&output),
            &mut delete_files,
        )
        .await
        .err()
        .expect("duplicate path should fail");
        assert!(error.to_string().contains("duplicate"));

        let selected =
            write_test_position_delete(&table, &config, "selected", output.file_path(), [1])
                .await?;
        let second_selected =
            write_test_position_delete(&table, &config, "second-selected", output.file_path(), [2])
                .await?;
        let retained_with_same_path = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(selected.file_path().to_owned())
            .file_format(selected.file_format())
            .partition(selected.partition().clone())
            .partition_spec_id(selected.partition_spec_id())
            .record_count(1)
            .file_size_in_bytes(1)
            .referenced_data_file(Some("file:///unrelated-data.parquet".to_owned()))
            .build()?;
        let mut delete_files = vec![selected, second_selected, retained_with_same_path];
        let error = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut delete_files,
        )
        .await
        .err()
        .expect("selected and retained duplicate path should fail");
        assert!(error.to_string().contains("duplicate"));
    }
    Ok(())
}

#[tokio::test]
async fn coalesce_position_delete_files_rejects_missing_reference() -> Result<()> {
    for format_version in [FormatVersion::V2, FormatVersion::V3] {
        let temp_dir = tempfile::tempdir()?;
        let table = coalesce_test_table(&temp_dir, format_version)?;
        let config = coalesce_test_config();
        let output = output_data_file(&table, "output")?;
        let mut delete_files = vec![
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(format!(
                    "{}/data/missing-reference.parquet",
                    table.metadata().location()
                ))
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty())
                .partition_spec_id(table.metadata().default_partition_spec_id())
                .record_count(1)
                .file_size_in_bytes(1)
                .build()?,
        ];

        let result = coalesce_position_delete_files(
            &table,
            &config,
            SinkId::new(42),
            99,
            &[output],
            &mut delete_files,
        )
        .await;

        let error = result.err().expect("missing reference should fail");
        assert!(error.to_string().contains("missing referenced_data_file"));
    }
    Ok(())
}

#[test]
fn pre_commit_state_round_trip_preserves_format_version() -> Result<()> {
    let input = IcebergPkIndexSinkAggResult {
        schema_id: 3,
        partition_spec_id: 4,
        format_version: Some(FormatVersion::V3),
        data_files: vec![serialized_file("data")],
        delete_files: vec![serialized_file("delete")],
        overwrite_files: vec![serialized_file("overwrite")],
    };

    let blob = encode_pre_commit_state(&input, 99)?;
    let (decoded, snapshot_id) = decode_pre_commit_state(&blob)?;

    assert_eq!(snapshot_id, 99);
    assert_eq!(decoded.format_version, Some(FormatVersion::V3));
    assert_eq!(decoded.schema_id, 3);
    assert_eq!(decoded.partition_spec_id, 4);
    assert_eq!(decoded.data_files.len(), 1);
    assert_eq!(decoded.delete_files.len(), 1);
    assert_eq!(decoded.overwrite_files.len(), 1);
    Ok(())
}

#[test]
fn pre_commit_state_decodes_legacy_state_without_format_version() -> Result<()> {
    let legacy_aggregate = serde_json::json!({
        "schema_id": 3,
        "partition_spec_id": 4,
        "data_files": [],
        "delete_files": [],
        "overwrite_files": []
    });
    let blob = PbIcebergPkIndexPreCommitState {
        agg_result: serde_json::to_vec(&legacy_aggregate)?,
        snapshot_id: 99,
    }
    .encode_to_vec();

    let (decoded, snapshot_id) = decode_pre_commit_state(&blob)?;

    assert_eq!(snapshot_id, 99);
    assert_eq!(decoded.format_version, None);
    Ok(())
}

#[test]
fn ordinary_aggregate_accepts_table_ids() {
    let aggregate = aggregate_ordinary_reports(
        &[
            report(
                PbIcebergPkIndexSinkRole::Writer,
                Some(writer_metadata(
                    3,
                    4,
                    vec![serialized_file("ordinary-data")],
                )),
            ),
            report(
                PbIcebergPkIndexSinkRole::PositionDeleteMerger,
                Some(merger_metadata(3, 4, vec![], vec![])),
            ),
        ],
        3,
        4,
        FormatVersion::V3,
    )
    .unwrap()
    .expect("ordinary metadata should produce an aggregate");

    assert_eq!(aggregate.schema_id, 3);
    assert_eq!(aggregate.partition_spec_id, 4);
}

#[test]
fn ordinary_aggregate_rejects_schema_id_different_from_table() {
    let error = aggregate_ordinary_reports(
        &[report(
            PbIcebergPkIndexSinkRole::Writer,
            Some(writer_metadata(2, 4, vec![serialized_file("data")])),
        )],
        3,
        4,
        FormatVersion::V3,
    )
    .err()
    .expect("schema mismatch should fail");
    assert!(error.to_string().contains("schema_id 2"));
}

#[test]
fn ordinary_aggregate_rejects_partition_spec_id_different_from_table() {
    let error = aggregate_ordinary_reports(
        &[report(
            PbIcebergPkIndexSinkRole::Writer,
            Some(writer_metadata(3, 5, vec![serialized_file("data")])),
        )],
        3,
        4,
        FormatVersion::V3,
    )
    .err()
    .expect("partition spec mismatch should fail");
    assert!(error.to_string().contains("partition_spec_id 5"));
}

#[test]
fn ordinary_aggregate_validates_non_empty_metadata_mixed_with_empty() {
    let error = aggregate_ordinary_reports(
        &[
            report(PbIcebergPkIndexSinkRole::Writer, None),
            report(
                PbIcebergPkIndexSinkRole::PositionDeleteMerger,
                Some(merger_metadata(2, 4, vec![], vec![])),
            ),
        ],
        3,
        4,
        FormatVersion::V3,
    )
    .err()
    .expect("non-empty metadata schema mismatch should fail");
    assert!(error.to_string().contains("schema_id 2"));
}

#[test]
fn ordinary_aggregate_all_empty_is_noop_without_ids() {
    let aggregate = aggregate_ordinary_reports(
        &[
            report(PbIcebergPkIndexSinkRole::Writer, None),
            report(PbIcebergPkIndexSinkRole::PositionDeleteMerger, None),
        ],
        3,
        4,
        FormatVersion::V3,
    )
    .unwrap();
    assert!(aggregate.is_none());
}

#[test]
fn combine_compaction_aggregate_merges_every_input() {
    let ordinary = aggregate_reports(&[
        report(
            PbIcebergPkIndexSinkRole::Writer,
            Some(
                SinkMetadata::try_from(&IcebergCommitResult {
                    schema_id: 3,
                    partition_spec_id: 4,
                    data_files: vec![serialized_file("ordinary-data")],
                })
                .unwrap(),
            ),
        ),
        report(
            PbIcebergPkIndexSinkRole::PositionDeleteMerger,
            Some(merger_metadata(
                3,
                4,
                vec![serialized_file("ordinary-delete")],
                vec![serialized_file("ordinary-overwrite")],
            )),
        ),
    ])
    .unwrap();
    let resolver = decode_compaction_resolver_delete_reports([report(
        PbIcebergPkIndexSinkRole::CompactionResolver,
        Some(merger_metadata(
            3,
            4,
            vec![serialized_file("resolver-delete")],
            vec![],
        )),
    )])
    .unwrap();

    let merged = combine_compaction_aggregate(
        3,
        4,
        FormatVersion::V3,
        ordinary,
        vec![serialized_file("compactor-output")],
        resolver,
        vec![serialized_file("resolved-overwrite")],
    )
    .unwrap();

    assert_eq!(merged.schema_id, 3);
    assert_eq!(merged.partition_spec_id, 4);
    assert_eq!(merged.data_files.len(), 2);
    assert_eq!(merged.delete_files.len(), 2);
    assert_eq!(merged.overwrite_files.len(), 2);
}

#[test]
fn combine_compaction_aggregate_rejects_report_ids_different_from_table() {
    let ordinary = aggregate_reports(&[report(
        PbIcebergPkIndexSinkRole::PositionDeleteMerger,
        Some(merger_metadata(2, 4, vec![], vec![])),
    )])
    .unwrap();
    let error =
        combine_compaction_aggregate(3, 4, FormatVersion::V3, ordinary, vec![], None, vec![])
            .err()
            .expect("schema mismatch should fail");
    assert!(error.to_string().contains("schema_id 2"));
}

#[test]
fn combine_compaction_aggregate_rejects_resolver_ids_different_from_table() {
    let resolver = decode_compaction_resolver_delete_reports([report(
        PbIcebergPkIndexSinkRole::CompactionResolver,
        Some(merger_metadata(3, 5, vec![], vec![])),
    )])
    .unwrap();
    let error =
        combine_compaction_aggregate(3, 4, FormatVersion::V3, None, vec![], resolver, vec![])
            .err()
            .expect("partition spec mismatch should fail");
    assert!(error.to_string().contains("partition_spec_id 5"));
}

#[test]
fn aggregate_reports_ignores_empty_ordinary_metadata() {
    let merger_metadata = SinkMetadata::try_from(&IcebergPositionDeleteCommitResult {
        schema_id: 3,
        partition_spec_id: 4,
        delete_files: vec![],
        overwrite_files: vec![],
    })
    .unwrap();
    let merged = aggregate_reports(&[
        report(PbIcebergPkIndexSinkRole::Writer, None),
        report(
            PbIcebergPkIndexSinkRole::PositionDeleteMerger,
            Some(merger_metadata),
        ),
    ])
    .unwrap()
    .expect("merger metadata should produce an aggregate");

    assert_eq!(merged.schema_id, 3);
    assert_eq!(merged.partition_spec_id, 4);
}

#[test]
fn aggregate_reports_returns_none_for_only_empty_metadata() {
    let merged = aggregate_reports(&[
        report(PbIcebergPkIndexSinkRole::Writer, None),
        report(PbIcebergPkIndexSinkRole::PositionDeleteMerger, None),
    ])
    .unwrap();
    assert!(merged.is_none());
}

#[test]
fn aggregate_reports_rejects_empty_input() {
    assert!(aggregate_reports(&[]).is_err());
}
