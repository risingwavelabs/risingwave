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

// Iceberg tables require a native Tokio runtime, which madsim does not provide.
#![cfg(not(madsim))]

use std::time::Duration;

use iceberg::TableIdent;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId, TableId};
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_connector::connector_common::mock_catalog::MockCatalog;
use risingwave_connector::source::iceberg::update_planner::IcebergUpdateBinding;
use risingwave_connector::source::iceberg::update_state::{
    IcebergUpdateAssignment, IcebergUpdateFetchState, IcebergUpdateListState,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_sdk::key::TableKeyRange;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_storage::error::StorageResult;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::panic_store::PanicStateStoreIter;
use risingwave_storage::store::*;
use risingwave_storage::table::batch_table::BatchTable;
use serde_json::json;
use tokio::sync::watch;

use super::fetch::*;
use super::list::*;
use super::*;
use crate::executor::BarrierKind;
use crate::executor::source::SourceStateTableHandler;
use crate::executor::source::state_table_handler::default_source_internal_table;

// Gate committed reads independently of local writes. Barriers must continue to flow
// while List waits for phase completion. File reads, state and vnode routing remain real.
#[derive(Clone)]
struct GatedStore {
    inner: MemoryStateStore,
    committed: watch::Receiver<u64>,
}

impl StateStoreReadLog for GatedStore {
    type ChangeLogIter = PanicStateStoreIter<StateStoreReadLogItem>;

    async fn next_epoch(&self, _: u64, _: NextEpochOptions) -> StorageResult<u64> {
        unreachable!()
    }

    async fn iter_log(
        &self,
        _: (u64, u64),
        _: TableKeyRange,
        _: ReadLogOptions,
    ) -> StorageResult<Self::ChangeLogIter> {
        unreachable!()
    }
}

impl StateStore for GatedStore {
    type Local = <MemoryStateStore as StateStore>::Local;
    type ReadSnapshot = <MemoryStateStore as StateStore>::ReadSnapshot;
    type VectorWriter = <MemoryStateStore as StateStore>::VectorWriter;

    async fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
        options: TryWaitEpochOptions,
    ) -> StorageResult<()> {
        let HummockReadEpoch::Committed(epoch) = epoch else {
            panic!("completion read bypassed the global commit fence");
        };
        assert_eq!(options.table_id, TableId::new(42));
        self.committed
            .clone()
            .wait_for(|committed| *committed >= epoch)
            .await
            .unwrap();
        Ok(())
    }

    async fn new_local(&self, options: NewLocalOptions) -> Self::Local {
        self.inner.new_local(options).await
    }

    async fn new_read_snapshot(
        &self,
        epoch: HummockReadEpoch,
        options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        self.try_wait_epoch(
            epoch,
            TryWaitEpochOptions {
                table_id: options.table_id,
            },
        )
        .await?;
        self.inner.new_read_snapshot(epoch, options).await
    }

    async fn new_vector_writer(&self, options: NewVectorWriterOptions) -> Self::VectorWriter {
        self.inner.new_vector_writer(options).await
    }
}

fn list_state_catalog(id: u32) -> risingwave_pb::catalog::Table {
    let mut catalog = default_source_internal_table(id);
    catalog.columns.remove(0);
    catalog.columns[0].column_desc.as_mut().unwrap().column_id = 0;
    catalog.pk.clear();
    catalog.value_indices = vec![0];
    catalog
}

#[test]
fn list_state_requires_a_single_json_value_without_a_key() -> anyhow::Result<()> {
    let mut catalog = list_state_catalog(43);
    validate_update_state_table(&catalog, true)?;
    assert!(validate_update_state_table(&catalog, false).is_err());
    catalog.pk = default_source_internal_table(43).pk;
    assert!(validate_update_state_table(&catalog, true).is_err());
    catalog.pk.clear();
    catalog.distribution_key = vec![0];
    assert!(validate_update_state_table(&catalog, true).is_err());
    catalog.distribution_key.clear();
    catalog.value_indices.clear();
    assert!(validate_update_state_table(&catalog, true).is_err());
    Ok(())
}
/// The Fetch state layout planned by the frontend: `(route, seq, state)` keyed by
/// `(route, seq)` and distributed by `route`.
fn fetch_state_catalog(id: u32) -> risingwave_pb::catalog::Table {
    let mut catalog = default_source_internal_table(id);
    let template = catalog.columns[1].clone();
    catalog.columns = [DataType::Int64, DataType::Int64, DataType::Jsonb]
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| {
            let mut column = template.clone();
            let desc = column.column_desc.as_mut().unwrap();
            desc.column_type = Some(data_type.to_protobuf());
            desc.column_id = index as i32;
            column
        })
        .collect();
    let route = catalog.pk[0];
    catalog.pk = vec![
        route,
        risingwave_pb::common::PbColumnOrder {
            column_index: 1,
            ..route
        },
    ];
    catalog.value_indices = vec![0, 1, 2];
    catalog.distribution_key = vec![0];
    catalog.maybe_vnode_count = Some(4);
    catalog
}

async fn fetch_state<S: StateStore>(
    catalog: &risingwave_pb::catalog::Table,
    store: S,
) -> SourceStateTableHandler<S> {
    SourceStateTableHandler::from_table_catalog_with_vnodes(
        catalog,
        store,
        Some(Arc::new(Bitmap::ones(4))),
    )
    .await
}

async fn put_task<S: StateStore>(
    state: &mut SourceStateTableHandler<S>,
    task: &IcebergUpdateFetchState,
) -> anyhow::Result<()> {
    let table = state.state_table_mut();
    let row = task_row(&task.assignment, task)?;
    match table.get_row(task_pk(&task.assignment)).await? {
        Some(old) => table.update(old, row),
        None => table.insert(row),
    }
    Ok(())
}

async fn delete_task<S: StateStore>(
    state: &mut SourceStateTableHandler<S>,
    assignment: &IcebergUpdateAssignment,
) -> anyhow::Result<()> {
    let table = state.state_table_mut();
    if let Some(old) = table.get_row(task_pk(assignment)).await? {
        table.delete(old);
    }
    Ok(())
}

#[test]
fn fetch_state_requires_route_seq_rows_distributed_by_route() -> anyhow::Result<()> {
    let catalog = fetch_state_catalog(42);
    validate_update_state_table(&catalog, false)?;
    assert!(validate_update_state_table(&catalog, true).is_err());
    let mut legacy = default_source_internal_table(42);
    legacy.distribution_key = vec![0];
    assert!(validate_update_state_table(&legacy, false).is_err());
    let mut by_seq = catalog.clone();
    by_seq.distribution_key = vec![1];
    assert!(validate_update_state_table(&by_seq, false).is_err());
    let mut by_route = catalog;
    by_route.pk.truncate(1);
    assert!(validate_update_state_table(&by_route, false).is_err());
    Ok(())
}

fn descriptor(table: &risingwave_pb::catalog::Table) -> StorageTableDesc {
    StorageTableDesc {
        table_id: table.id,
        columns: table
            .columns
            .iter()
            .map(|column| column.column_desc.clone().unwrap())
            .collect(),
        pk: table.pk.clone(),
        value_indices: table
            .value_indices
            .iter()
            .map(|index| *index as u32)
            .collect(),
        dist_key_in_pk_indices: table
            .distribution_key
            .iter()
            .map(|index| *index as u32)
            .collect(),
        maybe_vnode_count: table.maybe_vnode_count,
        ..Default::default()
    }
}

fn reader<S: StateStore>(store: S, desc: &StorageTableDesc) -> BatchTable<S> {
    BatchTable::new_partial(
        store,
        desc.columns
            .iter()
            .map(|column| ColumnId::new(column.column_id))
            .collect(),
        Some(Arc::new(Bitmap::ones(desc.vnode_count()))),
        desc,
    )
}

fn list_context<S: StateStore>(
    properties: IcebergProperties,
    fetch_reader: BatchTable<S>,
) -> Arc<ListContext<S>> {
    Arc::new(ListContext {
        properties,
        downstream_columns: output_columns(),
        batch_size: 2,
        ordered_max_tasks: 0,
        job_id: 42,
        fetch_reader,
    })
}

fn fixture() -> anyhow::Result<(
    IcebergUpdateBinding,
    IcebergUpdateListState,
    IcebergUpdateAssignment,
)> {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "key",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()?;
    let binding = json!({
        "table_uuid": "00000000-0000-0000-0000-000000000001",
        "bootstrap_snapshot_id": 1, "schema": schema,
        "project_field_ids": [1], "name_mapping": null,
    });
    let task = json!({
        "id": {"table_uuid": binding["table_uuid"], "snapshot_id": 2,
            "phase": "Delete", "data_file_path": "file:///fixture.parquet"},
        "parent_snapshot_id": 1, "binding": binding, "parent_deletes": [],
        "file": {"path": "file:///fixture.parquet", "size": 100, "record_count": 10,
            "sequence_number": 1, "file_sequence_number": 1, "first_row_id": null,
            "partition": {}, "partition_spec": {"spec-id": 0, "fields": []}, "deletes": []}
    });
    let cursor = json!({"snapshot_id": 2, "parent_snapshot_id": 1,
        "bootstrap": false, "phase": "Insert", "after_path": null});
    // The Delete phase of snapshot 2 fanned out and has not been confirmed yet.
    let state = serde_json::from_value(json!({
        "job_id": 42, "binding": binding, "bootstrap_complete": true, "dispatched_snapshot": 1,
        "cursor": cursor, "next_seq": 8,
        "fence": {"epoch": test_epoch(1), "ordered": false}
    }))?;
    Ok((
        serde_json::from_value(binding)?,
        state,
        serde_json::from_value(json!({"job_id": 42, "route": 7, "seq": 7, "task": task}))?,
    ))
}

fn output_columns() -> Vec<ColumnCatalog> {
    vec![ColumnCatalog::visible(ColumnDesc::named(
        "key",
        ColumnId::new(0),
        DataType::Int64,
    ))]
}

fn streaming_config() -> Arc<StreamingConfig> {
    streaming_config_with(
        0,
        StreamingConfig::default()
            .developer
            .iceberg_update_ordered_max_tasks,
    )
}

fn streaming_config_with(list_interval_sec: u64, ordered_max_tasks: u64) -> Arc<StreamingConfig> {
    let mut config = StreamingConfig::default();
    config.developer.iceberg_fetch_batch_size = 2;
    config.developer.chunk_size = 1;
    config.developer.iceberg_list_interval_sec = list_interval_sec;
    config.developer.iceberg_update_ordered_max_tasks = ordered_max_tasks;
    Arc::new(config)
}

#[tokio::test]
async fn pending_task_batches_respect_the_configured_size() -> anyhow::Result<()> {
    let (_, _, assignment) = fixture()?;
    let mut state = fetch_state(&fetch_state_catalog(42), MemoryStateStore::new()).await;
    state
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    for seq in 1..=3 {
        let mut assignment = assignment.clone();
        (assignment.route, assignment.seq) = (seq, seq);
        put_task(&mut state, &IcebergUpdateFetchState::new(assignment)).await?;
    }
    let batch_size = streaming_config().developer.iceberg_fetch_batch_size as usize;
    let batch = pending_tasks(&state, 42, batch_size, &output_columns(), &[0]).await?;
    assert_eq!(batch.len(), batch_size);
    for task in batch {
        delete_task(&mut state, &task.assignment).await?;
    }
    assert_eq!(
        pending_tasks(&state, 42, batch_size, &output_columns(), &[0])
            .await?
            .len(),
        1
    );
    assert!(
        pending_tasks(&state, 42, 0, &output_columns(), &[0])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn tasks_of_one_route_are_fetched_in_dispatch_order() -> anyhow::Result<()> {
    let (_, _, assignment) = fixture()?;
    let mut state = fetch_state(&fetch_state_catalog(42), MemoryStateStore::new()).await;
    state
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    // Sequence numbers above 9 also check numeric rather than textual order.
    for seq in [12, 3, 9, 10] {
        let mut assignment = assignment.clone();
        (assignment.route, assignment.seq) = (IcebergUpdateAssignment::ORDERED_ROUTE, seq);
        put_task(&mut state, &IcebergUpdateFetchState::new(assignment)).await?;
    }
    let tasks = pending_tasks(&state, 42, 32, &output_columns(), &[0]).await?;
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.assignment.seq)
            .collect::<Vec<_>>(),
        vec![3, 9, 10, 12]
    );

    // A row whose key differs from its assignment is rejected.
    let mut moved = IcebergUpdateFetchState::new(assignment.clone());
    moved.assignment.seq = 99;
    let row = task_row(&assignment, &moved)?;
    state.state_table_mut().insert(row);
    assert!(
        pending_tasks(&state, 42, 32, &output_columns(), &[0])
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn rescaled_fetch_owners_restore_only_their_vnode_cursors() -> anyhow::Result<()> {
    let (_, _, assignment) = fixture()?;
    let store = MemoryStateStore::new();
    let catalog = fetch_state_catalog(42);
    let mut owner = fetch_state(&catalog, store.clone()).await;
    owner
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    let vnode = owner
        .state_table()
        .compute_vnode_by_pk(task_pk(&assignment))
        .to_index();
    let mut progress = IcebergUpdateFetchState::new(assignment.clone());
    progress.advance(6, false)?;
    put_task(&mut owner, &progress).await?;
    owner
        .commit(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    drop(owner);
    for owns_task in [false, true] {
        let bitmap: Bitmap = (0..4).map(|index| (index == vnode) == owns_task).collect();
        let mut owner = SourceStateTableHandler::from_table_catalog_with_vnodes(
            &catalog,
            store.clone(),
            Some(Arc::new(bitmap)),
        )
        .await;
        owner
            .init_epoch(EpochPair::new_test_epoch(test_epoch(2)))
            .await?;
        let tasks = pending_tasks(&owner, 42, 32, &output_columns(), &[0]).await?;
        if owns_task {
            assert_eq!(tasks, vec![progress.clone()]);
        } else {
            assert!(tasks.is_empty());
        }
    }
    let row = reader(store, &descriptor(&catalog))
        .get_row(
            task_pk(&assignment),
            HummockReadEpoch::Committed(test_epoch(1)),
        )
        .await?
        .unwrap();
    assert_eq!(
        decode(&row, |task: &IcebergUpdateFetchState| &task.assignment)?,
        progress
    );
    Ok(())
}

#[tokio::test]
async fn phase_completion_requires_a_committed_snapshot_covering_all_dispatches()
-> anyhow::Result<()> {
    let (_, state, assignment) = fixture()?;
    let (commit_tx, committed) = watch::channel(0);
    let store = GatedStore {
        inner: MemoryStateStore::new(),
        committed,
    };
    let catalog = fetch_state_catalog(42);
    let properties: IcebergProperties =
        serde_json::from_value(json!({"catalog.type": "mock", "table.name": "unused"}))?;
    let ctx = list_context(properties, reader(store.clone(), &descriptor(&catalog)));

    // Even an empty old snapshot cannot advance this phase. This must not start a
    // committed read, since that epoch is intentionally unavailable in GatedStore.
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        list_work(
            ctx.clone(),
            Some(state.clone()),
            None,
            test_epoch(0),
            Duration::ZERO,
        )
        .next(),
    )
    .await?
    .unwrap()?;
    assert_eq!(result.state, state);

    // A task on any Fetch vnode prevents completion, even if it is not List's vnode.
    let mut fetch = fetch_state(&catalog, store.clone()).await;
    fetch
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    put_task(
        &mut fetch,
        &IcebergUpdateFetchState::new(assignment.clone()),
    )
    .await?;
    fetch
        .commit(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    let mut check = list_work(
        ctx.clone(),
        Some(state.clone()),
        None,
        test_epoch(1),
        Duration::ZERO,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), check.next())
            .await
            .is_err()
    );
    commit_tx.send(test_epoch(1))?;
    let result = check.next().await.unwrap()?;
    assert_eq!(result.state, state);

    // Local EOF retirement is not enough. Only a committed snapshot containing
    // that deletion can authorize the Delete -> Insert transition.
    delete_task(&mut fetch, &assignment).await?;
    fetch
        .commit(EpochPair::new_test_epoch(test_epoch(3)))
        .await?;
    let result = list_work(
        ctx.clone(),
        Some(state.clone()),
        None,
        test_epoch(1),
        Duration::ZERO,
    )
    .next()
    .await
    .unwrap()?;
    assert_eq!(result.state, state);
    let mut check = list_work(ctx, Some(state), None, test_epoch(2), Duration::ZERO);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), check.next())
            .await
            .is_err()
    );
    commit_tx.send(test_epoch(2))?;
    let result = check.next().await.unwrap()?;
    assert!(result.assignments.is_empty());
    assert_eq!(result.state.wait_epoch(), None);
    assert!(serde_json::to_value(result.state)?["fence"].is_null());
    Ok(())
}
#[tokio::test]
async fn task_retirement_and_output_cursor_share_the_recovery_checkpoint() -> anyhow::Result<()> {
    let (_, list, assignment) = fixture()?;
    let store = MemoryStateStore::new();
    let fetch_catalog = fetch_state_catalog(42);
    let list_catalog = list_state_catalog(43);
    let mut fetch = fetch_state(&fetch_catalog, store.clone()).await;
    let mut listing =
        SourceStateTableHandler::from_table_catalog(&list_catalog, store.clone()).await;
    let epoch = |n| EpochPair::new_test_epoch(test_epoch(n));
    fetch.init_epoch(epoch(1)).await?;
    listing.init_epoch(epoch(1)).await?;
    let mut progress = IcebergUpdateFetchState::new(assignment.clone());
    progress.advance(4, false)?;
    put_task(&mut fetch, &progress).await?;
    listing
        .state_table_mut()
        .insert([Some(ScalarImpl::Jsonb(encode(&list)?))]);
    fetch.commit(epoch(2)).await?;
    listing.commit(epoch(2)).await?;
    // Uncheckpointed EOF and retirement disappear together on restart.
    delete_task(&mut fetch, &assignment).await?;
    drop(fetch);
    let mut fetch = fetch_state(&fetch_catalog, store.clone()).await;
    fetch.init_epoch(epoch(2)).await?;
    let restored = pending_tasks(&fetch, 42, 32, &output_columns(), &[0]).await?;
    assert_eq!(restored, vec![progress]);
    assert!(pending_tasks(&fetch, 42, 32, &[], &[0]).await.is_err());
    assert!(
        pending_tasks(&fetch, 42, 32, &output_columns(), &[])
            .await
            .is_err()
    );
    assert!(
        pending_tasks(&fetch, 42, 32, &output_columns(), &[1])
            .await
            .is_err()
    );
    assert!(
        pending_tasks(&fetch, 99, 32, &output_columns(), &[0])
            .await
            .is_err()
    );

    delete_task(&mut fetch, &assignment).await?;
    fetch.commit(epoch(3)).await?;
    listing.commit(epoch(3)).await?;
    assert!(
        pending_tasks(&fetch, 42, 32, &output_columns(), &[0])
            .await?
            .is_empty()
    );
    let list_reader = reader(store.clone(), &descriptor(&list_catalog));
    let row = list_reader
        .get_row(
            OwnedRow::default(),
            HummockReadEpoch::Committed(test_epoch(2)),
        )
        .await?
        .unwrap();
    let mut recovered: IcebergUpdateListState =
        decode_value(row.datum_at(0).unwrap().into_jsonb().to_owned_scalar())?;
    assert_eq!(recovered, list);
    assert_eq!(recovered.wait_epoch(), Some(test_epoch(1)));
    assert!(
        reader(store, &descriptor(&fetch_catalog))
            .get_row(
                task_pk(&assignment),
                HummockReadEpoch::Committed(test_epoch(2)),
            )
            .await?
            .is_none()
    );
    // A recovered drained phase continues with the next phase, not with completed files.
    recovered.complete_fence(test_epoch(2))?;
    assert_eq!(recovered.wait_epoch(), None);
    assert_eq!(
        serde_json::to_value(&recovered)?["cursor"]["phase"],
        "Insert"
    );
    listing.state_table_mut().update(
        [Some(ScalarImpl::Jsonb(encode(&list)?))],
        [Some(ScalarImpl::Jsonb(encode(&recovered)?))],
    );
    listing.commit(epoch(4)).await?;
    Ok(())
}
async fn live_tables() -> anyhow::Result<(Table, Table)> {
    live_tables_with_files(1).await
}

async fn live_tables_with_files(initial_files: usize) -> anyhow::Result<(Table, Table)> {
    use iceberg::io::FileIO;
    use iceberg::spec::{
        FormatVersion, ManifestListWriter, ManifestWriterBuilder, Operation, Snapshot, SortOrder,
        Summary, TableMetadataBuilder, UnboundPartitionSpec,
    };
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
    use iceberg::{NamespaceIdent, Runtime, TableIdent};
    use risingwave_common::array::arrow::arrow_array_iceberg::{Int64Array, RecordBatch};
    use risingwave_connector::sink::iceberg::write_dv_puffin_file;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "key",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()?;
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "memory://changes".to_owned(),
        FormatVersion::V3,
        std::collections::HashMap::new(),
    )?
    .build()?
    .metadata;
    let mut table = Table::builder()
        .identifier(TableIdent::new(
            NamespaceIdent::new(format!("db_{}", uuid::Uuid::new_v4().simple())),
            "changes".to_owned(),
        ))
        .metadata(metadata)
        .file_io(FileIO::new_with_memory())
        .runtime(Runtime::try_current()?)
        .build()?;
    let mut files = vec![];
    let mut tables = vec![];
    for id in [1, 2] {
        let schema = table.metadata().current_schema();
        for file_index in 0..if id == 1 { initial_files } else { 1 } {
            let mut writer = ParquetWriterBuilder::new(
                parquet::file::properties::WriterProperties::builder().build(),
                schema.clone(),
            )
            .build(
                table
                    .file_io()
                    .new_output(format!("memory://changes/data-{id}-{file_index}.parquet"))?,
            )
            .await?;
            writer
                .write(&RecordBatch::try_new(
                    Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?),
                    vec![Arc::new(Int64Array::from(if id == 1 {
                        vec![
                            file_index as i64 * 3,
                            file_index as i64 * 3 + 1,
                            file_index as i64 * 3 + 2,
                        ]
                    } else {
                        vec![0]
                    }))],
                )?)
                .await?;
            files.push(
                writer
                    .close()
                    .await?
                    .remove(0)
                    .first_row_id(Some((id * initial_files as i64 + file_index as i64) * 3))
                    .build()?,
            );
        }
        let builder = |delete| {
            ManifestWriterBuilder::new(
                table
                    .file_io()
                    .new_output(format!("memory://changes/manifest-{id}-{delete}.avro"))
                    .unwrap(),
                Some(id),
                schema.clone(),
                table.metadata().default_partition_spec().as_ref().clone(),
            )
        };
        let mut writer = builder(false).build_v3_data();
        for file in &files {
            writer.add_existing_file(file.clone(), id, 1, Some(1))?;
        }
        let mut manifests = vec![writer.write_manifest_file().await?];
        if id == 2 {
            let delete = write_dv_puffin_file(
                &table,
                &DefaultLocationGenerator::new(table.metadata())?,
                &DefaultFileNameGenerator::new(
                    "delete".to_owned(),
                    None,
                    iceberg::spec::DataFileFormat::Puffin,
                ),
                files[0].file_path().to_owned(),
                &iceberg::delete_vector::DeleteVector::from([0]),
                None,
            )
            .await?;
            let mut writer = builder(true).build_v3_deletes();
            writer.add_existing_file(delete, id, id, Some(id))?;
            manifests.push(writer.write_manifest_file().await?);
        }
        let path = format!("memory://changes/list-{id}.avro");
        let mut list = ManifestListWriter::v3(
            table.file_io().new_output(&path)?.writer().await?,
            id,
            table.metadata().current_snapshot_id(),
            id,
            Some(0),
        );
        list.add_manifests(manifests.into_iter())?;
        list.close().await?;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(id)
            .with_parent_snapshot_id(table.metadata().current_snapshot_id())
            .with_sequence_number(id)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 1)
            .with_manifest_list(path)
            .with_schema_id(table.metadata().current_schema_id())
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: std::collections::HashMap::new(),
            })
            .with_row_range(0, 0)
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_branch_snapshot(snapshot, "main")?
            .build()?
            .metadata;
        table = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(metadata)
            .file_io(table.file_io().clone())
            .runtime(Runtime::try_current()?)
            .build()?;
        tables.push(table.clone());
    }
    Ok((tables.remove(0), tables.remove(0)))
}

async fn test_core<S: StateStore>(
    store: S,
    catalog: &risingwave_pb::catalog::Table,
    vnodes: Option<Arc<Bitmap>>,
    table: &TableIdent,
) -> StreamSourceCore<S> {
    use risingwave_connector::WithOptionsSecResolved;
    use risingwave_connector::source::monitor::SourceMetrics;
    use risingwave_connector::source::reader::desc::SourceDescBuilder;
    let builder = SourceDescBuilder::new(
        vec![],
        Arc::new(SourceMetrics::default()),
        None,
        WithOptionsSecResolved::without_secrets(
            [
                ("connector".to_owned(), "iceberg".to_owned()),
                ("catalog.type".to_owned(), "mock".to_owned()),
                ("database.name".to_owned(), table.namespace.to_string()),
                ("table.name".to_owned(), table.name().to_owned()),
                ("streaming_updates".to_owned(), "true".to_owned()),
            ]
            .into(),
        ),
        Default::default(),
        16,
        vec![],
    );
    StreamSourceCore::new(
        9.into(),
        "changes".to_owned(),
        vec![],
        builder,
        SourceStateTableHandler::from_table_catalog_with_vnodes(catalog, store, vnodes).await,
    )
}

async fn pipeline(
    store: GatedStore,
    table: &TableIdent,
    config: Arc<StreamingConfig>,
) -> anyhow::Result<(
    tokio::sync::mpsc::UnboundedSender<Barrier>,
    BoxedMessageStream,
)> {
    let list_catalog = list_state_catalog(43);
    let fetch_catalog = fetch_state_catalog(42);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let list = IcebergUpdateListExecutor::new(
        test_core(store.clone(), &list_catalog, None, table).await,
        output_columns(),
        descriptor(&fetch_catalog),
        rx,
        config.clone(),
    );
    let upstream = Executor::new(
        ExecutorInfo::for_test(
            risingwave_common::catalog::Schema::new(
                task_row_types()
                    .into_iter()
                    .map(risingwave_common::catalog::Field::unnamed)
                    .collect(),
            ),
            vec![0, 1],
            "List".to_owned(),
            1,
        ),
        list.boxed(),
    );
    let fetch = IcebergUpdateFetchExecutor::new(
        ActorContext::for_test(1),
        test_core(
            store,
            &fetch_catalog,
            Some(Arc::new(Bitmap::ones(4))),
            table,
        )
        .await,
        upstream,
        config,
        output_columns(),
        vec![0],
    );
    Ok((tx, fetch.boxed().execute()))
}

#[tokio::test]
async fn list_binds_once_and_recovers_before_issuing_tasks() -> anyhow::Result<()> {
    let (first, second) = live_tables().await?;
    let table = MockCatalog::register(first.clone());
    let properties: IcebergProperties = serde_json::from_value(json!({
        "catalog.type": "mock",
        "database.name": first.identifier().namespace.to_string(),
        "table.name": first.identifier().name(),
        "streaming_updates": "true",
    }))?;
    let store = MemoryStateStore::new();
    let columns = output_columns();
    let ctx = list_context(
        properties,
        reader(store.clone(), &descriptor(&fetch_state_catalog(42))),
    );
    let initial = list_work(ctx.clone(), None, None, test_epoch(0), Duration::ZERO)
        .next()
        .await
        .unwrap()?;
    assert!(initial.assignments.is_empty());
    assert_eq!(
        serde_json::to_value(&initial.state)?["binding"]["bootstrap_snapshot_id"],
        1
    );

    let catalog = list_state_catalog(43);
    let mut state_table =
        SourceStateTableHandler::from_table_catalog(&catalog, store.clone()).await;
    state_table
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    state_table
        .state_table_mut()
        .insert([Some(ScalarImpl::Jsonb(encode(&initial.state)?))]);
    state_table
        .commit(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    drop(state_table);
    table.update(second);
    let mut state_table =
        SourceStateTableHandler::from_table_catalog(&catalog, store.clone()).await;
    state_table
        .init_epoch(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    let state: IcebergUpdateListState = decode_value(
        state_table
            .state_table()
            .get_from_one_value_table()
            .await?
            .unwrap()
            .into_jsonb(),
    )?;
    state.validate_columns(42, &columns)?;
    assert!(state.validate_columns(99, &columns).is_err());
    assert!(state.validate_columns(42, &[]).is_err());
    let replay = list_work(ctx, Some(state), None, test_epoch(0), Duration::ZERO)
        .next()
        .await
        .unwrap()?;
    assert_eq!(replay.assignments.len(), 1);
    assert_eq!(replay.assignments[0].task.id.snapshot_id, 1);
    Ok(())
}

#[tokio::test]
async fn live_list_fetch_waits_only_at_phase_boundaries_and_keeps_barriers_flowing()
-> anyhow::Result<()> {
    live_list_fetch(0).await
}

#[tokio::test]
async fn live_list_fetch_orders_small_phases_without_waiting_for_commits() -> anyhow::Result<()> {
    live_list_fetch(16).await
}

/// Without ordered phases, Insert waits until the Delete phase is committed. With them, both
/// phases share one Fetch route, so Insert follows Delete while global commit is held.
async fn live_list_fetch(ordered_max_tasks: u64) -> anyhow::Result<()> {
    let ordered = ordered_max_tasks > 0;
    let config = streaming_config_with(0, ordered_max_tasks);
    let (first, second) = live_tables().await?;
    let table = MockCatalog::register(first.clone());
    let (commit_tx, committed) = watch::channel(0);
    let store = GatedStore {
        inner: MemoryStateStore::new(),
        committed,
    };
    let (mut tx, mut output) = pipeline(store.clone(), first.identifier(), config.clone()).await?;
    let mut events = vec![];
    let mut hold_until = None;
    let mut released = false;
    let mut restart = false;
    let mut restarted = false;
    let mut applied = false;
    for epoch in 1..70 {
        let mut barrier = Barrier::new_test_barrier(test_epoch(epoch));
        if epoch > 1 && !restart && epoch % 4 != 0 {
            barrier.kind = BarrierKind::Barrier;
        }
        tx.send(barrier)?;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), output.next())
                .await?
                .unwrap()?,
            Message::Barrier(_)
        ));
        if hold_until.is_none_or(|until| epoch >= until) {
            commit_tx.send(test_epoch(epoch - 1))?;
            released |= hold_until.is_some();
        }
        if restart {
            // The first row and its physical cursor are checkpointed. Latest has
            // advanced, but both executors must recover the original binding.
            table.update(second.clone());
            drop(output);
            (tx, output) = pipeline(store.clone(), first.identifier(), config.clone()).await?;
            tx.send(Barrier::new_test_barrier(test_epoch(epoch)))?;
            assert!(matches!(output.next().await.unwrap()?, Message::Barrier(_)));
            restart = false;
            restarted = true;
        }
        while let Ok(Some(message)) =
            tokio::time::timeout(Duration::from_millis(30), output.next()).await
        {
            let Message::Chunk(chunk) = message? else {
                panic!("unexpected message");
            };
            for (op, row) in chunk.rows() {
                let key = row.datum_at(0).unwrap().into_int64();
                if op == Op::Delete {
                    assert_eq!(
                        events,
                        vec![(Op::Insert, 0), (Op::Insert, 1), (Op::Insert, 2)]
                    );
                    // An ordered Insert must not need any further commit.
                    hold_until = Some(if ordered { u64::MAX } else { epoch + 4 });
                } else if hold_until.is_some() {
                    assert!(
                        released != ordered,
                        "Insert crossed an uncommitted fan-out Delete phase or waited for an ordered one"
                    );
                }
                events.push((op, key));
            }
            if events.len() == 1 && !restarted {
                restart = true;
                break;
            }
        }
        if events.len() == 3 {
            table.update(second.clone());
        }
        if events.len() == 5 {
            let row = reader(store.inner.clone(), &descriptor(&list_state_catalog(43)))
                .get_row(
                    OwnedRow::default(),
                    HummockReadEpoch::Committed(test_epoch(epoch - 1)),
                )
                .await?
                .unwrap();
            let state: IcebergUpdateListState =
                decode_value(row.datum_at(0).unwrap().into_jsonb().to_owned_scalar())?;
            let encoded = serde_json::to_value(&state)?;
            if encoded["dispatched_snapshot"] == 2 && encoded["cursor"].is_null() {
                if ordered {
                    assert_eq!(encoded["fence"]["ordered"], true);
                }
                if state.wait_epoch().is_none() {
                    applied = true;
                    break;
                }
            }
        }
    }
    assert!(released != ordered && restarted && applied);
    assert_eq!(
        events,
        vec![
            (Op::Insert, 0),
            (Op::Insert, 1),
            (Op::Insert, 2),
            (Op::Delete, 0),
            (Op::Insert, 0),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn list_dispatches_multiple_pages_before_waiting_for_a_checkpoint() -> anyhow::Result<()> {
    let (first, _) = live_tables_with_files(5).await?;
    let _table = MockCatalog::register(first.clone());
    let (_commit_tx, committed) = watch::channel(0);
    let store = GatedStore {
        inner: MemoryStateStore::new(),
        committed,
    };
    let list_catalog = list_state_catalog(43);
    let fetch_catalog = fetch_state_catalog(42);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let list = IcebergUpdateListExecutor::new(
        test_core(store.clone(), &list_catalog, None, first.identifier()).await,
        output_columns(),
        descriptor(&fetch_catalog),
        rx,
        streaming_config(),
    );
    let mut output = list.boxed().execute();
    tx.send(Barrier::new_test_barrier(test_epoch(1)))?;
    assert!(matches!(output.next().await.unwrap()?, Message::Barrier(_)));
    // Bind first, then checkpoint the binding before dispatching any task.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), output.next())
            .await
            .is_err()
    );
    tx.send(Barrier::new_test_barrier(test_epoch(2)))?;
    assert!(matches!(output.next().await.unwrap()?, Message::Barrier(_)));
    let mut keys = std::collections::HashSet::new();
    for expected in [2, 2, 1] {
        let Message::Chunk(chunk) = tokio::time::timeout(Duration::from_secs(5), output.next())
            .await?
            .unwrap()?
        else {
            panic!("expected a task page without any intervening barrier");
        };
        assert_eq!(chunk.cardinality(), expected);
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert);
            // A phase spanning several pages fans out: each task has its own route.
            let seq = row.datum_at(1).unwrap().into_int64();
            assert_eq!(row.datum_at(0).unwrap().into_int64(), seq);
            assert!(keys.insert(seq));
        }
    }
    // Start a phase completion read with global commit still blocked.
    tx.send(Barrier::new_test_barrier(test_epoch(3)))?;
    assert!(matches!(output.next().await.unwrap()?, Message::Barrier(_)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), output.next())
            .await
            .is_err()
    );
    let mut barrier = Barrier::new_test_barrier(test_epoch(4));
    barrier.kind = BarrierKind::Barrier;
    tx.send(barrier)?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), output.next())
            .await?
            .unwrap()?,
        Message::Barrier(_)
    ));
    let row = reader(store.inner, &descriptor(&list_catalog))
        .get_row(
            OwnedRow::default(),
            HummockReadEpoch::Committed(test_epoch(3)),
        )
        .await?
        .unwrap();
    let state: IcebergUpdateListState =
        decode_value(row.datum_at(0).unwrap().into_jsonb().to_owned_scalar())?;
    assert_eq!(state.wait_epoch(), Some(test_epoch(2)));
    assert!(serde_json::to_value(state)?.get("pending").is_none());
    Ok(())
}

async fn list_step(
    tx: &tokio::sync::mpsc::UnboundedSender<Barrier>,
    output: &mut BoxedMessageStream,
    epoch: &mut u64,
    mutation: Option<Mutation>,
) -> anyhow::Result<usize> {
    *epoch += 1;
    let mut barrier = Barrier::new_test_barrier(test_epoch(*epoch));
    if let Some(mutation) = mutation {
        barrier = barrier.with_mutation(mutation);
    }
    tx.send(barrier)?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(5), output.next())
            .await?
            .unwrap()?,
        Message::Barrier(_)
    ));
    let mut rows = 0;
    while let Ok(Some(message)) =
        tokio::time::timeout(Duration::from_millis(100), output.next()).await
    {
        let Message::Chunk(chunk) = message? else {
            panic!("unexpected message");
        };
        rows += chunk.cardinality();
    }
    Ok(rows)
}

#[tokio::test]
async fn idle_list_polls_are_throttled_while_barriers_keep_flowing() -> anyhow::Result<()> {
    // Waiting for phase completion never sleeps, however long the list interval is.
    let (_, state, _) = fixture()?;
    let properties: IcebergProperties =
        serde_json::from_value(json!({"catalog.type": "mock", "table.name": "unused"}))?;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        list_work(
            list_context(
                properties,
                reader(
                    MemoryStateStore::new(),
                    &descriptor(&fetch_state_catalog(42)),
                ),
            ),
            Some(state.clone()),
            None,
            test_epoch(0),
            Duration::from_secs(3600),
        )
        .next(),
    )
    .await?
    .unwrap()?;
    assert_eq!(result.state, state);

    let (first, second) = live_tables().await?;
    let table = MockCatalog::register(first.clone());
    let list_catalog = list_state_catalog(43);
    let fetch_catalog = fetch_state_catalog(42);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let list = IcebergUpdateListExecutor::new(
        test_core(
            MemoryStateStore::new(),
            &list_catalog,
            None,
            first.identifier(),
        )
        .await,
        output_columns(),
        descriptor(&fetch_catalog),
        rx,
        // Fan out so that the bootstrap phase waits for its completion like a large phase.
        streaming_config_with(1, 0),
    );
    let mut output = list.boxed().execute();
    let mut epoch = 0;
    // Bind and the bootstrap page are not delayed.
    let mut bootstrapped = false;
    for _ in 0..10 {
        if list_step(&tx, &mut output, &mut epoch, None).await? > 0 {
            bootstrapped = true;
            break;
        }
    }
    assert!(bootstrapped);
    // Complete the bootstrap phase, then find nothing new.
    for _ in 0..2 {
        assert_eq!(list_step(&tx, &mut output, &mut epoch, None).await?, 0);
    }
    // This barrier starts a throttled poll, which must not see the new snapshot early.
    let throttled_since = std::time::Instant::now();
    assert_eq!(list_step(&tx, &mut output, &mut epoch, None).await?, 0);
    table.update(second);
    while throttled_since.elapsed() < Duration::from_millis(500) {
        assert_eq!(list_step(&tx, &mut output, &mut epoch, None).await?, 0);
    }
    // A paused List neither polls nor emits, even after the interval elapsed.
    assert_eq!(
        list_step(&tx, &mut output, &mut epoch, Some(Mutation::Pause)).await?,
        0
    );
    while throttled_since.elapsed() < Duration::from_millis(1500) {
        assert_eq!(list_step(&tx, &mut output, &mut epoch, None).await?, 0);
    }
    let mut rows = list_step(&tx, &mut output, &mut epoch, Some(Mutation::Resume)).await?;
    for _ in 0..10 {
        if rows > 0 {
            break;
        }
        rows = list_step(&tx, &mut output, &mut epoch, None).await?;
    }
    assert!(rows > 0, "the new snapshot is listed after the interval");
    Ok(())
}
