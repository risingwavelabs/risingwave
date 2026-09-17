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

use std::time::Duration;

use futures::FutureExt;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::util::epoch::{EpochPair, test_epoch};
use risingwave_connector::connector_common::IcebergSourceContract;
use risingwave_hummock_sdk::key::TableKeyRange;
use risingwave_storage::error::StorageResult;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::panic_store::PanicStateStoreIter;
use risingwave_storage::store::*;
use serde_json::json;
use tokio::sync::watch;

use super::*;
use crate::executor::source::state_table_handler::default_source_internal_table;

// MemoryStateStore deliberately does not implement a global commit fence. Add a controlled
// fence while retaining its real local table, epoch snapshots, encoding and vnode routing.
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
        self.inner.new_read_snapshot(epoch, options).await
    }

    async fn new_vector_writer(&self, options: NewVectorWriterOptions) -> Self::VectorWriter {
        self.inner.new_vector_writer(options).await
    }
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

fn reader<S: StateStore>(store: S, desc: &StorageTableDesc) -> Arc<BatchTable<S>> {
    Arc::new(BatchTable::new_partial(
        store,
        vec![ColumnId::new(0), ColumnId::new(1)],
        Some(Arc::new(Bitmap::ones(desc.vnode_count()))),
        desc,
    ))
}

fn fixture() -> anyhow::Result<(IcebergUpdateBinding, IcebergUpdateListState)> {
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
        "contract": IcebergSourceContract::new(&schema, vec![1])?,
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
        "bootstrap": false, "phase": "Delete", "after_path": null});
    let state = serde_json::from_value(json!({
        "job_id": 42, "binding": binding, "bootstrap_complete": true, "applied_snapshot": 1,
        "cursor": cursor, "generation": 7,
        "pending": {"tasks": [task], "phase_finished": true}
    }))?;
    Ok((serde_json::from_value(binding)?, state))
}

fn output_columns() -> Vec<ColumnCatalog> {
    vec![ColumnCatalog::visible(ColumnDesc::named(
        "key",
        ColumnId::new(0),
        DataType::Int64,
    ))]
}

#[tokio::test]
async fn rescaled_fetch_owners_restore_only_their_vnode_cursors() -> anyhow::Result<()> {
    let (_, state) = fixture()?;
    let store = MemoryStateStore::new();
    let mut catalog = default_source_internal_table(42);
    catalog.distribution_key = vec![0];
    catalog.maybe_vnode_count = Some(4);
    validate_update_state_table(&catalog, false)?;
    assert!(validate_update_state_table(&catalog, true).is_err());
    let mut owner = SourceStateTableHandler::from_table_catalog_with_vnodes(
        &catalog,
        store.clone(),
        Some(Arc::new(Bitmap::ones(4))),
    )
    .await;
    owner
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    let assignment = state.assignments().remove(0);
    let key = assignment.key()?;
    let vnode = owner
        .state_table()
        .compute_vnode_by_pk(OwnedRow::new(vec![Some(ScalarImpl::Utf8(
            key.clone().into(),
        ))]))
        .to_index();
    let mut progress = IcebergUpdateFetchState::new(assignment);
    progress.advance(6, false)?;
    owner.set(&key, encode(&progress)?).await?;
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
            OwnedRow::new(vec![Some(ScalarImpl::Utf8(key.into()))]),
            HummockReadEpoch::Committed(test_epoch(1)),
        )
        .await?
        .unwrap();
    assert_eq!(decode::<IcebergUpdateFetchState>(&row)?, progress);
    Ok(())
}

#[tokio::test]
async fn committed_completion_wait_keeps_barriers_pollable() -> anyhow::Result<()> {
    let (_, state) = fixture()?;
    let (commit_tx, committed) = watch::channel(0);
    let store = GatedStore {
        inner: MemoryStateStore::new(),
        committed,
    };
    let catalog = default_source_internal_table(42);
    let mut writer = SourceStateTableHandler::from_table_catalog(&catalog, store.clone()).await;
    writer
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    let assignment = state.assignments().remove(0);
    let key = assignment.key()?;
    let mut done = IcebergUpdateFetchState::new(assignment.clone());
    done.advance(10, true)?;
    writer.set(&key, encode(&done)?).await?;
    writer
        .commit(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    let completion = reader(store, &descriptor(&catalog));
    let properties = TableLoader {
        properties: serde_json::from_value(json!({"table.name": "unused"}))?,
        table: None,
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut stream = StreamReaderWithPause::<true, ListResult>::new(
        barrier_to_message_stream(rx).boxed(),
        list_work(
            properties,
            Some(state),
            Arc::new(output_columns()),
            IcebergUpdateLimits::default(),
            completion,
            test_epoch(1),
        ),
    );
    assert!(
        stream.next().now_or_never().is_none(),
        "a local flush is not a checkpoint"
    );
    for epoch in [2, 3] {
        tx.send(Barrier::new_test_barrier(test_epoch(epoch)))?;
        assert!(matches!(
            stream.next().await.unwrap()?,
            Either::Left(Message::Barrier(_))
        ));
        assert!(stream.next().now_or_never().is_none());
    }
    commit_tx.send(test_epoch(1))?;
    let Either::Right(result) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await?
        .unwrap()?
    else {
        panic!("expected completed page");
    };
    assert_eq!(result.completed, vec![assignment]);
    assert!(result.assignments.is_empty());
    assert!(result.state.assignments().is_empty());
    assert_eq!(
        serde_json::to_value(result.state)?["cursor"]["phase"],
        "Insert"
    );
    Ok(())
}

#[tokio::test]
async fn task_cursor_and_gc_recover_at_the_same_epoch_as_list_state() -> anyhow::Result<()> {
    let (_, mut list) = fixture()?;
    let store = MemoryStateStore::new();
    let fetch_catalog = default_source_internal_table(42);
    let list_catalog = default_source_internal_table(43);
    let mut fetch =
        SourceStateTableHandler::from_table_catalog(&fetch_catalog, store.clone()).await;
    let mut listing =
        SourceStateTableHandler::from_table_catalog(&list_catalog, store.clone()).await;
    let epoch = |n| EpochPair::new_test_epoch(test_epoch(n));
    fetch.init_epoch(epoch(1)).await?;
    listing.init_epoch(epoch(1)).await?;
    let assignment = list.assignments().remove(0);
    let key = assignment.key()?;
    let mut progress = IcebergUpdateFetchState::new(assignment);
    progress.advance(4, false)?;
    fetch.set(&key, encode(&progress)?).await?;
    listing.set(LIST_KEY, encode(&list)?).await?;
    fetch.commit(epoch(2)).await?;
    listing.commit(epoch(2)).await?;
    // Restart loses uncheckpointed progress, not the committed physical position.
    progress.advance(8, false)?;
    fetch.set(&key, encode(&progress)?).await?;
    drop(fetch);
    let mut fetch =
        SourceStateTableHandler::from_table_catalog(&fetch_catalog, store.clone()).await;
    fetch.init_epoch(epoch(2)).await?;
    let restored = pending_tasks(&fetch, 42, 32, &output_columns(), &[0]).await?;
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].next_position, 4);
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
    progress.advance(10, true)?;
    fetch.set(&key, encode(&progress)?).await?;
    fetch.commit(epoch(3)).await?;
    listing.commit(epoch(3)).await?;
    assert!(
        pending_tasks(&fetch, 42, 32, &output_columns(), &[0])
            .await?
            .is_empty()
    );
    assert!(list.acknowledge_committed(std::slice::from_ref(&progress))?);
    fetch.delete(&key).await?;
    listing.set(LIST_KEY, encode(&list)?).await?;
    // Before the next checkpoint both old pending List state and EOF row are recoverable.
    let old_list = reader(store.clone(), &descriptor(&list_catalog))
        .get_row(
            OwnedRow::new(vec![Some(ScalarImpl::Utf8(LIST_KEY.into()))]),
            HummockReadEpoch::Committed(test_epoch(2)),
        )
        .await?
        .unwrap();
    let mut old_list: IcebergUpdateListState = decode(&old_list)?;
    let done = reader(store.clone(), &descriptor(&fetch_catalog))
        .get_row(
            OwnedRow::new(vec![Some(ScalarImpl::Utf8(key.clone().into()))]),
            HummockReadEpoch::Committed(test_epoch(2)),
        )
        .await?
        .unwrap();
    assert!(old_list.acknowledge_committed(&[decode(&done)?])?);
    assert_eq!(list, old_list);
    fetch.commit(epoch(4)).await?;
    listing.commit(epoch(4)).await?;
    assert!(
        reader(store, &descriptor(&fetch_catalog))
            .get_row(
                OwnedRow::new(vec![Some(ScalarImpl::Utf8(key.into()))]),
                HummockReadEpoch::Committed(test_epoch(3))
            )
            .await?
            .is_none()
    );
    Ok(())
}

async fn live_tables() -> anyhow::Result<(Table, Table)> {
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
    use risingwave_connector::connector_common::IcebergCommitKind;
    use risingwave_connector::sink::iceberg::write_dv_puffin_file;
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "key",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()?;
    let props = IcebergSourceContract::new(&schema, vec![1])?.to_properties();
    let metadata = TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        "memory://changes".to_owned(),
        FormatVersion::V3,
        props,
    )?
    .build()?
    .metadata;
    let mut table = Table::builder()
        .identifier(TableIdent::new(
            NamespaceIdent::new("db".to_owned()),
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
        let mut writer = ParquetWriterBuilder::new(
            parquet::file::properties::WriterProperties::builder().build(),
            schema.clone(),
        )
        .build(
            table
                .file_io()
                .new_output(format!("memory://changes/data-{id}.parquet"))?,
        )
        .await?;
        writer
            .write(&RecordBatch::try_new(
                Arc::new(iceberg::arrow::schema_to_arrow_schema(schema)?),
                vec![Arc::new(Int64Array::from(if id == 1 {
                    vec![0, 1, 2]
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
                .first_row_id(Some(id * 3))
                .build()?,
        );
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
                additional_properties: IcebergCommitKind::Data.to_properties(),
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
                ("table.name".to_owned(), "unused".to_owned()),
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
    table_rx: watch::Receiver<Table>,
) -> anyhow::Result<(
    tokio::sync::mpsc::UnboundedSender<Barrier>,
    BoxedMessageStream,
)> {
    let list_catalog = default_source_internal_table(43);
    let mut fetch_catalog = default_source_internal_table(42);
    fetch_catalog.distribution_key = vec![0];
    fetch_catalog.maybe_vnode_count = Some(4);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut list = IcebergUpdateListExecutor::new(
        test_core(store.clone(), &list_catalog, None).await,
        output_columns(),
        descriptor(&fetch_catalog),
        store.clone(),
        rx,
    );
    list.table = Some(table_rx.clone());
    let upstream = Executor::new(
        ExecutorInfo::for_test(
            risingwave_common::catalog::Schema::new(vec![
                risingwave_common::catalog::Field::unnamed(DataType::Varchar),
                risingwave_common::catalog::Field::unnamed(DataType::Jsonb),
            ]),
            vec![0],
            "List".to_owned(),
            1,
        ),
        list.boxed(),
    );
    let mut fetch = IcebergUpdateFetchExecutor::new(
        ActorContext::for_test(1),
        test_core(store, &fetch_catalog, Some(Arc::new(Bitmap::ones(4)))).await,
        upstream,
        1,
        output_columns(),
        vec![0],
    );
    fetch.table = Some(table_rx);
    Ok((tx, fetch.boxed().execute()))
}

#[tokio::test]
async fn list_binds_once_and_recovers_before_issuing_tasks() -> anyhow::Result<()> {
    let (first, second) = live_tables().await?;
    let (table_tx, table_rx) = watch::channel(first);
    let properties = TableLoader {
        properties: serde_json::from_value(json!({"table.name": "unused"}))?,
        table: Some(table_rx),
    };
    let store = MemoryStateStore::new();
    let completion = reader(
        store.clone(),
        &descriptor(&default_source_internal_table(42)),
    );
    let columns = Arc::new(output_columns());
    let initial = list_work(
        properties.clone(),
        None,
        columns.clone(),
        IcebergUpdateLimits::default(),
        completion.clone(),
        test_epoch(1),
    )
    .next()
    .await
    .unwrap()?;
    assert!(initial.assignments.is_empty());
    assert!(initial.completed.is_empty());
    assert_eq!(
        serde_json::to_value(&initial.state)?["binding"]["bootstrap_snapshot_id"],
        1
    );

    let catalog = default_source_internal_table(43);
    let mut state_table =
        SourceStateTableHandler::from_table_catalog(&catalog, store.clone()).await;
    state_table
        .init_epoch(EpochPair::new_test_epoch(test_epoch(1)))
        .await?;
    state_table.set(LIST_KEY, encode(&initial.state)?).await?;
    state_table
        .commit(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    drop(state_table);
    table_tx.send(second)?;
    let mut state_table = SourceStateTableHandler::from_table_catalog(&catalog, store).await;
    state_table
        .init_epoch(EpochPair::new_test_epoch(test_epoch(2)))
        .await?;
    let state: IcebergUpdateListState = decode(state_table.get(LIST_KEY).await?.unwrap())?;
    state.validate_columns(42, &columns)?;
    assert!(state.validate_columns(99, &columns).is_err());
    assert!(state.validate_columns(42, &[]).is_err());
    let replay = list_work(
        properties,
        Some(state),
        columns,
        IcebergUpdateLimits::default(),
        completion,
        test_epoch(1),
    )
    .next()
    .await
    .unwrap()?;
    assert_eq!(replay.assignments.len(), 1);
    assert_eq!(replay.assignments[0].task.id.snapshot_id, 1);
    Ok(())
}

#[tokio::test]
async fn live_list_fetch_delete_checkpoint_insert_loop() -> anyhow::Result<()> {
    let (first, second) = live_tables().await?;
    let (table_tx, table_rx) = watch::channel(first);
    let (commit_tx, committed) = watch::channel(0);
    let store = GatedStore {
        inner: MemoryStateStore::new(),
        committed,
    };
    let (mut tx, mut output) = pipeline(store.clone(), table_rx.clone()).await?;
    let mut events = vec![];
    let mut hold_until = None;
    let mut released = false;
    let mut restart = false;
    let mut restarted = false;
    let mut applied = false;
    for epoch in 1..35 {
        tx.send(Barrier::new_test_barrier(test_epoch(epoch)))?;
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
            // The first physical row and downstream output were checkpointed. Rebuild both
            // executors after advancing latest. Only persisted state supplies the old binding.
            table_tx.send(second.clone())?;
            drop(output);
            (tx, output) = pipeline(store.clone(), table_rx.clone()).await?;
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
                    hold_until = Some(epoch + 4);
                } else if hold_until.is_some() {
                    assert!(released, "Insert crossed an uncommitted Delete phase");
                }
                events.push((op, key));
            }
            if events.len() == 1 && !restarted {
                restart = true;
                break;
            }
        }
        if events.len() == 3 {
            table_tx.send(second.clone())?;
        }
        if events.len() == 5 {
            let row = reader(
                store.inner.clone(),
                &descriptor(&default_source_internal_table(43)),
            )
            .get_row(
                OwnedRow::new(vec![Some(ScalarImpl::Utf8(LIST_KEY.into()))]),
                HummockReadEpoch::Committed(test_epoch(epoch - 1)),
            )
            .await?
            .unwrap();
            let state: IcebergUpdateListState = decode(&row)?;
            if serde_json::to_value(&state)?["applied_snapshot"] == 2 {
                assert!(state.assignments().is_empty());
                applied = true;
                break;
            }
        }
    }
    assert!(released && restarted && applied);
    assert_eq!(
        events,
        vec![
            (Op::Insert, 0),
            (Op::Insert, 1),
            (Op::Insert, 2),
            (Op::Delete, 0),
            (Op::Insert, 0)
        ]
    );
    Ok(())
}
