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

use std::collections::HashMap;
use std::fmt;
use std::io::Cursor;
use std::ops::Deref;

use anyhow::{Context, anyhow};
use arrow_58_ipc::reader::StreamReader;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use futures_async_stream::try_stream;
use jni::objects::GlobalRef;
use jni::sys::jlong;
use prost::Message;
use risingwave_common::array::DataChunk;
use risingwave_common::array::arrow::Arrow58FromArrow;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::global_jvm::Jvm;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_jni_core::{JavaBindingIterator, JniSenderType, call_static_method};
use risingwave_pb::connector_service::{
    OracleConnectionProperties, OracleSnapshotReadRequest, OracleSnapshotReadResponse, TableSchema,
};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::sync::{OnceCell, mpsc};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, ExternalTableConfig,
    ExternalTableReader, SchemaTableName,
};

const ORACLE_SNAPSHOT_CHANNEL_SIZE: usize = 4;

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OracleOffset {
    pub scn: u64,
}

pub struct OracleExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl OracleExternalTable {
    /// Discovers the table schema through JNI on a Tokio blocking thread, so the caller's async
    /// runtime is never blocked.
    ///
    /// Cancellation safe: every call opens its own Oracle connection, so calls are independent. If
    /// the future is dropped, the discovery keeps running in the detached blocking task, but it
    /// shares no connection with any later call and cannot block it. It then closes its
    /// connection, and the discovered schema is discarded. Discovery only runs two short catalog
    /// queries, so the abandoned task also finishes quickly.
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let properties = config.oracle_connection_properties()?;
        let table_schema = tokio::task::spawn_blocking(move || invoke_jni_discover(&properties))
            .await
            .context("failed to join Oracle schema discovery task")??;

        let column_descs = table_schema
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect::<Vec<_>>();
        let pk_names = table_schema
            .pk_indices
            .iter()
            .map(|index| {
                table_schema
                    .columns
                    .get(*index as usize)
                    .map(|column| column.name.clone())
                    .with_context(|| {
                        format!("Oracle primary-key index {index} is unable to extract column name")
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        if column_descs.is_empty() {
            bail!("Oracle schema discovery returned no columns");
        }
        if pk_names.is_empty() {
            bail!("Oracle schema discovery returned no valid primary keys");
        }

        Ok(Self {
            column_descs,
            pk_names,
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

#[derive(Debug)]
pub struct OracleExternalTableReader {
    connection_properties: OracleConnectionProperties,
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    table_schema: TableSchema,
    table: String,
    pool: OnceCell<OraclePool>,
}

impl ExternalTableReader for OracleExternalTableReader {
    /// Queries the current SCN through JNI on a Tokio blocking thread, so the caller's async
    /// runtime is never blocked.
    ///
    /// Cancellation safe: dropping the future cancels the in-flight Oracle query, so its
    /// connection returns to the pool promptly instead of being held for later calls.
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let pool = self.pool().await?;
        let cancel = invoke_jni_new_statement_cancel(&self.table, "current SCN query")?;
        let cancel_on_drop =
            cancel_statement_on_drop(cancel.clone(), self.table.clone(), "current SCN query");
        let result =
            tokio::task::spawn_blocking(move || invoke_jni_current_scn(&pool, &cancel)).await;
        cancel_on_drop.disarm();
        let scn = result.context("failed to join Oracle SCN query task")??;
        if scn <= 0 {
            bail!("Oracle returned an invalid current SCN: {scn}");
        }
        Ok(CdcOffset::Oracle(OracleOffset { scn: scn as u64 }))
    }

    /// Streams the table from `start_pk` to its end with a single Oracle query, ignoring `limit`.
    /// The query runs through JNI on a Tokio blocking thread and hands rows over through a bounded
    /// channel, so the caller's async runtime is never blocked and back pressure reaches Oracle.
    ///
    /// Cancellation safe: dropping the stream cancels the in-flight Oracle query, so its
    /// connection returns to the pool promptly and the rebuilt stream's query does not wait on it.
    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys)
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        // TODO(#26804): Implement Oracle snapshot split discovery for parallel CDC backfill.
        stream::empty().boxed()
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        // TODO(#26804): Implement Oracle split-range snapshot reads for parallel CDC backfill.
        stream::once(async {
            Err(anyhow!("Oracle CDC parallelized backfill is not implemented").into())
        })
        .boxed()
    }
}

impl OracleExternalTableReader {
    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        // TODO(#26804): Parse Oracle Debezium offsets when Oracle CDC backfill is implemented.
        Box::new(|_offset| Err(anyhow!("Oracle CDC offset parsing is not supported yet").into()))
    }

    pub fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        if pk_indices.is_empty() {
            bail!("Oracle snapshot reader requires at least one primary-key column");
        }
        for &index in &pk_indices {
            if index >= rw_schema.len() {
                bail!(
                    "Oracle snapshot primary-key index {index} is outside a {}-column schema",
                    rw_schema.len()
                );
            }
        }

        let table_schema = TableSchema {
            columns: rw_schema
                .fields
                .iter()
                .map(|field| {
                    ColumnDesc::named(
                        field.name.clone(),
                        ColumnId::placeholder(),
                        field.data_type.clone(),
                    )
                    .to_protobuf()
                })
                .collect(),
            pk_indices: pk_indices.iter().map(|index| *index as u32).collect(),
        };

        Ok(Self {
            connection_properties: config.oracle_connection_properties()?,
            rw_schema,
            pk_indices,
            table_schema,
            table: format!("{}.{}", config.schema, config.table),
            pool: OnceCell::new(),
        })
    }

    /// Returns the reader's connection pool, opening it through JNI on a Tokio blocking thread on
    /// first use, so the caller's async runtime is never blocked.
    ///
    /// Cancellation safe: if the caller stops waiting, the pool the blocking task opens is closed
    /// when the task's result is dropped, and the next call opens a new one.
    async fn pool(&self) -> ConnectorResult<JavaRef> {
        let pool = self
            .pool
            .get_or_try_init(|| async {
                let properties = self.connection_properties.clone();
                let table = self.table.clone();
                tokio::task::spawn_blocking(move || OraclePool::open(&properties, table))
                    .await
                    .context("failed to join Oracle connection pool open task")?
                    .map_err(ConnectorError::from)
            })
            .await?;
        Ok(pool.pool.clone())
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) {
        if primary_keys.len() != self.pk_indices.len() {
            Err(anyhow!(
                "Oracle snapshot read received {} primary-key columns but there are actually {} primary-key columns",
                primary_keys.len(),
                self.pk_indices.len()
            ))?;
        }

        if let Some(start_pk) = &start_pk {
            validate_start_pk(start_pk, self.pk_indices.len())?;
        }

        let pool = self.pool().await?;
        let cancel = invoke_jni_new_statement_cancel(&self.table, "snapshot read")?;
        let cancel_on_drop =
            cancel_statement_on_drop(cancel.clone(), self.table.clone(), "snapshot read");
        let request = OracleSnapshotReadRequest {
            schema_name: table_name.schema_name,
            table_name: table_name.table_name,
            table_schema: Some(self.table_schema.clone()),
            primary_keys,
        };
        let (sender, mut receiver) = mpsc::channel(ORACLE_SNAPSHOT_CHANNEL_SIZE);
        let read_task = tokio::task::spawn_blocking(move || {
            invoke_jni_snapshot_read(&pool, &request, start_pk, &sender, &cancel)
        });

        while let Some(response) = receiver.recv().await {
            let chunk = decode_arrow_batch(&response?.arrow_ipc, &self.rw_schema)?;
            for row in chunk.rows() {
                // Oracle guarantees that no column of a primary key is NULL ("Primary Key
                // Constraints" in https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/constraint.html).
                // Pagination depends on it rather than only checking it: the keyset predicate uses
                // `=` and `>`, which never match NULL, the next start key cannot be NULL, and
                // parallel CDC backfill reserves an all-NULL key for an unbounded split edge.
                if let Some(&index) = self
                    .pk_indices
                    .iter()
                    .find(|&&index| row.datum_at(index).is_none())
                {
                    Err(anyhow!(
                        "Oracle snapshot primary key `{}` cannot be NULL",
                        self.rw_schema.fields[index].name
                    ))?;
                }
                yield row.into_owned_row();
            }
        }
        let result = read_task.await;
        cancel_on_drop.disarm();
        result.context("failed to join Oracle snapshot query task")??;
    }
}

/// A global reference to a Java object that is deleted on a thread attached to the JVM, so
/// dropping it does not log the detached-thread warning of [`GlobalRef`].
#[derive(Debug, Clone)]
struct JavaRef(Option<GlobalRef>);

impl JavaRef {
    fn as_obj(&self) -> &jni::objects::JObject<'static> {
        self.0
            .as_ref()
            .expect("Java reference is present until dropped")
    }
}

impl Drop for JavaRef {
    fn drop(&mut self) {
        let Some(object) = self.0.take() else {
            return;
        };
        let attached = Jvm::get_or_init()
            .ok()
            .and_then(|jvm| jvm.0.attach_current_thread().ok());
        drop(object);
        drop(attached);
    }
}

/// Runs a release action once when dropped, on a blocking thread when inside a Tokio runtime.
struct ReleaseOnDrop(Option<Box<dyn FnOnce() + Send + Sync>>);

impl ReleaseOnDrop {
    fn new(release: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(release)))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl fmt::Debug for ReleaseOnDrop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReleaseOnDrop")
            .field(&self.0.is_some())
            .finish()
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        let Some(release) = self.0.take() else {
            return;
        };
        let span = tracing::Span::current();
        let release = move || span.in_scope(release);
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => drop(handle.spawn_blocking(release)),
            Err(_) => release(),
        }
    }
}

/// A UCP connection pool owned by one [`OracleExternalTableReader`] and closed when dropped.
#[derive(Debug)]
struct OraclePool {
    pool: JavaRef,
    _close_on_drop: ReleaseOnDrop,
}

impl OraclePool {
    fn open(properties: &OracleConnectionProperties, table: String) -> anyhow::Result<Self> {
        let pool = invoke_jni_open_pool(properties)?;
        let closing = pool.clone();
        Ok(Self {
            pool,
            _close_on_drop: ReleaseOnDrop::new(move || {
                tracing::info!(table, "closing Oracle connection pool");
                match invoke_jni_close_pool(&closing) {
                    Ok(()) => tracing::info!(table, "closed Oracle connection pool"),
                    Err(error) => tracing::warn!(
                        table,
                        error = %error.as_report(),
                        "failed to close Oracle connection pool"
                    ),
                }
            }),
        })
    }
}

/// Cancels the Oracle query watched by `cancel` when dropped, unless disarmed first.
fn cancel_statement_on_drop(cancel: JavaRef, table: String, query: &'static str) -> ReleaseOnDrop {
    ReleaseOnDrop::new(move || {
        if let Err(error) = invoke_jni_cancel_statement(&cancel) {
            tracing::warn!(
                table,
                query,
                error = %error.as_report(),
                "failed to cancel Oracle query"
            );
        }
    })
}

impl ExternalTableConfig {
    pub fn oracle_connection_properties(&self) -> ConnectorResult<OracleConnectionProperties> {
        let pdb_name = self
            .pdb_name
            .clone()
            .context("Oracle CDC requires 'database.pdb.name'")?;
        Ok(OracleConnectionProperties {
            properties: HashMap::from([
                ("hostname".to_owned(), self.host.clone()),
                ("port".to_owned(), self.port.clone()),
                ("username".to_owned(), self.username.clone()),
                ("password".to_owned(), self.password.clone()),
                ("database.name".to_owned(), self.database.clone()),
                ("database.pdb.name".to_owned(), pdb_name),
                ("schema.name".to_owned(), self.schema.clone()),
                ("table.name".to_owned(), self.table.clone()),
            ]),
        })
    }
}

fn validate_start_pk(row: &OwnedRow, pk_len: usize) -> ConnectorResult<()> {
    if row.len() != pk_len {
        bail!(
            "Oracle snapshot start key has {} values for {} primary-key columns",
            row.len(),
            pk_len
        );
    }
    if let Some(position) = row.iter().position(|datum| datum.is_none()) {
        bail!("Oracle snapshot primary-key position {position} cannot be NULL");
    }
    Ok(())
}

fn decode_arrow_batch(arrow_ipc: &[u8], schema: &Schema) -> ConnectorResult<DataChunk> {
    let mut reader = StreamReader::try_new(Cursor::new(arrow_ipc), None)
        .context("failed to read Oracle snapshot Arrow stream")?;
    let batch = reader
        .next()
        .context("Oracle snapshot Arrow stream has no record batch")?
        .context("failed to read Oracle snapshot Arrow record batch")?;
    let chunk = OracleArrowConvert
        .from_record_batch(&batch)
        .context("failed to convert Oracle snapshot Arrow record batch")?;
    if chunk.data_types() != schema.data_types() {
        bail!(
            "Oracle snapshot batch has types {:?} but the table schema has types {:?}",
            chunk.data_types(),
            schema.data_types()
        );
    }
    Ok(chunk)
}

struct OracleArrowConvert;

impl Arrow58FromArrow for OracleArrowConvert {}

fn invoke_jni_discover(properties: &OracleConnectionProperties) -> anyhow::Result<TableSchema> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let properties_bytes = env.byte_array_from_slice(&properties.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] discover(byte[] propertiesBytes)},
            &properties_bytes
        )?;
        TableSchema::decode(risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref())
            .map_err(Into::into)
    })
}

fn invoke_jni_open_pool(properties: &OracleConnectionProperties) -> anyhow::Result<JavaRef> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let properties_bytes = env.byte_array_from_slice(&properties.encode_to_vec())?;
        let pool = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {Object openPool(byte[] propertiesBytes)},
            &properties_bytes
        )?;
        Ok(JavaRef(Some(env.new_global_ref(pool)?)))
    })
}

fn invoke_jni_close_pool(pool: &JavaRef) -> anyhow::Result<()> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {void closePool(Object)},
            pool.as_obj()
        )?;
        Ok(())
    })
}

fn invoke_jni_current_scn(pool: &JavaRef, cancel: &JavaRef) -> anyhow::Result<i64> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let scn = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {long currentScn(Object, Object)},
            pool.as_obj(),
            cancel.as_obj()
        )?;
        Ok(scn)
    })
}

fn invoke_jni_new_statement_cancel(table: &str, query: &str) -> anyhow::Result<JavaRef> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let table = env.new_string(table)?;
        let query = env.new_string(query)?;
        let cancel = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {Object newStatementCancel(String, String)},
            &table,
            &query
        )?;
        Ok(JavaRef(Some(env.new_global_ref(cancel)?)))
    })
}

fn invoke_jni_cancel_statement(cancel: &JavaRef) -> anyhow::Result<()> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {void cancelStatement(Object)},
            cancel.as_obj()
        )?;
        Ok(())
    })
}

fn invoke_jni_snapshot_read(
    pool: &JavaRef,
    request: &OracleSnapshotReadRequest,
    start_pk: Option<OwnedRow>,
    sender: &JniSenderType<OracleSnapshotReadResponse>,
    cancel: &JavaRef,
) -> anyhow::Result<()> {
    let start_pk = start_pk.map(JavaBindingIterator::new_singleton_row);
    let start_pk_pointer = start_pk
        .as_ref()
        .map_or(0, |row| row as *const JavaBindingIterator<'_> as jlong);
    let channel_pointer = sender as *const JniSenderType<OracleSnapshotReadResponse> as jlong;
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {void snapshotRead(Object, byte[] requestBytes, long startPkPointer, long channelPtr, Object)},
            pool.as_obj(),
            &request_bytes,
            start_pk_pointer,
            channel_pointer,
            cancel.as_obj()
        )?;
        Ok(())
    })
}
