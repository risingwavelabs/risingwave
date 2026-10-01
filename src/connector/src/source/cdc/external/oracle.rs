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
use std::io::Cursor;
use std::ops::Deref;

use anyhow::{Context, anyhow};
use arrow_58_ipc::reader::StreamReader;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use futures_async_stream::try_stream;
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
    pub decoded_commit_scn: u64,
}

impl OracleOffset {
    /// The per-event commit SCN is attached by RisingWave's Java CDC consumer; Debezium's
    /// own sourceOffset.commit_scn is a per-redo-thread recovery map, not this event's SCN.
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        // Native Oracle offset fields (such as the string txId) differ from other connectors.
        let dbz_offset: serde_json::Value = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream Oracle CDC offset: {offset}"))?;
        let decoded_commit_scn = dbz_offset
            .pointer("/sourceOffset/decoded_commit_scn")
            .and_then(serde_json::Value::as_str)
            .context("Oracle CDC data offset is missing decoded_commit_scn")?
            .parse::<u64>()
            .context("invalid Oracle CDC commit SCN")?;
        if decoded_commit_scn == 0 {
            bail!("Oracle CDC commit SCN must be positive");
        }
        Ok(Self { decoded_commit_scn })
    }
}

pub struct OracleExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl OracleExternalTable {
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
    session_id: OnceCell<i64>,
}

impl ExternalTableReader for OracleExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let session_id = self.session_id().await?;
        let scn = tokio::task::spawn_blocking(move || invoke_jni_current_scn(session_id))
            .await
            .context("failed to join Oracle SCN query task")??;
        if scn <= 0 {
            bail!("Oracle returned an invalid current SCN: {scn}");
        }
        Ok(CdcOffset::Oracle(OracleOffset {
            decoded_commit_scn: scn as u64,
        }))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
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
        Box::new(|offset| {
            Ok(CdcOffset::Oracle(OracleOffset::parse_debezium_offset(
                offset,
            )?))
        })
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
            session_id: OnceCell::new(),
        })
    }

    async fn session_id(&self) -> ConnectorResult<i64> {
        let session_id = self
            .session_id
            .get_or_try_init(|| async {
                let properties = self.connection_properties.clone();
                tokio::task::spawn_blocking(move || invoke_jni_open_session(&properties))
                    .await
                    .context("failed to join Oracle session open task")?
            })
            .await?;
        Ok(*session_id)
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        if limit == 0 {
            Err(anyhow!("Oracle snapshot read has an invalid limit"))?;
        }
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

        let session_id = self.session_id().await?;
        let request = OracleSnapshotReadRequest {
            schema_name: table_name.schema_name,
            table_name: table_name.table_name,
            table_schema: Some(self.table_schema.clone()),
            primary_keys,
            limit,
        };
        let (sender, mut receiver) = mpsc::channel(ORACLE_SNAPSHOT_CHANNEL_SIZE);
        let read_task = tokio::task::spawn_blocking(move || {
            invoke_jni_snapshot_read(session_id, &request, start_pk, &sender)
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
        read_task
            .await
            .context("failed to join Oracle snapshot query task")??;
    }
}

impl Drop for OracleExternalTableReader {
    fn drop(&mut self) {
        let Some(&session_id) = self.session_id.get() else {
            return;
        };
        let close = move || {
            if let Err(error) = invoke_jni_close_session(session_id) {
                tracing::warn!(
                    session_id,
                    error = %error.as_report(),
                    "failed to close Oracle session"
                );
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => drop(handle.spawn_blocking(close)),
            Err(_) => close(),
        }
    }
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
                (
                    "debezium.rac.nodes".to_owned(),
                    self.rac_nodes.clone().unwrap_or_default(),
                ),
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

fn invoke_jni_open_session(properties: &OracleConnectionProperties) -> anyhow::Result<i64> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let properties_bytes = env.byte_array_from_slice(&properties.encode_to_vec())?;
        let session_id = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {long openSession(byte[] propertiesBytes)},
            &properties_bytes
        )?;
        Ok(session_id)
    })
}

fn invoke_jni_close_session(session_id: i64) -> anyhow::Result<()> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {void closeSession(long sessionId)},
            session_id
        )?;
        Ok(())
    })
}

fn invoke_jni_current_scn(session_id: i64) -> anyhow::Result<i64> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let scn = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {long currentScn(long sessionId)},
            session_id
        )?;
        Ok(scn)
    })
}

fn invoke_jni_snapshot_read(
    session_id: i64,
    request: &OracleSnapshotReadRequest,
    start_pk: Option<OwnedRow>,
    sender: &JniSenderType<OracleSnapshotReadResponse>,
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
            {void snapshotRead(long sessionId, byte[] requestBytes, long startPkPointer, long channelPtr)},
            session_id,
            &request_bytes,
            start_pk_pointer,
            channel_pointer
        )?;
        Ok(())
    })
}
