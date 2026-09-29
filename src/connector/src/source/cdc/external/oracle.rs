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
use std::ops::Deref;

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use futures_async_stream::try_stream;
use prost::Message;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::global_jvm::Jvm;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, ScalarImpl, ToText};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;
use risingwave_pb::connector_service::{
    OracleDatum, OracleExternalTableRequest, OracleExternalTableResponse,
    OracleOldestOpenTransactionStartScn, OracleRow, TableSchema,
};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, ExternalTableConfig,
    ExternalTableReader, SchemaTableName,
};

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
    /// Find the earliest open transaction per instance for the new-table preflight.
    /// The table-time gate compares these starts with the source split's fixed initial mining SCN.
    /// This query does not establish redo availability.
    pub async fn oldest_open_transaction_scns(
        config: &ExternalTableConfig,
    ) -> ConnectorResult<Vec<OracleOldestOpenTransactionStartScn>> {
        let request = OracleExternalTableRequest {
            properties: config.oracle_connection_properties(),
            ..Default::default()
        };
        let response =
            tokio::task::spawn_blocking(move || invoke_jni_oldest_open_transaction_scns(&request))
                .await
                .context("failed to join Oracle oldest-open-transaction query task")??;
        ensure_success(&response)?;
        Ok(response.oldest_open_transaction_scns)
    }

    /// Reject creating a table if any open transaction predates the source's initial mining SCN.
    /// Passing this check does not prove redo availability for any mining thread.
    pub fn check_open_transactions(
        initial_mining_scn: u64,
        oldest_start_scns: &[OracleOldestOpenTransactionStartScn],
    ) -> ConnectorResult<()> {
        for transaction in oldest_start_scns {
            if transaction.start_scn < initial_mining_scn {
                bail!(
                    "Cannot create Oracle CDC table: instance {} has an open transaction starting at SCN {} before the source's initial mining SCN {}. This transaction may be unrelated to the requested table, but its affected tables are unknown. Recreate the Oracle CDC source before retrying table creation",
                    transaction.instance_id,
                    transaction.start_scn,
                    initial_mining_scn
                );
            }
        }
        Ok(())
    }

    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let request = OracleExternalTableRequest {
            properties: config.oracle_connection_properties(),
            ..Default::default()
        };
        let response = tokio::task::spawn_blocking(move || discover_table(request))
            .await
            .context("failed to join Oracle schema discovery task")??;
        ensure_success(&response)?;

        let table_schema = response
            .table_schema
            .context("Oracle schema discovery returned no table schema")?;
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
    config: ExternalTableConfig,
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    table_schema: TableSchema,
}

impl ExternalTableReader for OracleExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let request = OracleExternalTableRequest {
            properties: self.config.oracle_connection_properties(),
            ..Default::default()
        };
        let response = tokio::task::spawn_blocking(move || current_scn(request))
            .await
            .context("failed to join Oracle SCN query task")??;
        ensure_success(&response)?;
        if response.snapshot_scn == 0 {
            bail!("Oracle returned an invalid current SCN");
        }
        Ok(CdcOffset::Oracle(OracleOffset {
            decoded_commit_scn: response.snapshot_scn,
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
            config,
            rw_schema,
            pk_indices,
            table_schema,
        })
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

        let start_pk = start_pk
            .map(|row| encode_start_pk(row, &self.rw_schema, &self.pk_indices))
            .transpose()?;

        let properties = self
            .config
            .oracle_connection_properties_with_schema_table_name(table_name);
        let request = OracleExternalTableRequest {
            properties,
            table_schema: Some(self.table_schema.clone()),
            start_pk: start_pk.unwrap_or_default(),
            primary_keys,
            limit,
        };
        let response = tokio::task::spawn_blocking(move || read_snapshot(request))
            .await
            .context("failed to join Oracle snapshot query task")??;
        ensure_success(&response)?;

        for row in response.rows {
            yield decode_row(row, &self.rw_schema, &self.pk_indices)?;
        }
    }
}

impl ExternalTableConfig {
    pub fn oracle_connection_properties(&self) -> HashMap<String, String> {
        HashMap::from([
            ("hostname".to_owned(), self.host.clone()),
            ("port".to_owned(), self.port.clone()),
            ("username".to_owned(), self.username.clone()),
            ("password".to_owned(), self.password.clone()),
            ("database.name".to_owned(), self.database.clone()),
            ("database.pdb.name".to_owned(), self.pdb_name.clone()),
            ("schema.name".to_owned(), self.schema.clone()),
            ("table.name".to_owned(), self.table.clone()),
            (
                "debezium.rac.nodes".to_owned(),
                self.rac_nodes.clone().unwrap_or_default(),
            ),
        ])
    }

    pub fn oracle_connection_properties_with_schema_table_name(
        &self,
        table_name: SchemaTableName,
    ) -> HashMap<String, String> {
        let mut properties = self.oracle_connection_properties();
        properties.insert("schema.name".to_owned(), table_name.schema_name);
        properties.insert("table.name".to_owned(), table_name.table_name);
        properties
    }
}

fn encode_start_pk(
    row: OwnedRow,
    schema: &Schema,
    pk_indices: &[usize],
) -> ConnectorResult<Vec<OracleDatum>> {
    if row.len() != pk_indices.len() {
        bail!(
            "Oracle snapshot start key has {} values for {} primary-key columns",
            row.len(),
            pk_indices.len()
        );
    }
    row.into_iter()
        .zip_eq_fast(pk_indices)
        .enumerate()
        .map(|(position, (datum, &schema_index))| {
            let scalar = datum.with_context(|| {
                format!("Oracle snapshot primary-key position {position} cannot be NULL")
            })?;
            let value = match &schema.fields[schema_index].data_type {
                DataType::Bytea => scalar.into_bytea().into_vec(),
                data_type => scalar
                    .as_scalar_ref_impl()
                    .to_text_with_type(data_type)
                    .into_bytes(),
            };
            Ok(OracleDatum {
                is_null: false,
                value,
            })
        })
        .collect()
}

fn decode_row(row: OracleRow, schema: &Schema, pk_indices: &[usize]) -> ConnectorResult<OwnedRow> {
    if row.values.len() != schema.len() {
        bail!(
            "Oracle snapshot row has {} values for a {}-column schema",
            row.values.len(),
            schema.len()
        );
    }

    let datums = row
        .values
        .into_iter()
        .zip_eq_fast(&schema.fields)
        .enumerate()
        .map(|(index, (datum, field))| {
            if datum.is_null {
                if pk_indices.contains(&index) {
                    return Err(anyhow!(
                        "Oracle snapshot primary key `{}` cannot be NULL",
                        field.name
                    ));
                }
                return Ok(None);
            }

            let parsed = match &field.data_type {
                DataType::Bytea => Ok(ScalarImpl::Bytea(datum.value.into())),
                data_type => {
                    let text = std::str::from_utf8(&datum.value).with_context(|| {
                        format!("Oracle snapshot column `{}` is not valid UTF-8", field.name)
                    })?;
                    ScalarImpl::from_text(text, data_type).map_err(|error| anyhow!(error))
                }
            };
            match parsed {
                Ok(value) => Ok(Some(value)),
                Err(error) if pk_indices.contains(&index) => Err(error.context(format!(
                    "failed to decode Oracle snapshot primary key `{}`",
                    field.name
                ))),
                Err(error) => {
                    tracing::warn!(
                        column = field.name,
                        data_type = %field.data_type,
                        error = %error.as_report(),
                        "failed to decode Oracle snapshot value; using NULL",
                    );
                    Ok(None)
                }
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(OwnedRow::new(datums))
}

fn ensure_success(response: &OracleExternalTableResponse) -> ConnectorResult<()> {
    if let Some(error) = &response.error {
        bail!(
            "Oracle external table operation failed: {}",
            error.error_message
        );
    }
    Ok(())
}

fn discover_table(
    request: OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_discover(&request)
}

fn current_scn(request: OracleExternalTableRequest) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_current_scn(&request)
}

fn read_snapshot(
    request: OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    invoke_jni_snapshot_read(&request)
}

fn invoke_jni_discover(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] discover(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

fn invoke_jni_current_scn(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] currentScn(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

fn invoke_jni_oldest_open_transaction_scns(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] oldestOpenTransactionScns(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

fn invoke_jni_snapshot_read(
    request: &OracleExternalTableRequest,
) -> anyhow::Result<OracleExternalTableResponse> {
    let jvm = Jvm::get_or_init()?;
    execute_with_jni_env(jvm, |env| {
        let request_bytes = env.byte_array_from_slice(&request.encode_to_vec())?;
        let response_bytes = call_static_method!(
            env,
            {com.risingwave.connector.source.common.JniOracleExternalTable},
            {byte[] snapshotRead(byte[] requestBytes)},
            &request_bytes
        )?;
        OracleExternalTableResponse::decode(
            risingwave_jni_core::to_guarded_slice(&response_bytes, env)?.deref(),
        )
        .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {
    use super::{OracleExternalTable, OracleOldestOpenTransactionStartScn};

    #[test]
    fn test_open_transaction_mining_boundary() {
        let transaction = |instance_id, start_scn| OracleOldestOpenTransactionStartScn {
            instance_id,
            start_scn,
        };
        let boundary = 100;

        let error = OracleExternalTable::check_open_transactions(boundary, &[transaction(0, 99)])
            .unwrap_err()
            .to_string();
        assert!(error.contains("Cannot create Oracle CDC table"));
        assert!(error.contains("starting at SCN 99"));
        assert!(error.contains("initial mining SCN 100"));

        for transactions in [vec![], vec![transaction(0, 100)], vec![transaction(0, 101)]] {
            OracleExternalTable::check_open_transactions(boundary, &transactions).unwrap();
        }

        // Every instance must pass; an acceptable first instance cannot hide a violation.
        let error = OracleExternalTable::check_open_transactions(
            boundary,
            &[transaction(1, 120), transaction(2, 90)],
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("instance 2"));
        assert!(error.contains("starting at SCN 90"));
    }
}
