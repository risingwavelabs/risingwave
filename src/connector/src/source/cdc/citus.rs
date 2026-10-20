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

use futures::stream::BoxStream;
use risingwave_common::catalog::Field;
use risingwave_common::row::OwnedRow;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcTableSnapshotSplitOption, DebeziumOffset, ExternalTableReader, SchemaTableName,
};
use crate::source::cdc::postgres::{PostgresSourceOffset, fill_and_validate_postgres_props};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcDebeziumPayloadInterpreter, CdcExternalTableHandler,
    CdcExternalTableNameParser, CdcSerializedNames, CdcSourceFrontendValidator,
    CdcSourceRuntimeFlags, Citus, Postgres, derived_cdc_offset_variant,
    derived_debezium_cdc_split_split_field,
};

// ------- Persisted names -------

impl CdcSerializedNames for Citus {
    const CDC_OFFSET_VARIANT: &'static str = derived_cdc_offset_variant!(Citus);
    const DEBEZIUM_CDC_SPLIT_SPLIT_FIELD: &'static str =
        derived_debezium_cdc_split_split_field!(Citus);
}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Citus {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] =
        Postgres::COMPATIBLE_FORMATS;

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        fill_and_validate_postgres_props(props)
    }
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Citus {}

// ------- Table name -------

/// Citus names tables like Postgres, but `CREATE TABLE ... FROM` is not supported for it.
impl CdcExternalTableNameParser for Citus {
    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        Postgres::schema_table_name_from_options(options)
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        Postgres::db_name_end_and_table_name_start_from_debezium_topic_suffix(topic_suffix)
    }
}

// ------- Debezium payload -------

impl CdcDebeziumPayloadInterpreter for Citus {}

// ------- Debezium offset -------

/// Citus offsets come from Debezium's Postgres connector.
impl CdcDebeziumOffsetInterpreter for Citus {
    type SourceOffset = PostgresSourceOffset;

    fn snapshot_done_from_debezium_offset(offset: &DebeziumOffset<PostgresSourceOffset>) -> bool {
        Postgres::snapshot_done_from_debezium_offset(offset)
    }
}

// ------- External table -------

/// Citus has no CDC backfill, so this reader is never constructed.
pub struct CitusExternalTableReader;

impl ExternalTableReader for CitusExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        unreachable!("cdc backfill is not supported for citus-cdc")
    }

    fn snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        unreachable!("cdc backfill is not supported for citus-cdc")
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        unreachable!("cdc backfill is not supported for citus-cdc")
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        unreachable!("cdc backfill is not supported for citus-cdc")
    }
}

impl CdcExternalTableHandler for Citus {
    type TableReader = CitusExternalTableReader;
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct CitusCdcSplit {
    pub inner: CdcSplitBase,
    /// Host and port of the worker node whose shard tables this split captures.
    pub server_addr: Option<String>,
}

impl CdcSplitTrait for CitusCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }
}

impl DebeziumCdcSplit<Citus> {
    /// Citus has one split per worker node; `server_addr` is that node's host and port.
    pub fn new_with_server_addr(split_id: u32, server_addr: String) -> Self {
        let mut split = Self::new(split_id, None);
        split
            .citus_split
            .as_mut()
            .expect("citus split must exist")
            .server_addr = Some(server_addr);
        split
    }
}
