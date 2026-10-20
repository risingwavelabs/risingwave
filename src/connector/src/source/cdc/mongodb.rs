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
    CdcOffset, CdcTableSnapshotSplitOption, DATABASE_NAME_KEY, DebeziumOffset, ExternalTableReader,
    SchemaTableName, TABLE_NAME_KEY,
};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait};
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcDebeziumPayloadInterpreter, CdcExternalTableHandler,
    CdcExternalTableNameParser, CdcSerializedNames, CdcSourceFrontendValidator,
    CdcSourceRuntimeFlags, Mongodb,
    db_name_end_and_table_name_start_from_database_qualified_topic_suffix,
    derived_cdc_offset_variant, derived_debezium_cdc_split_split_field,
};

// ------- Persisted names -------

impl CdcSerializedNames for Mongodb {
    const CDC_OFFSET_VARIANT: &'static str = derived_cdc_offset_variant!(Mongodb);
    const DEBEZIUM_CDC_SPLIT_SPLIT_FIELD: &'static str =
        derived_debezium_cdc_split_split_field!(Mongodb);
}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Mongodb {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[
        "cdc.source.wait.streaming.start.timeout",
        "debezium.max.queue.size",
        "debezium.queue.memory.ratio",
    ];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] =
        &[(PbFormatType::DebeziumMongo, &[PbEncodeType::Json])];
    const SUPPORTED_ADDITIONAL_COLUMNS: Option<&'static [&'static str]> = Some(&[
        "timestamp",
        "partition",
        "offset",
        "database_name",
        "collection_name",
    ]);
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Mongodb {}

// ------- Table name -------

impl CdcExternalTableNameParser for Mongodb {
    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        SchemaTableName {
            schema_name: options.get(DATABASE_NAME_KEY).cloned().unwrap_or_default(),
            table_name: options.get(TABLE_NAME_KEY).cloned().unwrap_or_default(),
        }
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        db_name_end_and_table_name_start_from_database_qualified_topic_suffix(topic_suffix)
    }
}

// ------- Debezium payload -------

impl CdcDebeziumPayloadInterpreter for Mongodb {}

// ------- Debezium offset -------

/// `sourceOffset` of a `MongoDB` Debezium offset, reduced to what RisingWave reads.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MongodbSourceOffset {
    pub snapshot: Option<bool>,
}

impl CdcDebeziumOffsetInterpreter for Mongodb {
    type SourceOffset = MongodbSourceOffset;

    /// Debezium sets `snapshot: true` while snapshotting and drops the key once streaming.
    fn snapshot_done_from_debezium_offset(offset: &DebeziumOffset<MongodbSourceOffset>) -> bool {
        !offset.source_offset.snapshot.unwrap_or(false)
    }
}

// ------- External table -------

/// `MongoDB` has no CDC backfill, so this reader is never constructed.
pub struct MongodbExternalTableReader;

impl ExternalTableReader for MongodbExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        unreachable!("cdc backfill is not supported for mongodb-cdc")
    }

    fn snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        unreachable!("cdc backfill is not supported for mongodb-cdc")
    }

    fn get_parallel_cdc_splits(
        &self,
        _options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        unreachable!("cdc backfill is not supported for mongodb-cdc")
    }

    fn split_snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        unreachable!("cdc backfill is not supported for mongodb-cdc")
    }
}

impl CdcExternalTableHandler for Mongodb {
    type TableReader = MongodbExternalTableReader;
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MongodbCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for MongodbCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }
}
