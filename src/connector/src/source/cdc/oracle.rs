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

use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};

use crate::source::cdc::external::{SCHEMA_NAME_KEY, SchemaTableName, TABLE_NAME_KEY};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait};
use crate::source::cdc::{
    CdcExternalTableNameParser, CdcSerializedFieldNames, CdcSourceFrontendValidator,
    CdcSourceRuntimeFlags, Oracle,
    db_name_end_and_table_name_start_from_schema_qualified_topic_suffix,
};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Oracle {}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Oracle {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[
        "cdc.source.wait.streaming.start.timeout",
        "debezium.max.queue.size",
        "debezium.queue.memory.ratio",
        "debezium.heartbeat.interval.ms",
        "heartbeat.table.name",
        "hostname",
        "port",
        "password",
    ];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] = &[
        (PbFormatType::Debezium, &[PbEncodeType::Json]),
        // support source stream job
        (PbFormatType::Plain, &[PbEncodeType::Json]),
    ];
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Oracle {}

// ------- Table name -------

impl CdcExternalTableNameParser for Oracle {
    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        SchemaTableName {
            schema_name: options.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default(),
            table_name: options.get(TABLE_NAME_KEY).cloned().unwrap_or_default(),
        }
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        db_name_end_and_table_name_start_from_schema_qualified_topic_suffix(topic_suffix)
    }
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct OracleCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for OracleCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }
}
