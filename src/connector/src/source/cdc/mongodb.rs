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

use crate::source::cdc::external::{DATABASE_NAME_KEY, SchemaTableName, TABLE_NAME_KEY};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait};
use crate::source::cdc::{
    CdcDebeziumPayloadInterpreter, CdcExternalTableNameParser, CdcSerializedFieldNames,
    CdcSourceFrontendValidator, CdcSourceRuntimeFlags, Mongodb,
    db_name_end_and_table_name_start_from_database_qualified_topic_suffix,
};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Mongodb {}

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
