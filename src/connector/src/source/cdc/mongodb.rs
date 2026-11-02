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
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceRuntimeFlags, CdcSourceTypeTrait, Mongodb,
    check_alter_with_options_inner, check_common_with_options, check_format_encode_inner,
    check_include_column_inner,
};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Mongodb {}

// ------- DDL validation -------

impl CdcSourceDdlValidator for Mongodb {
    fn check_format_encode(format: PbFormatType, encode: PbEncodeType) -> ConnectorResult<()> {
        check_format_encode_inner(
            Self::CDC_CONNECTOR_NAME,
            format,
            encode,
            &[(PbFormatType::DebeziumMongo, &[PbEncodeType::Json])],
        )
    }

    fn check_include_column(additional_column_type: &str) -> ConnectorResult<()> {
        check_include_column_inner(
            Self::CDC_CONNECTOR_NAME,
            additional_column_type,
            &[
                "timestamp",
                "partition",
                "offset",
                "database_name",
                "collection_name",
            ],
        )
    }

    fn check_alter_with_options(
        props: &BTreeMap<String, String>,
        secret_refs: &BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<()> {
        check_common_with_options(props)?;
        check_alter_with_options_inner(
            Self::CDC_CONNECTOR_NAME,
            props,
            secret_refs,
            &[
                "cdc.source.wait.streaming.start.timeout",
                "debezium.max.queue.size",
                "debezium.queue.memory.ratio",
            ],
        )
    }
}
