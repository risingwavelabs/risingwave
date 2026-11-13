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

use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceExternalTableIdentifier, CdcSourceRuntimeFlags,
    CdcSourceTypeTrait, Mariadb, Mysql, check_alter_with_options_inner, check_common_with_options,
};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Mariadb {
    const CAN_AUTO_SCHEMA_CHANGE: bool = true;
    const CAN_BACKFILL: bool = true;
    const CAN_USE_TRANSACTION_METADATA: bool = true;
    const NEEDS_NON_EMPTY_INITIAL_OFFSET: bool = true;
}

// ------- DDL validation -------

impl CdcSourceDdlValidator for Mariadb {
    fn check_and_fill_create_with_options(
        props: &mut BTreeMap<String, String>,
    ) -> ConnectorResult<()> {
        // MariaDB replication needs a unique `server.id` just like MySQL.
        Mysql::check_and_fill_create_with_options(props)
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
                "hostname",
                "port",
                "password",
            ],
        )
    }
}

// ------- External table identifier -------

impl CdcSourceExternalTableIdentifier for Mariadb {}
