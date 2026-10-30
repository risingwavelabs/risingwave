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

use anyhow::anyhow;
use risingwave_common::bail;
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceRuntimeFlags, CdcSourceTypeTrait, Oracle,
    check_alter_with_options_inner, check_common_with_options,
};

const HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY: &str = "heartbeat.table.auto.initialize";

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Oracle {
    const CAN_BACKFILL: bool = true;
    const NEEDS_NON_EMPTY_INITIAL_OFFSET: bool = true;
}

// ------- DDL validation -------

impl CdcSourceDdlValidator for Oracle {
    fn check_and_fill_create_with_options(
        props: &mut BTreeMap<String, String>,
    ) -> ConnectorResult<()> {
        check_common_with_options(props)?;
        if let Some(value) = props.get(HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY)
            && value.parse::<bool>().map_err(|_| {
                anyhow!("'{HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY}' must be 'true' or 'false'")
            })?
            && !props
                .get("heartbeat.table.name")
                .is_some_and(|value| !value.trim().is_empty())
        {
            bail!("'{HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY}' requires 'heartbeat.table.name'");
        }
        Ok(())
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
                "debezium.heartbeat.interval.ms",
                "heartbeat.table.name",
                "hostname",
                "port",
                "password",
            ],
        )
    }
}
