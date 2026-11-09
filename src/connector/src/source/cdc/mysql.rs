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

use rand::Rng;
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceExternalTableIdentifier, CdcSourceRuntimeFlags,
    CdcSourceTypeTrait, Mysql, check_alter_with_options_inner, check_common_with_options,
};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Mysql {
    const CAN_BACKFILL: bool = true;
    const CAN_USE_TRANSACTION_METADATA: bool = true;
    const NEEDS_NON_EMPTY_INITIAL_OFFSET: bool = true;
}

// ------- DDL validation -------

impl CdcSourceDdlValidator for Mysql {
    fn check_and_fill_create_with_options(
        props: &mut BTreeMap<String, String>,
    ) -> ConnectorResult<()> {
        check_common_with_options(props)?;
        // Generate a random server id for mysql cdc source if needed
        // `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
        // group (that is, different from any other server id being used by any master or slave)
        props
            .entry("server.id".to_owned())
            .or_insert(rand::rng().random_range(1..u32::MAX).to_string());
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
                "hostname",
                "port",
                "password",
            ],
        )
    }
}

// ------- External table identifier -------

impl CdcSourceExternalTableIdentifier for Mysql {}
