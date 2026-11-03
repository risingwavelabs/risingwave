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

use risingwave_common::bail;
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceRuntimeFlags, CdcSourceTypeTrait, Postgres,
    check_alter_with_options_inner, check_common_with_options,
};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Postgres {
    const CAN_BACKFILL: bool = true;
    const CAN_BACKFILL_IN_PARALLEL: bool = true;
    const CAN_USE_TRANSACTION_METADATA: bool = true;
    const NEEDS_OFFSET_COMMIT_ON_CHECKPOINT: bool = true;
}

// ------- DDL validation -------

impl CdcSourceDdlValidator for Postgres {
    fn check_and_fill_create_with_options(
        props: &mut BTreeMap<String, String>,
    ) -> ConnectorResult<()> {
        check_common_with_options(props)?;
        match props.get("slot.name") {
            None => {
                // Build a random slot name with UUID
                // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
                let uuid = uuid::Uuid::new_v4();
                props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));
            }
            Some(slot_name) => {
                // please refer to
                // - https://github.com/debezium/debezium/blob/97956ce25b7612e3413d363658661896b7d2e0a2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java#L1179
                // - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
                if !slot_name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    || slot_name.len() > 63
                {
                    bail!(
                        "Invalid replication slot name: {:?}. Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63",
                        slot_name
                    );
                }
            }
        }

        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
        if !props.contains_key("publication.name") {
            // Build a random publication name with UUID to avoid conflicts between sources
            // e.g. "rw_publication_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4();
            props.insert(
                "publication.name".into(),
                format!("rw_publication_{}", uuid.simple()),
            );
        }
        if !props.contains_key("publication.create.enable") {
            // Default auto create publication if doesn't exist
            props.insert("publication.create.enable".into(), "true".into());
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
                "hostname",
                "port",
                "password",
            ],
        )
    }
}
