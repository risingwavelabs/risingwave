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

use risingwave_common::license::{Feature, FeatureNotAvailable};
use risingwave_pb::secret::PbSecretRef;

use crate::error::ConnectorResult;
use crate::source::cdc::external::{DATABASE_NAME_KEY, SchemaTableName};
use crate::source::cdc::{
    CdcSourceDdlValidator, CdcSourceExternalTableIdentifier, CdcSourceRuntimeFlags,
    CdcSourceTypeTrait, SqlServer, check_alter_with_options_inner, check_common_with_options,
    split_quoted_identifier_inner,
};

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for SqlServer {
    const CAN_BACKFILL: bool = true;
    const NEEDS_NON_EMPTY_INITIAL_OFFSET: bool = true;
    // `CAN_USE_TRANSACTION_METADATA` stays false: Debezium only sends a transaction's `END` event
    // when the next transaction starts, which delays the newest events.
    // See https://debezium.io/documentation/reference/2.6/connectors/sqlserver.html#sqlserver-transaction-metadata
}

// ------- DDL validation -------

impl CdcSourceDdlValidator for SqlServer {
    fn check_and_fill_create_with_options(
        props: &mut BTreeMap<String, String>,
    ) -> ConnectorResult<()> {
        check_common_with_options(props)?;
        if !props.contains_key("schema.name") {
            // Default schema name is "dbo"
            props.insert("schema.name".into(), "dbo".into());
        }
        Ok(())
    }

    fn check_license() -> Result<(), FeatureNotAvailable> {
        Feature::SqlServerCdcSource.check_available()
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

impl CdcSourceExternalTableIdentifier for SqlServer {
    /// Debezium SQL Server always names topics in multi-partition mode, so the topic suffix
    /// starts with the database: `database.schema.table`. The database part must be the name
    /// SQL Server reports, which the Java validator guarantees by checking `database.name`
    /// against `DB_NAME()` exactly.
    fn canonical_name(connect_properties: &BTreeMap<String, String>) -> String {
        let SchemaTableName {
            schema_name,
            table_name,
        } = SchemaTableName::from_properties(connect_properties);
        let database_name = connect_properties
            .get(DATABASE_NAME_KEY)
            .map(String::as_str)
            .unwrap_or_default();
        format!("{database_name}.{schema_name}.{table_name}")
    }

    fn split_quoted_identifier(name: &str) -> Option<Vec<String>> {
        split_quoted_identifier_inner(name, '[', ']')
    }
}
