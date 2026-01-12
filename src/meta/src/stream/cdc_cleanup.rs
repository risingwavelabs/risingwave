// Copyright 2025 RisingWave Labs
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

//! CDC cleanup utilities for managing PostgreSQL replication slots lifecycle.

use risingwave_connector::connector_common::postgres::{create_pg_client, SslMode};
use risingwave_connector::source::cdc::{CITUS_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR};
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::catalog::PbSource;
use thiserror_ext::AsReport;

use crate::MetaResult;

/// Attempts to drop a PostgreSQL replication slot for a given source.
///
/// This is a best-effort operation - if the slot doesn't exist or PostgreSQL is unreachable,
/// a warning will be logged but no error will be returned.
///
/// # Arguments
/// * `source` - The source catalog containing connection information
///
/// # Returns
/// * `Ok(())` if the slot was successfully dropped or didn't exist
/// * `Err(...)` only for critical errors (currently never returns error)
pub async fn try_drop_postgres_slot(source: &PbSource) -> MetaResult<()> {
    let with_properties = &source.with_properties;
    
    // Check if this is a PostgreSQL CDC source
    let connector = match with_properties.get_connector() {
        Some(connector) => connector,
        None => {
            tracing::debug!(source_id = %source.id, "Source has no connector, skipping slot cleanup");
            return Ok(());
        }
    };

    if connector != POSTGRES_CDC_CONNECTOR && connector != CITUS_CDC_CONNECTOR {
        tracing::debug!(
            source_id = %source.id,
            connector = connector,
            "Source is not PostgreSQL CDC, skipping slot cleanup"
        );
        return Ok(());
    }

    // Extract connection parameters
    let hostname = match with_properties.get("hostname") {
        Some(h) => h,
        None => {
            tracing::warn!(source_id = %source.id, "Missing hostname in source properties");
            return Ok(());
        }
    };

    let port = with_properties.get("port").map(|s| s.as_str()).unwrap_or("5432");
    let username = with_properties.get("username").map(|s| s.as_str()).unwrap_or("postgres");
    let password = with_properties.get("password").map(|s| s.as_str()).unwrap_or("");
    let database = match with_properties.get("database.name") {
        Some(db) => db,
        None => {
            tracing::warn!(source_id = %source.id, "Missing database.name in source properties");
            return Ok(());
        }
    };

    let slot_name = match with_properties.get("slot.name") {
        Some(s) => s,
        None => {
            tracing::warn!(
                source_id = %source.id,
                "Missing slot.name in source properties, cannot drop slot"
            );
            return Ok(());
        }
    };

    // Get SSL configuration
    let ssl_mode = with_properties
        .get("ssl.mode")
        .and_then(|s| s.parse::<SslMode>().ok())
        .unwrap_or(SslMode::Preferred);

    let ssl_root_cert = with_properties.get("ssl.root.cert").cloned();

    tracing::info!(
        source_id = %source.id,
        hostname = hostname,
        port = port,
        database = database,
        slot_name = slot_name,
        "Attempting to drop PostgreSQL replication slot"
    );

    // Try to connect and drop the slot
    match drop_replication_slot_inner(
        username,
        password,
        hostname,
        port,
        database,
        slot_name,
        &ssl_mode,
        &ssl_root_cert,
    )
    .await
    {
        Ok(()) => {
            tracing::info!(
                source_id = %source.id,
                slot_name = slot_name,
                "Successfully dropped PostgreSQL replication slot"
            );
        }
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                source_id = %source.id,
                slot_name = slot_name,
                hostname = hostname,
                database = database,
                "Failed to drop PostgreSQL replication slot. \
                The slot may need to be manually cleaned up to free resources."
            );
        }
    }

    Ok(())
}

async fn drop_replication_slot_inner(
    user: &str,
    password: &str,
    host: &str,
    port: &str,
    database: &str,
    slot_name: &str,
    ssl_mode: &SslMode,
    ssl_root_cert: &Option<String>,
) -> anyhow::Result<()> {
    // Create PostgreSQL client
    let client = create_pg_client(user, password, host, port, database, ssl_mode, ssl_root_cert)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

    // Check if the slot exists before trying to drop it
    let check_query = "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1";
    let rows = client
        .query(check_query, &[&slot_name])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to check if replication slot exists: {}", e))?;

    if rows.is_empty() {
        tracing::info!(
            slot_name = slot_name,
            "Replication slot does not exist, nothing to drop"
        );
        return Ok(());
    }

    // Drop the replication slot
    // Note: We use pg_drop_replication_slot() which requires the slot to not be active
    let drop_query = format!("SELECT pg_drop_replication_slot('{}')", slot_name);
    client
        .execute(&drop_query, &[])
        .await
        .map_err(|e| anyhow::anyhow!("Failed to drop replication slot: {}", e))?;

    tracing::info!(slot_name = slot_name, "Replication slot dropped successfully");
    Ok(())
}
