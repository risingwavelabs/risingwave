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

use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use anyhow::anyhow;
use axum::Json;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use pgwire::pg_server::SessionManager;
use pgwire::types::Format;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk, JsonbArrayBuilder};
use risingwave_common::catalog::{AlterDatabaseParam, DEFAULT_DATABASE_NAME};
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_pb::task_service::fast_insert_response;

use super::types::*;
use crate::catalog::root_catalog::SchemaPath;
use crate::scheduler::choose_fast_insert_client;
use crate::session::{SESSION_MANAGER, SessionManagerImpl};

const RSTREAM_DB_PREFIX: &str = "rstream_";
const RSTREAM_TABLE_NAME: &str = "_records";
const RSTREAM_SCHEMA_NAME: &str = "public";
const RSTREAM_DEFAULT_BARRIER_INTERVAL_MS: u32 = 100;

fn stream_db_name(stream_name: &str) -> String {
    format!("{}{}", RSTREAM_DB_PREFIX, stream_name)
}

fn validate_stream_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(err(
            anyhow!("stream name cannot be empty"),
            StatusCode::BAD_REQUEST,
        ));
    }
    if name.len() > 63 {
        return Err(err(
            anyhow!("stream name too long (max 63 characters)"),
            StatusCode::BAD_REQUEST,
        ));
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(err(
            anyhow!("stream name cannot start with a digit"),
            StatusCode::BAD_REQUEST,
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(err(
            anyhow!("stream name must contain only alphanumeric characters and underscores"),
            StatusCode::BAD_REQUEST,
        ));
    }
    Ok(())
}

fn get_session_mgr() -> &'static Arc<SessionManagerImpl> {
    SESSION_MANAGER
        .get()
        .expect("session manager has been initialized")
}

async fn run_sql(session: &Arc<crate::session::SessionImpl>, sql: &str) -> Result<()> {
    session
        .clone()
        .run_statement(sql.into(), vec![Format::Text])
        .await
        .map_err(|e| {
            err(
                anyhow!("{}: {}", sql, e),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
    Ok(())
}

pub async fn handle_create_stream(
    Json(req): Json<CreateStreamRequest>,
) -> Result<(StatusCode, Json<CreateStreamResponse>)> {
    validate_stream_name(&req.name)?;
    let db_name = stream_db_name(&req.name);
    let session_mgr = get_session_mgr();

    // Check if stream already exists
    {
        let catalog_reader = session_mgr.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        if reader.get_database_by_name(&db_name).is_ok() {
            return Err(err(
                anyhow!("stream '{}' already exists", req.name),
                StatusCode::CONFLICT,
            ));
        }
    }

    // Step 1: Create database using a session on the default database
    let dev_db_id = {
        let catalog_reader = session_mgr.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        reader
            .get_database_by_name(DEFAULT_DATABASE_NAME)
            .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
            .id()
    };

    let dev_session = session_mgr
        .create_dummy_session(dev_db_id)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    run_sql(&dev_session, &format!("CREATE DATABASE {}", db_name)).await?;

    // Step 2: Set barrier interval for low-latency writes
    let new_db_id = {
        let catalog_reader = session_mgr.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        reader
            .get_database_by_name(&db_name)
            .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
            .id()
    };

    dev_session
        .catalog_writer()
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
        .alter_database_param(
            new_db_id,
            AlterDatabaseParam::BarrierIntervalMs(Some(RSTREAM_DEFAULT_BARRIER_INTERVAL_MS)),
        )
        .await
        .map_err(|e| {
            err(
                anyhow!(e).context("failed to set barrier interval"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;

    // Step 3: Create table in the new database
    let stream_session = session_mgr
        .create_dummy_session(new_db_id)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    // Configure single vnode for total ordering
    stream_session
        .set_config("streaming_max_parallelism", "1".to_owned())
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;
    stream_session
        .set_config("streaming_parallelism", "1".to_owned())
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    run_sql(
        &stream_session,
        &format!("CREATE TABLE {} (body JSONB) APPEND ONLY", RSTREAM_TABLE_NAME),
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateStreamResponse { stream: req.name }),
    ))
}

pub async fn handle_delete_stream(Path(name): Path<String>) -> Result<StatusCode> {
    let db_name = stream_db_name(&name);
    let session_mgr = get_session_mgr();

    // Check if stream exists
    let db_id = {
        let catalog_reader = session_mgr.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        reader
            .get_database_by_name(&db_name)
            .map_err(|_| err(anyhow!("stream '{}' not found", name), StatusCode::NOT_FOUND))?
            .id()
    };

    // Drop the table first
    let stream_session = session_mgr
        .create_dummy_session(db_id)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    run_sql(
        &stream_session,
        &format!("DROP TABLE IF EXISTS {}", RSTREAM_TABLE_NAME),
    )
    .await?;

    // Drop the database
    let dev_db_id = {
        let catalog_reader = session_mgr.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        reader
            .get_database_by_name(DEFAULT_DATABASE_NAME)
            .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?
            .id()
    };

    let dev_session = session_mgr
        .create_dummy_session(dev_db_id)
        .map_err(|e| err(e, StatusCode::INTERNAL_SERVER_ERROR))?;

    run_sql(&dev_session, &format!("DROP DATABASE {}", db_name)).await?;

    Ok(StatusCode::OK)
}

pub async fn handle_append_records(
    Extension(counter): Extension<Arc<AtomicU32>>,
    Path(name): Path<String>,
    Json(req): Json<AppendRecordsRequest>,
) -> Result<Json<AppendRecordsResponse>> {
    if req.records.is_empty() {
        return Err(err(
            anyhow!("empty records array"),
            StatusCode::BAD_REQUEST,
        ));
    }

    let db_name = stream_db_name(&name);
    let session_mgr = get_session_mgr();
    let frontend_env = session_mgr.env();

    let request_id = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Look up table info from catalog
    let (table_id, table_version_id, row_id_index) = {
        let catalog_reader = frontend_env.catalog_reader();
        let reader = catalog_reader.read_guard();

        let db = reader
            .get_database_by_name(&db_name)
            .map_err(|_| err(anyhow!("stream '{}' not found", name), StatusCode::NOT_FOUND))?;

        let search_path = Default::default();
        let schema_path = SchemaPath::new(
            Some(RSTREAM_SCHEMA_NAME),
            &search_path,
            risingwave_common::catalog::DEFAULT_SUPER_USER,
        );

        let (table_catalog, _schema) = reader
            .get_any_table_by_name(db.name(), schema_path, RSTREAM_TABLE_NAME)
            .map_err(|e| {
                err(
                    anyhow!(e).context("stream table not found"),
                    StatusCode::INTERNAL_SERVER_ERROR,
                )
            })?;

        (
            table_catalog.id(),
            table_catalog.version_id().expect("table must be versioned"),
            table_catalog.row_id_index.map(|idx| idx as u32),
        )
    };

    // Build DataChunk from the records array
    let count = req.records.len();
    let mut builder = JsonbArrayBuilder::with_type(count, DataType::Jsonb);

    for record in &req.records {
        let jsonb_val = JsonbVal::from(jsonbb::Value::from(record.clone()));
        builder.append(Some(jsonb_val.as_scalar_ref()));
    }

    let data_chunk = DataChunk::new(vec![builder.finish().into_ref()], count);

    // Build FastInsertRequest
    let fast_insert_request = risingwave_pb::task_service::FastInsertRequest {
        table_id,
        table_version_id,
        column_indices: vec![0],
        data_chunk: Some(data_chunk.to_protobuf()),
        row_id_index,
        request_id,
        wait_for_persistence: true,
    };

    // Route to compute node and execute
    let compute_client = choose_fast_insert_client(table_id, frontend_env, request_id)
        .await
        .map_err(|e| {
            err(
                anyhow!(e).context("failed to choose compute node"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;

    let res = compute_client
        .fast_insert(fast_insert_request)
        .await
        .map_err(|e| {
            err(
                anyhow!(e).context("failed to execute fast insert"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;

    if res.status == fast_insert_response::Status::Succeeded as i32 {
        Ok(Json(AppendRecordsResponse { count }))
    } else {
        Err(err(
            anyhow!("fast insert failed: {}", res.error_message),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

pub async fn handle_list_streams() -> Result<Json<ListStreamsResponse>> {
    let session_mgr = get_session_mgr();
    let catalog_reader = session_mgr.env().catalog_reader();
    let reader = catalog_reader.read_guard();

    let streams: Vec<String> = reader
        .iter_databases()
        .filter_map(|db| {
            db.name()
                .strip_prefix(RSTREAM_DB_PREFIX)
                .map(|s| s.to_owned())
        })
        .collect();

    Ok(Json(ListStreamsResponse { streams }))
}

pub async fn handle_get_stream(Path(name): Path<String>) -> Result<Json<GetStreamResponse>> {
    let db_name = stream_db_name(&name);
    let session_mgr = get_session_mgr();
    let catalog_reader = session_mgr.env().catalog_reader();
    let reader = catalog_reader.read_guard();

    reader
        .get_database_by_name(&db_name)
        .map_err(|_| err(anyhow!("stream '{}' not found", name), StatusCode::NOT_FOUND))?;

    Ok(Json(GetStreamResponse { name }))
}
