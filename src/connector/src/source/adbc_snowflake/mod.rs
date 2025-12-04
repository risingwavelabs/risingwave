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

use std::collections::HashMap;

use adbc_core::{Connection as _, Database as _, Statement as _};
use adbc_snowflake::database::Builder as DatabaseBuilder;
use adbc_snowflake::{Connection, Database, Driver, Statement};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::arrow::{Arrow55FromArrow, arrow_array_55};
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceEnumeratorContextRef, SourceProperties,
    SplitEnumerator, SplitId, SplitMetaData, SplitReader, UnknownFields,
};

pub const ADBC_SNOWFLAKE_CONNECTOR: &str = "adbc_snowflake";

/// Converter for Arrow 55 record batches to RisingWave `DataChunks`.
/// This is similar to `IcebergArrowConvert` but uses Arrow 55 (used by `adbc_snowflake`).
#[derive(Default)]
pub struct AdbcSnowflakeArrowConvert;

impl Arrow55FromArrow for AdbcSnowflakeArrowConvert {}

impl AdbcSnowflakeArrowConvert {
    pub fn chunk_from_record_batch(
        &self,
        batch: &arrow_array_55::RecordBatch,
    ) -> Result<risingwave_common::array::DataChunk, risingwave_common::array::ArrayError> {
        Arrow55FromArrow::from_record_batch(self, batch)
    }
}

/// Properties for ADBC Snowflake source connector.
#[derive(Clone, Debug, Deserialize, with_options::WithOptions)]
pub struct AdbcSnowflakeProperties {
    /// The Snowflake account identifier (e.g., "myaccount" or "myaccount.us-east-1").
    #[serde(rename = "adbc_snowflake.account")]
    pub account: String,

    /// The username for authentication.
    #[serde(rename = "adbc_snowflake.username")]
    pub username: String,

    /// The password for authentication.
    #[serde(rename = "adbc_snowflake.password")]
    pub password: String,

    /// The name of the database to use.
    #[serde(rename = "adbc_snowflake.database")]
    pub database: String,

    /// The name of the schema to use.
    #[serde(rename = "adbc_snowflake.schema")]
    pub schema: String,

    /// The name of the warehouse to use.
    #[serde(rename = "adbc_snowflake.warehouse")]
    pub warehouse: String,

    /// The SQL query to execute.
    #[serde(rename = "adbc_snowflake.query")]
    pub query: String,

    /// The role to use (optional).
    #[serde(rename = "adbc_snowflake.role")]
    pub role: Option<String>,

    /// The host to connect to (optional, defaults to Snowflake cloud).
    #[serde(rename = "adbc_snowflake.host")]
    pub host: Option<String>,

    /// The port to connect to (optional).
    #[serde(rename = "adbc_snowflake.port")]
    pub port: Option<u16>,

    /// The protocol to use (optional, defaults to "https").
    #[serde(rename = "adbc_snowflake.protocol")]
    pub protocol: Option<String>,

    /// The authentication type (optional, defaults to "`auth_snowflake`").
    /// Possible values: `auth_snowflake`, `auth_oauth`, `auth_ext_browser`, `auth_okta`, `auth_jwt`, `auth_mfa`, `auth_pat`, `auth_wif`
    #[serde(rename = "adbc_snowflake.auth_type")]
    pub auth_type: Option<String>,

    /// `OAuth` token for authentication (when using `auth_oauth`).
    #[serde(rename = "adbc_snowflake.auth_token")]
    pub auth_token: Option<String>,

    /// JWT private key file path (when using `auth_jwt`).
    #[serde(rename = "adbc_snowflake.jwt_private_key")]
    pub jwt_private_key: Option<String>,

    /// Unknown fields for forward compatibility.
    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl crate::enforce_secret::EnforceSecret for AdbcSnowflakeProperties {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "adbc_snowflake.password",
        "adbc_snowflake.auth_token",
        "adbc_snowflake.jwt_private_key",
    };
}

impl UnknownFields for AdbcSnowflakeProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SourceProperties for AdbcSnowflakeProperties {
    type Split = AdbcSnowflakeSplit;
    type SplitEnumerator = AdbcSnowflakeSplitEnumerator;
    type SplitReader = AdbcSnowflakeSplitReader;

    const SOURCE_NAME: &'static str = ADBC_SNOWFLAKE_CONNECTOR;
}

impl AdbcSnowflakeProperties {
    /// Build a database builder from properties.
    fn build_database_builder(&self) -> ConnectorResult<DatabaseBuilder> {
        let mut builder = DatabaseBuilder::default()
            .with_account(&self.account)
            .with_username(&self.username)
            .with_password(&self.password)
            .with_database(&self.database)
            .with_schema(&self.schema)
            .with_warehouse(&self.warehouse);

        if let Some(ref role) = self.role {
            builder = builder.with_role(role);
        }

        if let Some(ref host) = self.host {
            builder = builder
                .with_parse_host(host)
                .map_err(|e| anyhow!("Failed to parse host: {}", e))?;
        }

        if let Some(port) = self.port {
            builder = builder.with_port(port);
        }

        if let Some(ref protocol) = self.protocol {
            builder = builder
                .with_parse_protocol(protocol)
                .map_err(|e| anyhow!("Failed to parse protocol: {}", e))?;
        }

        if let Some(ref auth_type) = self.auth_type {
            builder = builder
                .with_parse_auth_type(auth_type)
                .map_err(|e| anyhow!("Failed to parse auth type: {}", e))?;
        }

        if let Some(ref auth_token) = self.auth_token {
            builder = builder.with_auth_token(auth_token);
        }

        if let Some(ref jwt_private_key) = self.jwt_private_key {
            builder = builder.with_jwt_private_key(jwt_private_key.into());
        }

        Ok(builder)
    }

    /// Create an ADBC Snowflake database connection.
    pub fn create_database(&self) -> ConnectorResult<Database> {
        let mut driver = Driver::try_load()
            .map_err(|e| anyhow!("Failed to load ADBC Snowflake driver: {}", e))?;
        let builder = self.build_database_builder()?;
        let database = builder
            .build(&mut driver)
            .map_err(|e| anyhow!("Failed to create database: {}", e))?;
        Ok(database)
    }

    /// Create a connection from the database.
    pub fn create_connection(&self, database: &Database) -> ConnectorResult<Connection> {
        let connection = database
            .new_connection()
            .map_err(|e| anyhow!("Failed to create connection: {}", e))?;
        Ok(connection)
    }

    /// Create a statement from the connection and set the SQL query.
    pub fn create_statement(&self, connection: &mut Connection) -> ConnectorResult<Statement> {
        let mut statement = connection
            .new_statement()
            .map_err(|e| anyhow!("Failed to create statement: {}", e))?;
        statement
            .set_sql_query(&self.query)
            .map_err(|e| anyhow!("Failed to set SQL query: {}", e))?;
        Ok(statement)
    }

    /// Execute a custom query and return the results as a vector of Arrow record batches.
    /// This is useful for metadata queries needed for split generation.
    pub fn execute_query(&self, query: &str) -> ConnectorResult<Vec<arrow_array_55::RecordBatch>> {
        let database = self.create_database()?;
        let mut connection = self.create_connection(&database)?;
        let mut statement = connection
            .new_statement()
            .map_err(|e| anyhow!("Failed to create statement: {}", e))?;
        statement
            .set_sql_query(query)
            .map_err(|e| anyhow!("Failed to set SQL query: {}", e))?;
        let reader = statement
            .execute()
            .map_err(|e| anyhow!("Failed to execute query: {}", e))?;

        // Collect all batches into a vector
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| anyhow!("Failed to read record batch: {}", e))?;
            batches.push(batch);
        }
        Ok(batches)
    }
}

/// Split for ADBC Snowflake source.
/// Since Snowflake queries are executed as a whole, we use a single split with the query as the identifier.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub struct AdbcSnowflakeSplit {
    /// The split identifier (typically based on the query).
    pub split_id: String,
    /// The SQL query to execute.
    pub query: String,
}

impl SplitMetaData for AdbcSnowflakeSplit {
    fn id(&self) -> SplitId {
        self.split_id.clone().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e).into())
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // ADBC Snowflake doesn't have offset-based reading for now
        Ok(())
    }
}

/// Split enumerator for ADBC Snowflake source.
pub struct AdbcSnowflakeSplitEnumerator {
    properties: AdbcSnowflakeProperties,
}

#[async_trait]
impl SplitEnumerator for AdbcSnowflakeSplitEnumerator {
    type Properties = AdbcSnowflakeProperties;
    type Split = AdbcSnowflakeSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Ok(Self { properties })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<Self::Split>> {
        // Validate connection and access by establishing a connection and preparing the query.
        // This ensures credentials are correct and the query is valid before returning the split.
        let database = self.properties.create_database()?;
        let mut connection = self.properties.create_connection(&database)?;

        // Validate connection and access by running a simple query against the target database/schema
        let validation_query = format!(
            "SELECT * FROM {}.information_schema.tables WHERE table_schema = '{}' LIMIT 1",
            self.properties.database, self.properties.schema
        );
        let mut statement = connection
            .new_statement()
            .map_err(|e| anyhow!("Failed to create statement: {}", e))?;
        statement
            .set_sql_query(&validation_query)
            .map_err(|e| anyhow!("Failed to set SQL query: {}", e))?;
        let _ = statement.execute().map_err(|e| {
            anyhow!(
                "Failed to validate connection - check credentials and permissions: {}",
                e
            )
        })?;

        // Connection and query are valid, return the split
        let split = AdbcSnowflakeSplit {
            split_id: "0".to_owned(),
            query: self.properties.query.clone(),
        };
        Ok(vec![split])
    }
}

/// Split reader for ADBC Snowflake source.
pub struct AdbcSnowflakeSplitReader {
    properties: AdbcSnowflakeProperties,
    #[allow(dead_code)]
    splits: Vec<AdbcSnowflakeSplit>,
    #[allow(dead_code)]
    parser_config: ParserConfig,
    #[allow(dead_code)]
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for AdbcSnowflakeSplitReader {
    type Properties = AdbcSnowflakeProperties;
    type Split = AdbcSnowflakeSplit;

    async fn new(
        properties: Self::Properties,
        splits: Vec<Self::Split>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            properties,
            splits,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_chunk_stream().boxed()
    }
}

impl AdbcSnowflakeSplitReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_chunk_stream(self) {
        // Execute the query and read the results as Arrow record batches
        let database = self.properties.create_database()?;
        let mut connection = self.properties.create_connection(&database)?;
        let mut statement = self.properties.create_statement(&mut connection)?;

        // Execute the query and get a record batch reader
        let reader = statement
            .execute()
            .map_err(|e| anyhow!("Failed to execute query: {}", e))?;

        let converter = AdbcSnowflakeArrowConvert;

        // Iterate over the record batches and convert them to StreamChunks
        for batch_result in reader {
            let batch = batch_result.map_err(|e| anyhow!("Failed to read record batch: {}", e))?;

            // Convert Arrow RecordBatch to RisingWave DataChunk using the converter
            let data_chunk = converter.chunk_from_record_batch(&batch)?;

            // Convert DataChunk to StreamChunk (all inserts)
            let stream_chunk = StreamChunk::from_parts(
                vec![risingwave_common::array::Op::Insert; data_chunk.capacity()],
                data_chunk,
            );

            yield stream_chunk;
        }
    }
}
