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

use anyhow::anyhow;
use risingwave_common::array::arrow::arrow_schema_55;

use super::AdbcSnowflakeProperties;
use crate::error::ConnectorResult;

impl AdbcSnowflakeProperties {
    /// Get the Arrow schema from the Snowflake table.
    /// This is used for schema inference when creating tables.
    /// The schema is obtained using ADBC's `get_table_schema` API.
    ///
    /// **Important**: The column order in the returned schema matches the column order
    /// in the Snowflake table. This ensures consistency between schema inference
    /// (used by frontend) and data fetching (used by executor).
    pub fn get_arrow_schema(&self) -> ConnectorResult<arrow_schema_55::Schema> {
        use adbc_core::Connection as _;

        let database = self.create_database()?;
        let connection = self.create_connection(&database)?;

        // Parse table name from query to call get_table_schema
        let table_name = self.extract_table_name_from_query()?;

        // Use ADBC's get_table_schema API to get the schema
        let schema = connection
            .get_table_schema(Some(&self.database), Some(&self.schema), &table_name)
            .map_err(|e| anyhow!("Failed to get table schema: {}", e))?;

        Ok(schema)
    }

    /// Extract table name from the query.
    /// Simplified parser for SELECT queries.
    fn extract_table_name_from_query(&self) -> ConnectorResult<String> {
        let query_lower = self.query.to_lowercase();
        let from_idx = query_lower
            .find(" from ")
            .ok_or_else(|| anyhow!("Could not find FROM clause in query"))?;

        let after_from = &self.query[from_idx + 6..].trim();

        // Find the table name (stop at whitespace, semicolon, or end of string)
        let table_part = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow!("Could not extract table name from query"))?
            .trim_end_matches(';');

        // If schema.table format, take just the table name
        let table_name = table_part.split('.').next_back().unwrap_or(table_part);

        Ok(table_name.to_uppercase())
    }
}
