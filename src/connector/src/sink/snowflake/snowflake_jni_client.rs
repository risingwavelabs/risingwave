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

use anyhow::Context;
use jni::JavaVM;
use risingwave_common::global_jvm::JVM;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;

use crate::sink::Result;
use crate::sink::snowflake::{SNOWFLAKE_SINK_OP, SNOWFLAKE_SINK_ROW_ID};

pub struct SnowflakeTaskContext {
    pub task_name: String,
    pub cdc_table_name: String,
    pub target_table_name: String,
    pub schedule: String,
    pub warehouse: String,
    pub pk_column_names: Vec<String>,
    pub all_column_names: Vec<String>,
    pub database: String,
    pub schema: String,
}

pub struct SnowflakeJniClient {
    snowflake_task_context: SnowflakeTaskContext,
    jvm: &'static JavaVM,
    jdbc_url: String,
}

impl SnowflakeJniClient {
    pub fn new(
        snowflake_task_context: SnowflakeTaskContext,
        jdbc_url: String,
        username: String,
        password: String,
    ) -> Result<Self> {
        let jvm = JVM.get_or_init()?;
        let jdbc_url = format!("{}?user={}&password={}", jdbc_url, username, password);
        Ok(Self {
            snowflake_task_context,
            jvm,
            jdbc_url,
        })
    }

    pub fn execute_create_merge_into_task_sql(&self) -> Result<()> {
        let create_task_sql = build_create_merge_into_task_sql(&self.snowflake_task_context);
        let start_task_sql = build_start_task_sql(&self.snowflake_task_context);
        self.execute_sql_sync(&create_task_sql)?;
        self.execute_sql_sync(&start_task_sql)?;
        Ok(())
    }

    pub fn execute_drop_task_sql(&self) -> Result<()> {
        let sql = build_drop_task_sql(&self.snowflake_task_context);
        if let Err(e) = self.execute_sql_sync(&sql) {
            tracing::error!(
                "Failed to drop Snowflake sink task {}: {}",
                self.snowflake_task_context.task_name,
                e
            );
        } else {
            tracing::info!(
                "Snowflake sink task {} dropped",
                self.snowflake_task_context.task_name
            );
        }
        Ok(())
    }

    pub fn execute_sql_sync(&self, sql: &str) -> Result<()> {
        execute_with_jni_env(self.jvm, |env| {
            // get source handler by source id
            let full_url = env.new_string(&self.jdbc_url).with_context(|| {
                format!(
                    "Failed to create jni string from source offset: {}.",
                    self.jdbc_url
                )
            })?;
            let sql = env.new_string(normalize_sql(sql)).with_context(|| {
                format!("Failed to create jni string from source offset: {}.", sql)
            })?;
            call_static_method!(
                env,
                {com.risingwave.runner.SnowflakeJDBCRunner},
                {void executeSql(String, String)},
                &full_url,
                &sql
            )?;
            Ok(())
        })?;
        Ok(())
    }
}

fn build_start_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!("{}.{}.{}", database, schema, task_name);
    format!("ALTER TASK {} RESUME", full_task_name)
}

fn build_drop_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!("{}.{}.{}", database, schema, task_name);
    format!("DROP TASK IF EXISTS {}", full_task_name)
}

fn build_create_merge_into_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        cdc_table_name,
        target_table_name,
        schedule,
        warehouse,
        pk_column_names,
        all_column_names,
        database,
        schema,
    } = snowflake_task_context;
    let full_task_name = format!("{}.{}.{}", database, schema, task_name);
    let full_cdc_table_name = format!("{}.{}.{}", database, schema, cdc_table_name);
    let full_target_table_name = format!("{}.{}.{}", database, schema, target_table_name);

    let pk_names_str = pk_column_names.join(", ");
    let pk_names_eq_str = pk_column_names
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names.join(", ");
    let all_column_names_insert_str = all_column_names
        .iter()
        .map(|name| format!("source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");

    format!(
        r#"CREATE OR REPLACE TASK {task_name}
WAREHOUSE = {warehouse}
SCHEDULE = '{schedule}'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX({snowflake_sink_row_id}), '0') INTO :max_row_id
    FROM {cdc_table_name};

    MERGE INTO {target_table_name} AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_names_str} ORDER BY {snowflake_sink_row_id} DESC) AS dedupe_id
            FROM {cdc_table_name}
            WHERE {snowflake_sink_row_id} <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON {pk_names_eq_str}
    WHEN MATCHED AND source.{snowflake_sink_op} IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.{snowflake_sink_op} IN (1, 3) THEN UPDATE SET {all_column_names_set_str}
    WHEN NOT MATCHED AND source.{snowflake_sink_op} IN (1, 3) THEN INSERT ({all_column_names_str}) VALUES ({all_column_names_insert_str});

    DELETE FROM {cdc_table_name}
    WHERE {snowflake_sink_row_id} <= :max_row_id;
END;"#,
        task_name = full_task_name,
        warehouse = warehouse,
        schedule = schedule,
        cdc_table_name = full_cdc_table_name,
        target_table_name = full_target_table_name,
        pk_names_str = pk_names_str,
        pk_names_eq_str = pk_names_eq_str,
        all_column_names_set_str = all_column_names_set_str,
        all_column_names_str = all_column_names_str,
        all_column_names_insert_str = all_column_names_insert_str,
        snowflake_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
        snowflake_sink_op = SNOWFLAKE_SINK_OP,
    )
}

fn normalize_sql(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_sink_commit_coordinator() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: "test_task".to_string(),
            cdc_table_name: "test_cdc_table".to_string(),
            target_table_name: "test_target_table".to_string(),
            schedule: "1 HOUR".to_string(),
            warehouse: "test_warehouse".to_string(),
            pk_column_names: vec!["v1".to_string()],
            all_column_names: vec!["v1".to_string(), "v2".to_string()],
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK test_db.test_schema.test_task
WAREHOUSE = test_warehouse
SCHEDULE = '1 HOUR'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX(__row_id), '0') INTO :max_row_id
    FROM test_db.test_schema.test_cdc_table;

    MERGE INTO test_db.test_schema.test_target_table AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY v1 ORDER BY __row_id DESC) AS dedupe_id
            FROM test_db.test_schema.test_cdc_table
            WHERE __row_id <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target.v1 = source.v1
    WHEN MATCHED AND source.__op IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.__op IN (1, 3) THEN UPDATE SET target.v1 = source.v1, target.v2 = source.v2
    WHEN NOT MATCHED AND source.__op IN (1, 3) THEN INSERT (v1, v2) VALUES (source.v1, source.v2);

    DELETE FROM test_db.test_schema.test_cdc_table
    WHERE __row_id <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator_multi_pk() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: "test_task_multi_pk".to_string(),
            cdc_table_name: "cdc_multi_pk".to_string(),
            target_table_name: "target_multi_pk".to_string(),
            schedule: "5 MINUTE".to_string(),
            warehouse: "multi_pk_warehouse".to_string(),
            pk_column_names: vec!["id1".to_string(), "id2".to_string()],
            all_column_names: vec!["id1".to_string(), "id2".to_string(), "val".to_string()],
            database: "test_db".to_string(),
            schema: "test_schema".to_string(),
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK test_db.test_schema.test_task_multi_pk
WAREHOUSE = multi_pk_warehouse
SCHEDULE = '5 MINUTE'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX(__row_id), '0') INTO :max_row_id
    FROM test_db.test_schema.cdc_multi_pk;

    MERGE INTO test_db.test_schema.target_multi_pk AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY __row_id DESC) AS dedupe_id
            FROM test_db.test_schema.cdc_multi_pk
            WHERE __row_id <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target.id1 = source.id1 AND target.id2 = source.id2
    WHEN MATCHED AND source.__op IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.__op IN (1, 3) THEN UPDATE SET target.id1 = source.id1, target.id2 = source.id2, target.val = source.val
    WHEN NOT MATCHED AND source.__op IN (1, 3) THEN INSERT (id1, id2, val) VALUES (source.id1, source.id2, source.val);

    DELETE FROM test_db.test_schema.cdc_multi_pk
    WHERE __row_id <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }
}
