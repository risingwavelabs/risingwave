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
use thiserror_ext::AsReport;

use crate::sink::Result;
use crate::sink::snowflake::{SNOWFLAKE_SINK_OP, SNOWFLAKE_SINK_ROW_ID, SnowflakeTaskContext};

pub struct SnowflakeJniClient {
    jdbc_client: JdbcJniClient,
    snowflake_task_context: SnowflakeTaskContext,
}

impl SnowflakeJniClient {
    pub fn new(jdbc_client: JdbcJniClient, snowflake_task_context: SnowflakeTaskContext) -> Self {
        Self {
            jdbc_client,
            snowflake_task_context,
        }
    }

    pub fn execute_alter_add_columns(&mut self, columns: &Vec<(String, String)>) -> Result<()> {
        self.execute_drop_task()?;
        if let Some(cdc_table_name) = &self.snowflake_task_context.cdc_table_name {
            let alter_add_column_cdc_table_sql = build_alter_add_column_sql(
                cdc_table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema,
                &columns,
            );
            self.jdbc_client
                .execute_sql_sync(&alter_add_column_cdc_table_sql)?;
        }

        let alter_add_column_target_table_sql = build_alter_add_column_sql(
            &self.snowflake_task_context.target_table_name,
            &self.snowflake_task_context.database,
            &self.snowflake_task_context.schema,
            &columns,
        );
        self.jdbc_client
            .execute_sql_sync(&alter_add_column_target_table_sql)?;

        self.execute_create_merge_into_task()?;
        Ok(())
    }

    pub fn execute_create_merge_into_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let create_task_sql = build_create_merge_into_task_sql(&self.snowflake_task_context);
            let start_task_sql = build_start_task_sql(&self.snowflake_task_context);
            self.jdbc_client.execute_sql_sync(&create_task_sql)?;
            self.jdbc_client.execute_sql_sync(&start_task_sql)?;
        }
        Ok(())
    }

    pub fn execute_drop_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let sql = build_drop_task_sql(&self.snowflake_task_context);
            if let Err(e) = self.jdbc_client.execute_sql_sync(&sql) {
                tracing::error!(
                    "Failed to drop Snowflake sink task {:?}: {:?}",
                    self.snowflake_task_context.task_name,
                    e.as_report()
                );
            } else {
                tracing::info!(
                    "Snowflake sink task {:?} dropped",
                    self.snowflake_task_context.task_name
                );
            }
        }
        Ok(())
    }
}

pub struct JdbcJniClient {
    jvm: &'static JavaVM,
    jdbc_url: String,
}

impl JdbcJniClient {
    pub fn new(jdbc_url: String, username: String, password: String) -> Result<Self> {
        let jvm = JVM.get_or_init()?;
        let jdbc_url = format!("{}?user={}&password={}", jdbc_url, username, password);
        Ok(Self { jvm, jdbc_url })
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

fn build_alter_add_column_sql(
    table_name: &str,
    database: &str,
    schema: &str,
    columns: &Vec<(String, String)>,
) -> String {
    let full_task_name = format!("{}.{}.{}", database, schema, table_name);
    let column_definitions: Vec<String> = columns
        .iter()
        .map(|(name, typ)| format!("{} {}", name, typ))
        .collect();
    let column_definitions_str = column_definitions.join(", ");
    format!(
        "ALTER TABLE {} ADD COLUMN {} ",
        full_task_name, column_definitions_str
    )
}

fn build_start_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!("{}.{}.{}", database, schema, task_name.as_ref().unwrap());
    format!("ALTER TASK {} RESUME", full_task_name)
}

fn build_drop_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!("{}.{}.{}", database, schema, task_name.as_ref().unwrap());
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
    let full_task_name = format!("{}.{}.{}", database, schema, task_name.as_ref().unwrap());
    let full_cdc_table_name = format!(
        "{}.{}.{}",
        database,
        schema,
        cdc_table_name.as_ref().unwrap()
    );
    let full_target_table_name = format!("{}.{}.{}", database, schema, target_table_name);

    let pk_names_str = pk_column_names.as_ref().unwrap().join(", ");
    let pk_names_eq_str = pk_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names.as_ref().unwrap().join(", ");
    let all_column_names_insert_str = all_column_names
        .as_ref()
        .unwrap()
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
        warehouse = warehouse.as_ref().unwrap(),
        schedule = schedule.as_ref().unwrap(),
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
            task_name: Some("test_task".to_owned()),
            cdc_table_name: Some("test_cdc_table".to_owned()),
            target_table_name: "test_target_table".to_owned(),
            schedule: Some("1 HOUR".to_owned()),
            warehouse: Some("test_warehouse".to_owned()),
            pk_column_names: Some(vec!["v1".to_owned()]),
            all_column_names: Some(vec!["v1".to_owned(), "v2".to_owned()]),
            database: "test_db".to_owned(),
            schema: "test_schema".to_owned(),
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
            task_name: Some("test_task_multi_pk".to_owned()),
            cdc_table_name: Some("cdc_multi_pk".to_owned()),
            target_table_name: "target_multi_pk".to_owned(),
            schedule: Some("5 MINUTE".to_owned()),
            warehouse: Some("multi_pk_warehouse".to_owned()),
            pk_column_names: Some(vec!["id1".to_owned(), "id2".to_owned()]),
            all_column_names: Some(vec!["id1".to_owned(), "id2".to_owned(), "val".to_owned()]),
            database: "test_db".to_owned(),
            schema: "test_schema".to_owned(),
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
