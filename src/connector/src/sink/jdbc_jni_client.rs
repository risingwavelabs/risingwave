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

#[derive(Debug)]
pub struct JdbcJniClient {
    jvm: &'static JavaVM,
    jdbc_url: String,
}

impl JdbcJniClient {
    pub fn new(jdbc_url: String) -> Result<Self> {
        let jvm = JVM.get_or_init()?;
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

pub fn build_alter_add_column_sql(
    full_table_name: &str,
    columns: &Vec<(String, String)>,
) -> String {
    let column_definitions: Vec<String> = columns
        .iter()
        .map(|(name, typ)| format!(r#""{}" {}"#, name, typ))
        .collect();
    let column_definitions_str = column_definitions.join(", ");
    format!(
        "ALTER TABLE {} ADD COLUMN {} ",
        full_table_name, column_definitions_str
    )
}

pub fn normalize_sql(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
