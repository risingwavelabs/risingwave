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
use jni::objects::JObject;
use risingwave_common::global_jvm::Jvm;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;

use crate::sink::Result;

#[derive(Debug)]
pub struct JdbcJniClient {
    jvm: Jvm,
    jdbc_url: String,
}

impl Clone for JdbcJniClient {
    fn clone(&self) -> Self {
        Self {
            jvm: self.jvm,
            jdbc_url: self.jdbc_url.clone(),
        }
    }
}

impl JdbcJniClient {
    pub fn new(jdbc_url: String) -> Result<Self> {
        let jvm = Jvm::get_or_init()?;
        Ok(Self { jvm, jdbc_url })
    }

    pub async fn execute_sql_sync(&self, sql: Vec<String>) -> anyhow::Result<()> {
        let jvm = self.jvm;
        let jdbc_url = self.jdbc_url.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            execute_with_jni_env(jvm, |env| {
                // get source handler by source id
                let full_url = env.new_string(&jdbc_url).with_context(|| {
                    format!(
                        "Failed to create jni string from source offset: {}.",
                        jdbc_url
                    )
                })?;

                let props =
                    env.new_object_array((sql.len()) as i32, "java/lang/String", JObject::null())?;

                for (i, sql) in sql.iter().enumerate() {
                    let sql_j_str = env.new_string(sql)?;
                    env.set_object_array_element(&props, i as i32, sql_j_str)?;
                }

                call_static_method!(
                    env,
                    { com.risingwave.runner.JDBCSqlRunner },
                    { void executeSql(String, String[]) },
                    &full_url,
                    &props
                )?;
                Ok(())
            })?;
            Ok(())
        })
        .await
        .context("Failed to execute SQL via JDBC JNI client")?
    }
}

pub fn build_alter_add_column_sql(
    full_table_name: &str,
    columns: &Vec<(String, String)>,
    if_not_exists: bool,
) -> String {
    let column_definitions: Vec<String> = columns
        .iter()
        .map(|(name, typ)| format!(r#""{}" {}"#, name, typ))
        .collect();
    let column_definitions_str = column_definitions.join(", ");
    let if_not_exists_str = if if_not_exists { "IF NOT EXISTS " } else { "" };
    format!(
        "ALTER TABLE {} ADD COLUMN {}{} ",
        full_table_name, if_not_exists_str, column_definitions_str
    )
}

pub fn normalize_sql(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
