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

use std::fmt;

use anyhow::Context;
use jni::objects::JObject;
use risingwave_common::global_jvm::Jvm;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::execute_with_jni_env;

use crate::sink::Result;

pub struct JdbcJniClient {
    jvm: Jvm,
    jdbc_url: String,
    driver_props: Option<Vec<(String, String)>>,
}

impl Clone for JdbcJniClient {
    fn clone(&self) -> Self {
        Self {
            jvm: self.jvm,
            jdbc_url: self.jdbc_url.clone(),
            driver_props: self.driver_props.clone(),
        }
    }
}

impl fmt::Debug for JdbcJniClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JdbcJniClient")
            .field("jdbc_url", &self.jdbc_url)
            .field("driver_props", &"<redacted>")
            .finish()
    }
}

impl JdbcJniClient {
    pub fn new(jdbc_url: String) -> Result<Self> {
        let jvm = Jvm::get_or_init()?;
        Ok(Self {
            jvm,
            jdbc_url,
            driver_props: None,
        })
    }

    pub fn new_with_props(jdbc_url: String, driver_props: Vec<(String, String)>) -> Result<Self> {
        let jvm = Jvm::get_or_init()?;
        Ok(Self {
            jvm,
            jdbc_url,
            driver_props: Some(driver_props),
        })
    }

    pub async fn execute_sql_sync(&self, sql: Vec<String>) -> anyhow::Result<()> {
        self.execute_sql_sync_with_props(sql).await
    }

    pub async fn execute_sql_sync_with_props(&self, sql: Vec<String>) -> anyhow::Result<()> {
        let jvm = self.jvm;
        let jdbc_url = self.jdbc_url.clone();
        let driver_props = self.driver_props.clone().unwrap_or_default();
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            execute_with_jni_env(jvm, |env| {
                let j_url = env.new_string(&jdbc_url).with_context(|| {
                    format!("Failed to create jni string from jdbc url: {}.", jdbc_url)
                })?;

                // SQL array
                let sql_arr =
                    env.new_object_array(sql.len() as i32, "java/lang/String", JObject::null())?;
                for (i, s) in sql.iter().enumerate() {
                    let s_j = env.new_string(s)?;
                    env.set_object_array_element(&sql_arr, i as i32, s_j)?;
                }

                // Driver properties as separate key and value arrays
                let keys_arr = env.new_object_array(
                    driver_props.len() as i32,
                    "java/lang/String",
                    JObject::null(),
                )?;
                let values_arr = env.new_object_array(
                    driver_props.len() as i32,
                    "java/lang/String",
                    JObject::null(),
                )?;
                for (i, (k, v)) in driver_props.iter().enumerate() {
                    let k_j = env.new_string(k)?;
                    let v_j = env.new_string(v)?;
                    env.set_object_array_element(&keys_arr, i as i32, k_j)?;
                    env.set_object_array_element(&values_arr, i as i32, v_j)?;
                }

                call_static_method!(
                    env,
                    { com.risingwave.runner.JDBCSqlRunner },
                    { void executeSqlWithProps(String, String[], String[], String[]) },
                    &j_url,
                    &sql_arr,
                    &keys_arr,
                    &values_arr
                )?;
                Ok(())
            })?;
            Ok(())
        })
        .await
        .context("Failed to execute SQL via JDBC JNI client with properties")?
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
