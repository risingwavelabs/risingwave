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

use std::borrow::Cow;
use std::sync::{Arc, OnceLock};

use anyhow::anyhow;
use jni::objects::{JByteArray, JObject, JString};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::{DATA_DIRECTORY, STATE_STORE_URL};
use risingwave_object_store::object::object_metrics::GLOBAL_OBJECT_STORE_METRICS;
use risingwave_object_store::object::{ObjectStoreImpl, build_remote_object_store};

use crate::{EnvParam, JAVA_BINDING_ASYNC_RUNTIME, execute_and_catch, to_guarded_slice};

static OBJECT_STORE_INSTANCE: OnceLock<Arc<ObjectStoreImpl>> = OnceLock::new();

/// Security safeguard: Check if file has .dat extension
/// This prevents accidental overwriting of Hummock data or other critical files.
fn validate_dat_file_extension(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("File path cannot be empty".to_owned());
    }

    if !path.ends_with(".dat") {
        return Err(format!(
            "Security violation: Only .dat files are allowed for schema history operations, got: {}",
            path
        ));
    }

    Ok(())
}

// schema history is internal state, all data is stored under the DATA_DIRECTORY directory.
fn prepend_data_directory(path: &str) -> String {
    let data_dir = DATA_DIRECTORY.get().map(|s| s.as_str()).expect(
        "DATA_DIRECTORY is not set. This is dangerous in cloud environments as it can cause data conflicts between multiple instances sharing the same bucket. Please ensure data_directory is properly configured."
    );

    if data_dir.ends_with('/') || path.starts_with('/') {
        format!("{}{}", data_dir, path)
    } else {
        format!("{}/{}", data_dir, path)
    }
}

async fn get_object_store() -> Arc<ObjectStoreImpl> {
    if let Some(store) = OBJECT_STORE_INSTANCE.get() {
        store.clone()
    } else {
        let hummock_url = STATE_STORE_URL.get().unwrap();
        let object_store = build_remote_object_store(
            hummock_url.strip_prefix("hummock+").unwrap_or("memory"),
            Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone()),
            "rw-cdc-schema-history",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await;
        let arc = Arc::new(object_store);
        let _ = OBJECT_STORE_INSTANCE.set(arc.clone());
        arc
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_putObject(
    env: EnvParam<'_>,
    object_name: JString<'_>,
    data: JByteArray<'_>,
) {
    execute_and_catch(env, move |env| {
        let object_name = env.get_string(&object_name).map_err(|e| anyhow!(e))?;
        let object_name: Cow<'_, str> = (&object_name).into();
        // Security check: validate file extension before any operation
        validate_dat_file_extension(&object_name).map_err(|e| anyhow!(e))?;
        let object_name = prepend_data_directory(&object_name);
        let data_guard = to_guarded_slice(&data, env).map_err(|e| anyhow!(e))?;
        let data: Vec<u8> = data_guard.slice.to_vec();
        JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async {
                let object_store = get_object_store().await;
                object_store.upload(&object_name, data.into()).await
            })
            .map_err(|e| anyhow!(e))?;
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_getObject<'a>(
    env: EnvParam<'a>,
    object_name: JString<'a>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();

        // Security check: validate file extension before any operation
        validate_dat_file_extension(&object_name).map_err(|e| anyhow!(e))?;
        let object_name = prepend_data_directory(&object_name);
        let result = JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async {
                let object_store = get_object_store().await;
                object_store.read(&object_name, ..).await
            })
            .map_err(|e| anyhow!(e))?;

        Ok(env.byte_array_from_slice(&result)?)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_getObjectStoreType<'a>(
    env: EnvParam<'a>,
) -> JString<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let media_type = JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            object_store.media_type().to_owned()
        });
        Ok(env.new_string(media_type)?)
    })
}

fn strip_data_directory_prefix(path: &str) -> Result<String, String> {
    let data_directory = DATA_DIRECTORY
        .get()
        .map(|s| s.as_str())
        .ok_or_else(|| "expect DATA_DIRECTORY")?;
    let relative_path = path.strip_prefix(data_directory).ok_or_else(|| {
        format!(
            "DATA_DIRECTORY {} is not prefix of path {}",
            data_directory, path
        )
    })?;
    let stripped = if let Some(stripped) = relative_path.strip_prefix('/') {
        stripped.to_owned()
    } else {
        relative_path.to_owned()
    };
    Ok(stripped)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_listObject<'a>(
    env: EnvParam<'a>,
    dir: JString<'a>,
) -> jni::sys::jobjectArray {
    **execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let dir = env.get_string(&dir).map_err(|e| anyhow!(e))?;
        let dir: Cow<'_, str> = (&dir).into();

        // Note: listObject operates on directories, individual file security is checked in putObject/getObject

        let dir = prepend_data_directory(&dir);

        let files: Vec<String> = JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            let mut prefix_stripped_paths = Vec::new();
            let mut stream = object_store
                .list(&dir, None, None)
                .await
                .map_err(|e| anyhow!(e))?;
            use futures::StreamExt;
            while let Some(obj) = stream.next().await {
                let obj = obj.map_err(|e| anyhow!(e))?;
                // Additional security: only return files that pass validation
                // Remove the data directory prefix for validation
                let relative_path =
                    strip_data_directory_prefix(&obj.key).map_err(|e| anyhow!(e))?;
                if validate_dat_file_extension(&relative_path).is_ok() {
                    prefix_stripped_paths.push(relative_path);
                } else {
                    tracing::warn!("Filtering out non-.dat file from list: {}", obj.key);
                }
            }
            Ok::<_, anyhow::Error>(prefix_stripped_paths)
        })?;

        let string_class = env.find_class("java/lang/String").map_err(|e| anyhow!(e))?;
        let array = env
            .new_object_array(files.len() as i32, string_class, JObject::null())
            .map_err(|e| anyhow!(e))?;
        for (i, file) in files.iter().enumerate() {
            let jstr = env.new_string(file).map_err(|e| anyhow!(e))?;
            env.set_object_array_element(&array, i as i32, &jstr)
                .map_err(|e| anyhow!(e))?;
        }
        Ok(array)
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_deleteObjects<'a>(
    env: EnvParam<'a>,
    dir: JString<'a>,
) {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let dir = env.get_string(&dir).map_err(|e| anyhow!(e))?;
        let dir: Cow<'_, str> = (&dir).into();

        // Note: deleteObjects operates on directories, individual file security is checked in putObject/getObject

        let dir = prepend_data_directory(&dir);

        JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            let mut keys = Vec::new();
            let mut stream = object_store
                .list(&dir, None, None)
                .await
                .map_err(|e| anyhow!(e))?;
            use futures::StreamExt;
            while let Some(obj) = stream.next().await {
                let obj = obj.map_err(|e| anyhow!(e))?;
                // Additional security: only delete files that pass validation
                // Remove the data directory prefix for validation
                let relative_path =
                    strip_data_directory_prefix(&obj.key).map_err(|e| anyhow!(e))?;
                if validate_dat_file_extension(&relative_path).is_ok() {
                    keys.push(obj.key);
                } else {
                    tracing::warn!("Skipping deletion of non-.dat file: {}", obj.key);
                }
            }
            for key in keys {
                tracing::debug!("Deleting schema history file: {}", key);
                object_store.delete(&key).await.map_err(|e| anyhow!(e))?;
            }
            Ok::<_, anyhow::Error>(())
        })?;
        Ok(())
    });
}
