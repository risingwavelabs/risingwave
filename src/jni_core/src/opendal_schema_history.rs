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

use bytes::Bytes;
use jni::objects::{JByteArray, JObject, JString};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::{DATA_DIRECTORY, STATE_STORE_URL};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
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
    let data_dir = DATA_DIRECTORY.get().map(|s| s.as_str()).unwrap_or("");
    if data_dir.is_empty() {
        // In cloud environments, using empty data directory can cause data conflicts
        // between different instances sharing the same bucket. We should ensure
        // data_directory is properly set to avoid data overwrites.
        panic!(
            "DATA_DIRECTORY is not set. This is dangerous in cloud environments as it can cause data conflicts between multiple instances sharing the same bucket. Please ensure data_directory is properly configured."
        );
    }
    if path.starts_with(data_dir) {
        path.to_owned()
    } else if data_dir.ends_with('/') || path.starts_with('/') {
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
            Arc::new(ObjectStoreMetrics::unused()),
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
        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();

        // Security check: validate file extension before any operation
        if let Err(error_msg) = validate_dat_file_extension(&object_name) {
            tracing::error!(
                "putObject security validation failed, skipping operation: {}",
                error_msg
            );
            return Ok(()); // Skip this operation
        }

        let object_name = prepend_data_directory(&object_name);

        let data_guard = to_guarded_slice(&data, env)?;
        let data: Vec<u8> = data_guard.slice.to_vec();
        JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async {
                let object_store = get_object_store().await;
                object_store.upload(&object_name, data.into()).await
            })
            .unwrap();

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
        if let Err(error_msg) = validate_dat_file_extension(&object_name) {
            tracing::error!(
                "getObject security validation failed, returning empty result: {}",
                error_msg
            );
            return Ok(env.byte_array_from_slice(&[])?); // Return empty byte array
        }

        let object_name = prepend_data_directory(&object_name);
        let result = JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            match object_store.read(&object_name, ..).await {
                Ok(data) => data,
                Err(_) => Bytes::new(),
            }
        });

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

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_listObject<'a>(
    env: EnvParam<'a>,
    dir: JString<'a>,
) -> jni::sys::jobjectArray {
    **execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let dir = env.get_string(&dir)?;
        let dir: Cow<'_, str> = (&dir).into();

        // Note: listObject operates on directories, individual file security is checked in putObject/getObject

        let dir = prepend_data_directory(&dir);

        let files: Vec<String> = JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            let mut file_names = Vec::new();
            let mut stream = match object_store.list(&dir, None, None).await {
                Ok(s) => s,
                Err(_) => return file_names,
            };
            use futures::StreamExt;
            while let Some(obj) = stream.next().await {
                match obj {
                    Ok(obj) => {
                        // Additional security: only return files that pass validation
                        // Remove the data directory prefix for validation
                        let relative_path = obj
                            .key
                            .strip_prefix(DATA_DIRECTORY.get().map(|s| s.as_str()).unwrap_or(""))
                            .unwrap_or(&obj.key);
                        let relative_path = if let Some(stripped) = relative_path.strip_prefix('/')
                        {
                            stripped
                        } else {
                            relative_path
                        };

                        if validate_dat_file_extension(relative_path).is_ok() {
                            file_names.push(obj.key);
                        } else {
                            tracing::error!("Filtering out non-.dat file from list: {}", obj.key);
                        }
                    }
                    Err(_) => continue,
                }
            }
            file_names
        });

        let string_class = env.find_class("java/lang/String")?;
        let array = env.new_object_array(files.len() as i32, string_class, JObject::null())?;
        for (i, file) in files.iter().enumerate() {
            let jstr = env.new_string(file)?;
            env.set_object_array_element(&array, i as i32, &jstr)?;
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
        let dir = env.get_string(&dir)?;
        let dir: Cow<'_, str> = (&dir).into();

        // Note: deleteObjects operates on directories, individual file security is checked in putObject/getObject

        let dir = prepend_data_directory(&dir);

        JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let object_store = get_object_store().await;
            let mut keys = Vec::new();
            let mut stream = match object_store.list(&dir, None, None).await {
                Ok(s) => s,
                Err(_) => return,
            };
            use futures::StreamExt;
            while let Some(obj) = stream.next().await {
                if let Ok(obj) = obj {
                    // Additional security: only delete files that pass validation
                    // Remove the data directory prefix for validation
                    let relative_path = obj
                        .key
                        .strip_prefix(DATA_DIRECTORY.get().map(|s| s.as_str()).unwrap_or(""))
                        .unwrap_or(&obj.key);
                    let relative_path = if let Some(stripped) = relative_path.strip_prefix('/') {
                        stripped
                    } else {
                        relative_path
                    };

                    if validate_dat_file_extension(relative_path).is_ok() {
                        keys.push(obj.key);
                    } else {
                        tracing::error!("Skipping deletion of non-.dat file: {}", obj.key);
                    }
                }
            }
            for key in keys {
                tracing::debug!("Deleting schema history file: {}", key);
                let _ = object_store.delete(&key).await;
            }
        });
        Ok(())
    });
}
