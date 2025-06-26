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
use risingwave_common::STATE_STORE_URL;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{ObjectStoreImpl, build_remote_object_store};

use crate::{EnvParam, JAVA_BINDING_ASYNC_RUNTIME, execute_and_catch, to_guarded_slice};

static OBJECT_STORE_INSTANCE: OnceLock<Arc<ObjectStoreImpl>> = OnceLock::new();

async fn get_object_store() -> Arc<ObjectStoreImpl> {
    if let Some(store) = OBJECT_STORE_INSTANCE.get() {
        store.clone()
    } else {
        let hummock_url = STATE_STORE_URL.get().unwrap();
        let object_store = build_remote_object_store(
            hummock_url.strip_prefix("hummock+").unwrap(),
            Arc::new(ObjectStoreMetrics::unused()),
            "mysql-cdc-schema-history",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await;
        let arc = Arc::new(object_store);
        let _ = OBJECT_STORE_INSTANCE.set(arc.clone());
        arc
    }
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_putObject(
    env: EnvParam<'_>,
    object_name: JString<'_>,
    data: JByteArray<'_>,
) {
    execute_and_catch(env, move |env| {
        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();

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

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_getObject<'a>(
    env: EnvParam<'a>,
    object_name: JString<'a>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();
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

#[no_mangle]
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

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_listObject<'a>(
    env: EnvParam<'a>,
    dir: JString<'a>,
) -> jni::sys::jobjectArray {
    **execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let dir = env.get_string(&dir)?;
        let dir: Cow<'_, str> = (&dir).into();

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
                    Ok(obj) => file_names.push(obj.key),
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

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_deleteObjects<'a>(
    env: EnvParam<'a>,
    dir: JString<'a>,
) {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        let dir = env.get_string(&dir)?;
        let dir: Cow<'_, str> = (&dir).into();

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
                    keys.push(obj.key);
                }
            }
            for key in keys {
                let _ = object_store.delete(&key).await;
            }
        });
        Ok(())
    });
}
