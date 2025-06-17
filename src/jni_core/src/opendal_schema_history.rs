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
use std::sync::Arc;

use bytes::Bytes;
use jni::objects::{JByteArray, JString};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{ObjectStoreImpl, build_remote_object_store};

use crate::{EnvParam, JAVA_BINDING_ASYNC_RUNTIME, execute_and_catch, to_guarded_slice};

pub async fn new_object_store() -> ObjectStoreImpl {
    let hummock_url = "hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001";
    let object_store = build_remote_object_store(
        hummock_url.strip_prefix("hummock+").unwrap(),
        Arc::new(ObjectStoreMetrics::unused()),
        "mysql-cdc-schema-history",
        Arc::new(ObjectStoreConfig::default()),
    )
    .await;
    object_store
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
                let object_store = new_object_store().await;

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
            let object_store = new_object_store().await;
            match object_store.read(&object_name, ..).await {
                Ok(data) => data,
                Err(_) => Bytes::new(),
            }
        });

        Ok(env.byte_array_from_slice(&result)?)
    })
}
