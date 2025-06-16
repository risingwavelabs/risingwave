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

use std::{borrow::Cow, env};
use std::sync::Arc;

use bytes::Bytes;
use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint};
use opendal::layers::LoggingLayer;
use opendal::services::{Fs, S3};
use opendal::{Builder, Operator};
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::{build_remote_object_store, object_metrics::ObjectStoreMetrics, ObjectStoreImpl};
use tracing::Level;

use crate::{EnvParam, JAVA_BINDING_ASYNC_RUNTIME, execute_and_catch, to_guarded_slice};

struct ObjectStoreEngine {
    op: Operator,
}

pub struct HummockObjectStore {
    object_store: Arc<ObjectStoreImpl>,
}
impl HummockObjectStore {
    
    pub fn new(object_store: Arc<ObjectStoreImpl>) -> Self {

        Self { object_store }
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> anyhow::Result<()> {
        self.object_store.upload(path, obj).await?;
        Ok(())
    }
    
}

pub async fn new_object_store() -> ObjectStoreImpl {
        let hummock_url = "hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001";
        let object_store = build_remote_object_store(
            hummock_url.strip_prefix("hummock+").unwrap(),
            Arc::new(ObjectStoreMetrics::unused()),
            "Hummock",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await;
        object_store 
    }
impl ObjectStoreEngine {
    pub fn new_minio_engine(server: &str) -> anyhow::Result<Self> {
        let builder = Fs::default().root("/Users/wangcongyi/singularity/risingwave/");
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        Ok(Self { op })
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> anyhow::Result<()> {
        self.op.write(path, obj).await?;
        Ok(())
    }

    pub async fn read_object(&self, path: &str) -> anyhow::Result<Bytes> {
        let data = self.op.read_with(path).await?;
        Ok(data.to_bytes())
    }

}

// fn get_engine() -> anyhow::Result<ObjectStoreEngine> {
//     let bucket = "hummock001".to_string();

//     let engine = ObjectStoreEngine::new_minio_engine(&format!("minio://{}", bucket))?;

//     Ok(engine)
// }



#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_putObject(
    env: EnvParam<'_>,
    object_name: JString<'_>,
    data: JByteArray<'_>,
) {
    execute_and_catch(env, move |env| {
        // let engine = get_engine()?;

        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();

        let data_guard = to_guarded_slice(&data, env)?;
        let data: Vec<u8> = data_guard.slice.to_vec();
        let result = JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async {
                let object_store = new_object_store().await;
                // engine
                //     .op
                //     .write_with(&object_name, data)
                //     .content_type("text/plain")
                //     .await

                object_store.upload(&object_name, data.clone().into())
                    .await
            })
            .unwrap();

        Ok(result)
    });
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_getObject<'a>(
    env: EnvParam<'a>,
    object_name: JString<'a>,
) -> JByteArray<'a> {
    execute_and_catch(env, move |env: &mut EnvParam<'_>| {
        // let engine = get_engine()?;
        
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
