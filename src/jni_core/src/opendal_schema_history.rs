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
use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jboolean, jbyteArray, jint};
use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{Builder, Operator};
use tracing::Level;

use crate::{EnvParam, JAVA_BINDING_ASYNC_RUNTIME, execute_and_catch};

struct ObjectStoreEngine {
    op: Operator,
}

impl ObjectStoreEngine {
    pub fn new_minio_engine(server: &str) -> anyhow::Result<Self> {
        let builder = S3::default()
            .bucket("hummock001")
            .region("us-east-1")
            .access_key_id("hummockadmin")
            .secret_access_key("hummockadmin")
            .endpoint("http://hummock001.127.0.0.1:9301")
            .disable_config_load();
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

    pub async fn delete_object(&self, path: &str) -> anyhow::Result<()> {
        self.op.delete(path).await?;
        Ok(())
    }

    pub async fn list(&self, path: &str) -> anyhow::Result<()> {
        self.op.list(path).await?;
        Ok(())
    }
}

fn get_engine() -> anyhow::Result<ObjectStoreEngine> {
    let bucket = "hummock001".to_string();

    let engine = ObjectStoreEngine::new_minio_engine(&format!("minio://{}", bucket))?;

    Ok(engine)
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_putObject(
    env: EnvParam<'_>,
    object_name: JString<'_>,
    data: JString<'_>,
) {
    execute_and_catch(env, move |env| {
        let engine = get_engine()?;

        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();
        println!("rust这里写");
        let data = env.get_string(&data)?;
        let data: Cow<'_, str> = (&data).into();
        let data: Vec<u8> = data.as_bytes().to_vec();
        let result = JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async { engine.op.write(&object_name, data).await })
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
        let engine = get_engine()?;

        let object_name = env.get_string(&object_name)?;
        let object_name: Cow<'_, str> = (&object_name).into();
        println!("rust这里读");
        let result = JAVA_BINDING_ASYNC_RUNTIME
            .block_on(async {
                match engine.read_object(&object_name).await {
                    Ok(data) => data,
                    Err(_) =>  Bytes::new(), 
                }
            });
        
        Ok(env.byte_array_from_slice(&result)?)
    })
}
