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
use jni::{objects::{JObject, JString}, sys::jboolean};
use jni::sys::{jbyteArray, jint};
use tracing::Level;
use opendal::{layers::LoggingLayer, Builder, Operator};
use opendal::services::S3;

use crate::{execute_and_catch, EnvParam, JAVA_BINDING_ASYNC_RUNTIME};



struct ObjectStoreEngine {
    op: Operator,

}

impl ObjectStoreEngine {
    pub fn new_minio_engine(
        server: &str,
    ) -> anyhow::Result<Self> {
      
        let builder = S3::default()
            .bucket("hummock001")
            .region("custom")
            .access_key_id("hummockadmin")
            .secret_access_key("hummockadmin")
            .endpoint("http://hummock001.127.0.0.1:9301")
            .disable_config_load();
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        Ok(Self {
            op,
        })
    }

    pub async fn upload(&self, path: &str, obj: Bytes) -> anyhow::Result<()> {
         self.op.write(path, obj).await?;
            Ok(())
    }

    pub async fn read_object(&self, path: &str) -> anyhow::Result<Bytes> {
        let data = self.op
                .read_with(path).await?;
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
    println!("连接s3");
    
    let engine = ObjectStoreEngine::new_minio_engine(&format!("minio://{}",  bucket))?;
    
    
    Ok(engine)
}

#[no_mangle]
pub extern "system" fn Java_com_risingwave_java_binding_Binding_writeFile(
    env: EnvParam<'_>,
)  {
    execute_and_catch(env, move |_env| {
        // 使用 block_on 来处理异步的 write 操作
        let engine = get_engine()?;
        let result = JAVA_BINDING_ASYNC_RUNTIME.block_on(async {
            let path = "hummock_001/file.txt"; 

            engine.op.write(path,  "Hello, World!").await
        }).unwrap();
        
        Ok(result)
    });
}