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
use jni::objects::{JString, JObject};
use jni::sys::{jbyteArray, jint};
use tracing::Level;
use opendal::{layers::LoggingLayer, Builder, Operator};
use opendal::services::S3;

use crate::{execute_and_catch, EnvParam};



struct ObjectStoreEngine {
    op: Operator,

}

impl ObjectStoreEngine {
    pub fn new_minio_engine(
        server: &str,
    ) -> anyhow::Result<Self> {
        let server = server.strip_prefix("minio://").unwrap();
        let (access_key_id, rest) = server.split_once(':').unwrap();
        let (secret_access_key, mut rest) = rest.split_once('@').unwrap();

        let endpoint_prefix = if let Some(rest_stripped) = rest.strip_prefix("https://") {
            rest = rest_stripped;
            "https://"
        } else if let Some(rest_stripped) = rest.strip_prefix("http://") {
            rest = rest_stripped;
            "http://"
        } else {
            "http://"
        };
        let (address, bucket) = rest.split_once('/').unwrap();
        let builder = S3::default()
            .bucket(bucket)
            .region("custom")
            .access_key_id(access_key_id)
            .secret_access_key(secret_access_key)
            .endpoint(&format!("{}{}", endpoint_prefix, address))
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
    
    let engine = ObjectStoreEngine::new_minio_engine(&format!("minio://{}",  bucket))?;
    
    
    Ok(engine)
}

#[no_mangle]
pub extern "system" fn Java_com_yourpackage_binding_Binding_buildOp(
    env: EnvParam<'_>,
    obj: JObject,
) {
    let result = execute_and_catch(env, move |_env| {
        let engine = get_engine()?;
        let objects = engine.list("/").await?;
        Ok(())
    });

    
}
