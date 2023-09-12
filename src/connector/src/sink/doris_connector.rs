// Copyright 2023 RisingWave Labs
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

use core::mem;
use core::time::Duration;
use std::collections::HashMap;

use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use hyper::body::Sender;
use hyper::client::HttpConnector;
use hyper::{body, Body, Client as HttpClient, Request, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 1 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
pub struct DorisInsertClient {
    url: String,
    header: HashMap<String, String>,
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<DorisInsertResultResponse>>>,
    buffer: BytesMut,
}
impl DorisInsertClient {
    pub fn new(url: String,db: String, table: String) -> Self {
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);
        Self {
            url,
            header: HashMap::default(),
            sender: None,
            join_handle: None,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    pub fn set_url(mut self, url: String) -> Self {
        self.url = url;
        self
    }

    pub fn add_common_header(mut self) -> Self {
        self.header
            .insert("expect".to_string(), "100-continue".to_string());
        self
    }
    pub fn set_label(mut self,label: String) -> Self {
        self.header
            .insert("label".to_string(), label);
        self
    }

    pub fn add_hidden_column(mut self) -> Self {
        self.header.insert(
            "hidden_columns".to_string(),
            "__DORIS_DELETE_SIGN__".to_string(),
        );
        self
    }

    pub fn enable_2_pc(mut self) -> Self {
        self.header
            .insert("two_phase_commit".to_string(), "true".to_string());
        self
    }

    pub fn set_user_password(mut self, user: String, password: String) -> Self {
        let auth = format!("Basic {}", general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", user, password)));
        self.header.insert("Authorization".to_string(), auth);
        self
    }

    pub fn set_txn_id(mut self, txn_id: i64) -> Self {
        self.header
            .insert("txn_operation".to_string(), txn_id.to_string());
        self
    }

    pub fn add_commit(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "commit".to_string());
        self
    }

    pub fn add_abort(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "abort".to_string());
        self
    }

    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.header.extend(properties);
        self
    }

    pub fn build(&mut self) -> Result<DorisInsert> {
        let mut builder = Request::put(self.url.as_str());
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }
        let (sender, body) = Body::channel();
        let request = builder
            .body(body)
            .map_err(|err| SinkError::Http(err.into()))?;

        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(Duration::from_secs(60)));
        let client = HttpClient::builder()
            .pool_idle_timeout(Duration::from_secs(2))
            .build(connector);

        let feature = client.request(request);
        let handle: JoinHandle<Result<DorisInsertResultResponse>> = tokio::spawn(async move {
            let response = feature.await.map_err(|err| SinkError::Http(err.into()))?;
            let status = response.status();
            // println!("aaa{:?}",response.headers().clone());
            let raw_string = String::from_utf8(match body::to_bytes(response.into_body()).await {
                Ok(bytes) => bytes.to_vec(),
                Err(err) => return Err(SinkError::Http(err.into())),
            })
            .map_err(|err| SinkError::Http(err.into()))?;

            if status == StatusCode::OK && !raw_string.is_empty() {
                // println!("{:?},{:?}",raw_string,status);
                let response: DorisInsertResultResponse =
                    serde_json::from_str(&raw_string).map_err(|err| SinkError::Http(err.into()))?;
                Ok(response)
            } else {
                Err(SinkError::Http(anyhow::anyhow!(
                    "Failed connection {:?},{:?}",
                    status,raw_string
                )))
            }
        });
        
        Ok(DorisInsert::new(sender, handle))
    }

}

pub struct DorisInsert {
    sender: Option<Sender>,
    join_handle:Option<JoinHandle<Result<DorisInsertResultResponse>>>,
    buffer: BytesMut,
    // i: i128,
}
impl DorisInsert {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<DorisInsertResultResponse>> ) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
            // i: 0,
        }
    }

    async fn send_chunk(&mut self) -> Result<()> {
        // self.i = self.i + 1;
        // println!("{:?}", self.i);
        if self.sender.is_none() {
            return Ok(());
        }

        let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

        let is_timed_out = match tokio::time::timeout(
            Duration::from_secs(10),
            self.sender.as_mut().unwrap().send_data(chunk.into()),
        )
        .await
        {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(_)) => false,
            Err(_) => true,
        };
        self.abort()?;

        let res = self.wait_handle().await;

        if is_timed_out {
            Err(SinkError::Http(anyhow::anyhow!("timeout")))
        } else {
            res?;
            Err(SinkError::Http(anyhow::anyhow!("channel closed")))
        }
    }

    fn abort(&mut self) -> Result<()> {
        if let Some(sender) = self.sender.take() {
            sender.abort();
        }
        Ok(())
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        self.buffer.put_slice(&data);
        if self.buffer.len() >= MIN_CHUNK_SIZE {
            self.send_chunk().await?;
        }
        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<DorisInsertResultResponse> {
        let res =
            match tokio::time::timeout(Duration::from_secs(10), self.join_handle.as_mut().unwrap())
                .await
            {
                Ok(res) => res.map_err(|err| SinkError::Http(err.into()))??,
                Err(err) => return Err(SinkError::Http(err.into())),
            };
        if !vec!["Success".to_string(), "Publish Timeout".to_string()].contains(&res.status) {
            return Err(SinkError::Http(anyhow::anyhow!(
                "Insert error: {:?}",
                res.message
            )));
        };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<DorisInsertResultResponse> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.sender = None;
        self.wait_handle().await
    }

}

pub struct DorisGet {
    client: HttpClient<HttpConnector>,
    url: String,
    table: String,
    db: String,
    user: String,
    password: String,
}
impl DorisGet {
    pub fn new(
        url: String,
        table: String,
        db: String,
        user: String,
        password: String
    ) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(Duration::from_secs(60)));
        let client = HttpClient::builder()
            .pool_idle_timeout(Duration::from_secs(10))
            .build(connector);
        Self { client ,url,table,db,user,password}
    }

    pub async fn get_schema_from_doris(
        &self,
    ) -> Result<DorisSchema> {
        let uri = format!("{}/api/{}/{}/_schema", self.url, self.db, self.table);
        let builder = Request::get(uri);
        let response = builder
            .header("Authorization", format!("Basic {}", general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", self.user, self.password))))
            .body(Body::empty())
            .map_err(|err| SinkError::Http(err.into()))?;
        // println!("{:?}",response);
        let response_body = self
            .client
            .request(response)
            .await
            .map_err(|err| SinkError::Http(err.into()))?
            .into_body();

        let raw_bytes = String::from_utf8(match body::to_bytes(response_body).await {
            Ok(bytes) => bytes.to_vec(),
            Err(err) => return Err(SinkError::Http(err.into())),
        })
        .map_err(|err| SinkError::Http(err.into()))?;

        // println!("{}",raw_bytes);
        let json_map: HashMap<String, Value> =
            serde_json::from_str(&raw_bytes).map_err(|err| SinkError::Http(err.into()))?;
        let json_data = if json_map.contains_key("code") && json_map.contains_key("msg") {
            let data = json_map
                .get("data")
                .ok_or(SinkError::Http(anyhow::anyhow!("Can't find data")))?;
            data.to_string().clone()
        } else {
            raw_bytes.to_string()
        };
        let schema: DorisSchema =
            serde_json::from_str(&json_data).map_err(|err| SinkError::Http(err.into()))?;
        Ok(schema)
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DorisSchema {
    status: i32,
    #[serde(rename = "keysType")]
    keys_type: String,
    pub properties: Vec<DorisField>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DorisField {
    pub name: String,
    pub r#type: String,
    comment: String,
    precision: Option<i32>,
    scale: Option<i32>,
    aggregation_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DorisInsertResultResponse {
    #[serde(rename = "TxnId")]
    txn_id: i64,
    #[serde(rename = "Label")]
    label: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "TwoPhaseCommit")]
    two_phase_commit: String,
    // #[serde(rename = "keysType")]
    // existing_job_status: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "NumberTotalRows")]
    number_total_rows: i64,
    #[serde(rename = "NumberLoadedRows")]
    number_loaded_rows: i64,
    #[serde(rename = "NumberFilteredRows")]
    number_filtered_rows: i32,
    #[serde(rename = "NumberUnselectedRows")]
    number_unselected_rows: i32,
    #[serde(rename = "LoadBytes")]
    load_bytes: i64,
    #[serde(rename = "LoadTimeMs")]
    load_time_ms: i32,
    #[serde(rename = "BeginTxnTimeMs")]
    begin_txn_time_ms: i32,
    #[serde(rename = "StreamLoadPutTimeMs")]
    stream_load_put_time_ms: i32,
    #[serde(rename = "ReadDataTimeMs")]
    read_data_time_ms: i32,
    #[serde(rename = "WriteDataTimeMs")]
    write_data_time_ms: i32,
    #[serde(rename = "CommitAndPublishTimeMs")]
    commit_and_publish_time_ms: i32,
    // #[serde(rename = "keysType")]
    // error_url: String,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use chrono::{Local, DateTime};

    use crate::sink::doris_connector::{DorisInsert, DorisInsertClient};

    use super::DorisGet;


    #[tokio::test]
    async fn test_doris_connected() {
        let a = DorisGet::new("http://127.0.0.1:8030".to_string(), "example_tbl2".to_string(), "demo".to_string(), "xxhx".to_string(), "123456".to_string());
        let b= a.get_schema_from_doris().await.unwrap();
        println!("{:?}",b);
    }

    #[tokio::test]
    async fn test_doris_insert() {
        let mut map = HashMap::new();
            map.insert("format".to_string(), "json".to_string());
            map.insert("read_json_by_line".to_string(), "true".to_string());
            let mut a = DorisInsertClient::new("http://127.0.0.1:8040".to_string(),"demo".to_string(), "example_tbl".to_string())
            .add_common_header()
            .set_user_password("xxhx".to_string(), "123456".to_string())
            // .set_label("1".to_string())
            .set_properties(map);
        for i in 0..100{
            let mut b = a.build().unwrap(); 
            for j in 0..10000{
                let local_time: DateTime<Local> = Local::now();
                let format = "%Y-%m-%d %H:%M:%S";
                let formatted_time = local_time.format(format).to_string();
                let json = format!( "{{\"user_id\": {:?}, \"date\":\"2017-10-01\" , \"city\":\"北京\", \"age\": 20,\"sex\": 0, \"last_visit_date\":\"{}\" , \"cost\":20,\"max_dwell_time\":10,\"min_dwell_time\":10}}\n",i*10000+j,formatted_time); 
                b.write(bytes::Bytes::from(json)).await.unwrap();
            }
            b.finish().await.unwrap();
        }
    }
}