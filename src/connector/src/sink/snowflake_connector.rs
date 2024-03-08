use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use http::request::Builder;
use hyper::body::{Body, Sender};
use hyper::client::HttpConnector;
use hyper::{body, Client, Request, StatusCode};
use hyper_tls::HttpsConnector;
use tokio::task::JoinHandle;

use super::doris_starrocks_connector::POOL_IDLE_TIMEOUT;
use super::{Result, SinkError};

const SNOWFLAKE_HOST_ADDR: &str = "snowflakecomputing.com";
const SNOWFLAKE_REQUEST_ID: &str = "RW_SNOWFLAKE_SINK";

#[derive(Debug)]
pub struct SnowflakeInserterBuilder {
    url: String,
    header: HashMap<String, String>,
}

impl SnowflakeInserterBuilder {
    pub fn new(
        account: String,
        db: String,
        schema: String,
        pipe: String,
        header: HashMap<String, String>,
    ) -> Self {
        // TODO: ensure if we need user to *explicitly* provide the request id
        let url = format!(
            "https://{}.{}/v1/data/pipes/{}.{}.{}/insertFiles?request_id={}",
            account, SNOWFLAKE_HOST_ADDR, db, schema, pipe, SNOWFLAKE_REQUEST_ID
        );

        Self { url, header }
    }

    fn build_request_and_client(&self) -> (Builder, Client<HttpsConnector<HttpConnector>>) {
        let mut builder = Request::put(self.url.clone());
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }

        let connector = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        (builder, client)
    }

    pub async fn build(&self) -> Result<SnowflakeInserter> {
        Err(SinkError::Snowflake("err!".to_string()))
    }
}

#[derive(Debug)]
pub struct SnowflakeInserter {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    buffer: BytesMut,
}
