use http::request::Builder;
use hyper::body::{Body, Sender};
use hyper::client::HttpConnector;
use hyper::{body, Client, Request, StatusCode};
use hyper_tls::HttpsConnector;

use std::collections::HashMap;

use super::{Result, SinkError};

const SNOWFLAKE_HOST_ADDR: &str = "snowflakecomputing.com";
const SNOWFLAKE_REQUEST_ID: &str = "RW_SNOWFLAKE_SINK";

#[derive(Debug)]
pub struct SnowflakeInserterBuilder {
    url: String,
    header: HashMap<String, String>,
}

impl SnowflakeInserterBuilder {
    pub fn new(account: String, db: String, schema: String, pipe: String, header: HashMap<String, String>) -> Self {
        // TODO: ensure if we need user to *explicitly* provide the request id
        let url = format!("https://{}.{}/v1/data/pipes/{}.{}.{}/insertFiles?request_id={}",
            account,
            SNOWFLAKE_HOST_ADDR,
            db,
            schema.
            pipe,
            SNOWFLAKE_REQUEST_ID);
        
        Self {
            url,
            header,
        }
    }

    fn build_request_and_client() -> (Builder, Client<HttpsConnector<HttpConnector>>) {

    }

    pub async fn build(&self) -> Result<SnowflakeInserter> {

    }
}

#[derive(Debug)]
pub struct SnowflakeInserter {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>,
    buffer: BytesMut,
}