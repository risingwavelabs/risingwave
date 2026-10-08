// Copyright 2026 RisingWave Labs
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

#![cfg(not(madsim))]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use bytes::Bytes;
use iceberg::TableIdent;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::source::iceberg::IcebergProperties;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

// The local storage server uses HTTP; abfss requires an HTTPS endpoint.
const TABLE_LOCATION: &str = "abfs://fs@account.dfs.core.windows.net/table";
const ACCOUNT_SAS_KEY: &str = "adls.sas-token.account.dfs.core.windows.net";

#[derive(Default)]
struct Requests {
    table_loads: usize,
    refreshes: usize,
    reads: Vec<String>,
    writes: Vec<String>,
}

#[derive(Clone)]
struct MockState {
    endpoint: String,
    expires_at: SystemTime,
    requests: Arc<Mutex<Requests>>,
}

impl MockState {
    fn credentials(&self, refreshed: bool) -> Value {
        let generation = if refreshed { "new" } else { "old" };
        let expiry = if refreshed {
            SystemTime::now() + Duration::from_secs(3600)
        } else {
            self.expires_at
        };
        let expiry_ms = expiry.duration_since(UNIX_EPOCH).unwrap().as_millis();
        // The root grant deliberately differs from its nested metadata/data grants.
        // Signing all paths with the metadata credential or the first grant fails.
        json!(["", "/metadata", "/data"].map(|directory| {
            let scope = if directory.is_empty() { "root" } else { &directory[1..] };
            json!({
                "prefix": format!("{TABLE_LOCATION}{directory}/"),
                "config": {
                    ACCOUNT_SAS_KEY: format!("sig={scope}-{generation}"),
                    "adls.sas-token-expires-at-ms.account.dfs.core.windows.net": expiry_ms.to_string()
                }
            })
        }))
    }

    fn table_response(&self) -> Value {
        json!({
            "metadata-location": format!("{TABLE_LOCATION}/metadata/v1.json"),
            "metadata": {
                "format-version": 2,
                "table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
                "location": TABLE_LOCATION,
                "last-sequence-number": 0,
                "last-updated-ms": 1646787054459_i64,
                "last-column-id": 1,
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": [
                    {"id": 1, "name": "id", "required": false, "type": "long"}
                ]}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {}
            },
            "config": {
                "adls.endpoint": format!("{}/core.windows.net", self.endpoint),
                "io.max-retries": "0",
                "io.write.chunk-size": "1"
            },
            "storage-credentials": self.credentials(false)
        })
    }
}

async fn respond(
    State(state): State<MockState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    if uri.path().starts_with("/v1/") {
        if headers
            .get("x-iceberg-access-delegation")
            .and_then(|h| h.to_str().ok())
            != Some("vended-credentials")
        {
            return StatusCode::FORBIDDEN.into_response();
        }
        match uri.path() {
            "/v1/config" => return Json(json!({"defaults": {}, "overrides": {}})).into_response(),
            "/v1/namespaces/ns/tables/t" => {
                state.requests.lock().unwrap().table_loads += 1;
                return Json(state.table_response()).into_response();
            }
            "/v1/namespaces/ns/tables/t/credentials" => {
                state.requests.lock().unwrap().refreshes += 1;
                return Json(json!({"storage-credentials": state.credentials(true)}))
                    .into_response();
            }
            _ => return StatusCode::NOT_FOUND.into_response(),
        }
    }

    let query: HashMap<_, _> =
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let generation = if SystemTime::now() >= state.expires_at {
        "new"
    } else {
        "old"
    };
    let scope = if uri
        .path()
        .starts_with("/core.windows.net/fs/table/metadata/")
    {
        "metadata"
    } else if uri.path().starts_with("/core.windows.net/fs/table/data/") {
        "data"
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let signature = query.get("sig").cloned().unwrap_or_default();
    if signature != format!("{scope}-{generation}") {
        return (
            StatusCode::FORBIDDEN,
            [("x-ms-error-code", "AuthorizationFailure")],
        )
            .into_response();
    }
    match method {
        Method::GET => {
            state.requests.lock().unwrap().reads.push(signature);
            (StatusCode::OK, [("content-length", "4")], "data").into_response()
        }
        Method::HEAD => StatusCode::NOT_FOUND.into_response(),
        Method::PUT => {
            state.requests.lock().unwrap().writes.push(signature);
            (StatusCode::CREATED, [("etag", "test-etag")]).into_response()
        }
        Method::PATCH => {
            state.requests.lock().unwrap().writes.push(signature);
            (StatusCode::OK, [("etag", "test-etag")]).into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

struct MockServer(JoinHandle<()>);

impl Drop for MockServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[tokio::test]
async fn rest_vended_adls_source_refreshes_open_handles() {
    assert_adls_refresh(false).await;
}

#[tokio::test]
async fn rest_vended_adls_sink_refreshes_open_handles() {
    assert_adls_refresh(true).await;
}

async fn assert_adls_refresh(is_sink: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = MockState {
        endpoint: format!("http://{}", listener.local_addr().unwrap()),
        expires_at: SystemTime::now() + Duration::from_secs(3),
        requests: Arc::default(),
    };
    let router = Router::new().fallback(respond).with_state(state.clone());
    let _server = MockServer(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let options = BTreeMap::from([
        ("catalog.type".to_owned(), "rest".to_owned()),
        ("catalog.uri".to_owned(), state.endpoint.clone()),
        ("database.name".to_owned(), "ns".to_owned()),
        ("table.name".to_owned(), "t".to_owned()),
        ("vended_credentials".to_owned(), "true".to_owned()),
    ]);
    let catalog = if is_sink {
        let mut options = options;
        options.insert("type".to_owned(), "append-only".to_owned());
        IcebergConfig::from_btreemap(options)
            .unwrap()
            .create_catalog()
            .await
            .unwrap()
    } else {
        serde_json::from_value::<IcebergProperties>(serde_json::to_value(options).unwrap())
            .unwrap()
            .create_catalog()
            .await
            .unwrap()
    };
    let table = catalog
        .load_table(&TableIdent::from_strs(["ns", "t"]).unwrap())
        .await
        .unwrap();
    let io = table.file_io();
    let reader = io
        .new_input(format!("{TABLE_LOCATION}/data/input"))
        .unwrap()
        .reader()
        .await
        .unwrap();
    let metadata = io
        .new_input(format!("{TABLE_LOCATION}/metadata/v1.json"))
        .unwrap();
    assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data");
    assert_eq!(metadata.read().await.unwrap().as_ref(), b"data");
    let mut writer = io
        .new_output(format!("{TABLE_LOCATION}/data/output"))
        .unwrap()
        .writer()
        .await
        .unwrap();
    writer.write(Bytes::from_static(b"data")).await.unwrap();

    // Keep the same table, reader, and writer across expiry. No loadTable may
    // replace the query's metadata snapshot just to renew a storage grant.
    tokio::time::sleep(
        state
            .expires_at
            .duration_since(SystemTime::now())
            .unwrap_or_default()
            + Duration::from_millis(100),
    )
    .await;
    assert_eq!(reader.read(0..4).await.unwrap().as_ref(), b"data");
    assert_eq!(metadata.read().await.unwrap().as_ref(), b"data");
    writer.write(Bytes::from_static(b"data")).await.unwrap();
    writer.close().await.unwrap();

    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.table_loads, 1);
    assert_eq!(requests.refreshes, 1);
    assert_eq!(
        requests.reads,
        ["data-old", "metadata-old", "data-new", "metadata-new"]
    );
    assert!(
        requests
            .writes
            .iter()
            .any(|signature| signature == "data-new")
    );
    assert!(
        requests
            .writes
            .iter()
            .any(|signature| signature == "data-old")
    );
    assert!(
        io.config().get(ACCOUNT_SAS_KEY).is_none(),
        "vended tokens must stay in the runtime provider"
    );
    assert_eq!(table.metadata().location(), TABLE_LOCATION);
}
