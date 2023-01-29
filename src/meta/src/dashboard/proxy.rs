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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use axum::body::Body;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use hyper::header::CONTENT_TYPE;
use hyper::{HeaderMap, Request};
use parking_lot::Mutex;
use url::Url;

#[derive(Clone)]
pub struct CachedResponse {
    code: StatusCode,
    body: Bytes,
    headers: HeaderMap,
    uri: Url,
}

impl IntoResponse for CachedResponse {
    fn into_response(self) -> Response {
        let guess = mime_guess::from_path(self.uri.path());
        let mut headers = HeaderMap::new();
        if let Some(x) = self.headers.get(hyper::header::ETAG) {
            headers.insert(hyper::header::ETAG, x.clone());
        }
        if let Some(x) = self.headers.get(hyper::header::CACHE_CONTROL) {
            headers.insert(hyper::header::CACHE_CONTROL, x.clone());
        }
        if let Some(x) = self.headers.get(hyper::header::EXPIRES) {
            headers.insert(hyper::header::EXPIRES, x.clone());
        }
        if let Some(x) = guess.first() {
            if x.type_() == "image" && x.subtype() == "svg" {
                headers.insert(CONTENT_TYPE, "image/svg+xml".parse().unwrap());
            } else {
                headers.insert(
                    CONTENT_TYPE,
                    format!("{}/{}", x.type_(), x.subtype()).parse().unwrap(),
                );
            }
        }
        (self.code, headers, self.body).into_response()
    }
}

pub async fn proxy(
    req: Request<Body>,
    cache: Arc<Mutex<HashMap<String, CachedResponse>>>,
) -> anyhow::Result<Response> {
    let mut path = req.uri().path().to_string();
    if path.ends_with('/') {
        path += "index.html";
    }

    if let Some(resp) = cache.lock().get(&path) {
        return Ok(resp.clone().into_response());
    }

    let url_str = format!(
        "https://raw.githubusercontent.com/risingwavelabs/risingwave/dashboard-artifact{}",
        path
    );
    let url = Url::parse(&url_str)?;
    if url.to_string() != url_str {
        return Err(anyhow!("normalized URL isn't the same as the original one"));
    }

    tracing::info!("dashboard service: proxying {}", url);

    let content = reqwest::get(url.clone()).await?;

    let resp = CachedResponse {
        code: content.status(),
        headers: content.headers().clone(),
        body: content.bytes().await?,
        uri: url,
    };

    cache.lock().insert(path, resp.clone());

    Ok(resp.into_response())
}
