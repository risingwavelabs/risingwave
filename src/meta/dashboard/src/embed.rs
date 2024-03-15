// Copyright 2024 RisingWave Labs
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

use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse as _, Response};
use axum::Router;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$OUT_DIR/assets"]
struct Assets;

// TODO: switch to `axum-embed` for better robustness after bumping to axum 0.7.
async fn static_handler(uri: Uri) -> Response {
    let path = {
        if uri.path().ends_with('/') {
            // Append `index.html` to directory paths.
            format!("{}index.html", uri.path())
        } else {
            uri.path().to_owned()
        }
    };
    let path = path.trim_start_matches('/');

    match Assets::get(path) {
        Some(file) => {
            let mime = file.metadata.mimetype();

            let mut res = file.data.into_response();
            res.headers_mut()
                .insert(header::CONTENT_TYPE, mime.parse().unwrap());
            res
        }

        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

/// Router for embedded assets.
pub(crate) fn router() -> Router {
    Router::new().fallback(static_handler)
}
