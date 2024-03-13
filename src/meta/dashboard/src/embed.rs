use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse as _, Response};
use axum::Router;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "$OUT_DIR/assets"]
struct Assets;

const INDEX_HTML: &str = "index.html";

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    if path.is_empty() {
        let data = Assets::get(INDEX_HTML).unwrap().data;
        return Html(data).into_response();
    }

    match Assets::get(path) {
        Some(file) => {
            let mime = file.metadata.mimetype();

            let mut res = file.data.into_response();
            res.headers_mut()
                .insert(header::CONTENT_TYPE, mime.parse().unwrap());
            res
        }

        // TODO: document why we don't need SPA hack here
        None => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}

pub(crate) fn router() -> Router {
    Router::new().fallback(static_handler)
}
