use axum::Router;

mod embed;
mod proxy;

pub fn router() -> Router {
    if cfg!(dashboard_built) {
        embed::router()
    } else {
        proxy::router()
    }
}
