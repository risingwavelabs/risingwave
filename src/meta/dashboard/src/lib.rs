use axum::Router;

mod embed;
mod proxy;

pub fn router() -> Router {
    if cfg!(dashboard_built) {
        tracing::info!("using embedded dashboard assets");
        embed::router()
    } else {
        tracing::info!("using proxied dashboard assets");
        proxy::router()
    }
}
