use hyper::{Body, Request, Response};
use prometheus::{Encoder, TextEncoder};

pub struct StorageMetricsManager {}

impl StorageMetricsManager {
    pub async fn hummock_metrics_service(
        _req: Request<Body>,
    ) -> Result<Response<Body>, hyper::Error> {
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = prometheus::gather();
        encoder.encode(&mf, &mut buffer).unwrap();
        let response = Response::builder()
            .header(hyper::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap();

        Ok(response)
    }
}
