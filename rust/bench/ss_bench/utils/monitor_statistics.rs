use std::convert::Infallible;
use std::thread;
use std::time::Duration;

use hyper::body::HttpBody;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Request, Response, Server};
use prometheus::{Encoder, Registry, TextEncoder};

async fn prometheus_service(
    _req: Request<Body>,
    registry: &Registry,
) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = registry.gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    let response = Response::builder()
        .header(hyper::header::CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

pub(crate) async fn print_statistics() {
    let make_svc = make_service_fn(move |_| {
        let registry = prometheus::default_registry();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| async move {
                prometheus_service(req, registry).await
            }))
        }
    });

    let host_addr = "127.0.0.1:1222";
    let server = Server::bind(&host_addr.parse().unwrap()).serve(make_svc);

    tokio::spawn(async move {
        if let Err(err) = server.await {
            eprintln!("server error: {}", err);
        }
    });

    // ensure that the prometheus endpoint is up and running
    thread::sleep(Duration::from_millis(1000));
    let client = Client::new();
    let uri = "http://127.0.0.1:1222/metrics".parse().unwrap();
    let mut response = client.get(uri).await.unwrap();

    let mut web_page: Vec<u8> = Vec::new();
    while let Some(next) = response.data().await {
        let chunk = next.unwrap();
        web_page.append(&mut chunk.to_vec());
    }

    let s = String::from_utf8_lossy(&web_page);
    println!("\n---{}---\n", s);
}
