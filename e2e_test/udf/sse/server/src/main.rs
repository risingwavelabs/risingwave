use std::convert::Infallible;

use axum::{http, response::sse::Event};
use futures::StreamExt;

async fn sse_handle(
    request: http::Request<hyper::Body>,
) -> axum::response::Sse<impl futures::stream::Stream<Item = Result<Event, Infallible>>> {
    let (parts, body) = request.into_parts();
    let body_bytes = hyper::body::to_bytes(body).await.unwrap();

    let accept = parts
        .headers
        .get(http::header::ACCEPT)
        .unwrap()
        .to_str()
        .unwrap();

    assert_eq!(accept, "text/event-stream");

    let payload: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert!(serde_json::to_string(&payload)
        .expect("should be a valid json")
        .contains("subscription"),);

    let stream = futures::stream::iter(vec!["Hi", "Bonjour", "Hola", "Ciao", "Zdravo"])
        .map(|name| {
            let response = serde_json::json!({
                "data": {
                    "greetings": name
                }
            });

            Event::default()
                .event("next")
                .data(serde_json::to_string(&response).unwrap())
        })
        .chain(futures::stream::once(futures::future::ready(
            Event::default().event("complete"),
        )))
        .map(Ok);

    axum::response::Sse::new(stream)
}

#[tokio::main]
async fn main() {
    let router = axum::Router::new().route("/graphql/stream", axum::routing::post(sse_handle));

    let listener = std::net::TcpListener::bind("127.0.0.1:4200").unwrap();

    let socket_addr = listener.local_addr().unwrap();


    let server = hyper::Server::from_tcp(listener)
        .unwrap()
        .serve(router.into_make_service());
    server.await.unwrap();
}
