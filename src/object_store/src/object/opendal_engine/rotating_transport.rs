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

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use http::{Request, Response};
use opendal::raw::{HttpBody, HttpFetch};
use opendal::{Buffer, Error, ErrorKind, Result};
use thiserror_ext::AsReport as _;
use tokio::sync::oneshot;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_retry::strategy::jitter;

/// Periodically replace the HTTP client while existing response bodies finish normally.
pub(super) struct RotatingTransport {
    current: Arc<ArcSwap<reqwest::Client>>,
    // Dropping the last transport handle closes the channel and stops rotation.
    _shutdown_tx: oneshot::Sender<()>,
}

impl RotatingTransport {
    pub(super) fn new(
        mut factory: impl FnMut() -> Result<reqwest::Client> + Send + 'static,
        interval: Duration,
    ) -> Result<Self> {
        if interval.is_zero() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "HTTP client rotation interval must be positive",
            ));
        }
        let current = Arc::new(ArcSwap::from_pointee(factory()?));
        // Delay the first refresh and spread its phase across operators. Later
        // refreshes use the configured interval without accumulating build time.
        let start = Instant::now()
            .checked_add(interval)
            .and_then(|start| start.checked_add(jitter(interval)))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "HTTP client rotation deadline overflow",
                )
            })?;
        let mut ticker = interval_at(start, interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let _task = tokio::spawn({
            let current = current.clone();
            async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut shutdown_rx => break,
                        _ = ticker.tick() => {}
                    }
                    match factory() {
                        Ok(client) => current.store(Arc::new(client)),
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "failed to rotate object store HTTP client")
                        }
                    }
                }
            }
        });
        Ok(Self {
            current,
            _shutdown_tx: shutdown_tx,
        })
    }
}

impl HttpFetch for RotatingTransport {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.current.load_full().fetch(request).await
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, Weak};

    use bytes::Bytes;
    use futures::stream;
    use http_body_util::StreamBody;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper1::body::Frame;
    use hyper1::service::service_fn;
    use opendal::raw::oio::Read as _;
    use reqwest::dns::{Addrs, Name, Resolve, Resolving};
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, mpsc};
    use tokio::task::{JoinHandle, JoinSet};

    use super::*;

    const INTERVAL: Duration = Duration::from_secs(10);

    async fn advance_time(duration: Duration) {
        // Resume before socket I/O so Tokio cannot auto-advance past idle pool timers.
        tokio::time::pause();
        tokio::task::yield_now().await;
        tokio::time::advance(duration).await;
        tokio::task::yield_now().await;
        tokio::time::resume();
    }

    #[tokio::test]
    async fn periodic_rotation_keeps_current_client_on_failure() {
        let fail = Arc::new(AtomicBool::new(false));
        let builds = Arc::new(AtomicUsize::new(0));
        let transport = RotatingTransport::new(
            {
                let fail = fail.clone();
                let builds = builds.clone();
                move || {
                    builds.fetch_add(1, Ordering::Relaxed);
                    if fail.load(Ordering::Relaxed) {
                        return Err(Error::new(ErrorKind::Unexpected, "injected build failure"));
                    }
                    Ok(reqwest::Client::new())
                }
            },
            INTERVAL,
        )
        .unwrap();
        let original = transport.current.load_full();
        tokio::task::yield_now().await;
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        advance_time(INTERVAL / 2).await;
        assert_eq!(builds.load(Ordering::Relaxed), 1);

        // Refresh without requests, skipping missed ticks instead of building a burst.
        fail.store(true, Ordering::Relaxed);
        advance_time(INTERVAL * 100).await;
        assert_eq!(builds.load(Ordering::Relaxed), 2);
        assert!(Arc::ptr_eq(&original, &transport.current.load_full()));

        fail.store(false, Ordering::Relaxed);
        advance_time(INTERVAL * 2).await;
        let replacement = transport.current.load_full();
        assert_eq!(builds.load(Ordering::Relaxed), 3);
        assert!(!Arc::ptr_eq(&original, &replacement));
        let original_weak = Arc::downgrade(&original);
        drop(original);
        assert!(original_weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn streaming_body_survives_client_rotation_and_shutdown() {
        for http2 in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (body_tx, body_rx) = mpsc::unbounded_channel::<Bytes>();
            let body_rx = Arc::new(Mutex::new(Some(body_rx)));
            let requests = Arc::new(AtomicUsize::new(0));
            let server = tokio::spawn({
                let requests = requests.clone();
                async move {
                    let (socket, _) = listener.accept().await.unwrap();
                    let service = service_fn(move |_request| {
                        requests.fetch_add(1, Ordering::Relaxed);
                        let body_rx = body_rx.lock().unwrap().take().unwrap();
                        async move {
                            let stream = stream::unfold(body_rx, |mut rx| async move {
                                rx.recv()
                                    .await
                                    .map(|bytes| (Ok::<_, Infallible>(Frame::data(bytes)), rx))
                            });
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .header("content-length", "6")
                                    .body(StreamBody::new(stream))
                                    .unwrap(),
                            )
                        }
                    });
                    if http2 {
                        hyper1::server::conn::http2::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(socket), service)
                            .await
                            .unwrap();
                    } else {
                        hyper1::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(socket), service)
                            .await
                            .unwrap();
                    }
                }
            });
            let (factory_tx, factory_rx) = oneshot::channel::<()>();
            let transport = RotatingTransport::new(
                move || {
                    // Dropping the factory closes this channel and confirms task exit.
                    let _keep_alive = &factory_tx;
                    let builder = reqwest::Client::builder().no_proxy();
                    let builder = if http2 {
                        builder.http2_prior_knowledge()
                    } else {
                        builder.http1_only()
                    };
                    Ok(builder.build().unwrap())
                },
                INTERVAL,
            )
            .unwrap();
            let current = Arc::downgrade(&transport.current);
            let original = Arc::downgrade(&transport.current.load_full());
            body_tx.send(Bytes::from_static(b"A")).unwrap();
            let response = transport
                .fetch(
                    Request::builder()
                        .uri(format!("http://{address}/held"))
                        .body(Buffer::new())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.version(),
                if http2 {
                    http::Version::HTTP_2
                } else {
                    http::Version::HTTP_11
                }
            );
            let mut body = response.into_body();
            assert_eq!(body.read().await.unwrap().to_bytes(), "A");

            advance_time(INTERVAL * 2).await;
            assert!(original.upgrade().is_none());
            drop(transport);
            assert!(
                tokio::time::timeout(Duration::from_secs(1), factory_rx)
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(current.upgrade().is_none());

            // The remaining bytes are produced only after the old client is gone
            // and the rotation task has stopped. The native body still completes.
            body_tx.send(Bytes::from_static(b"tail!")).unwrap();
            drop(body_tx);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), body.to_buffer())
                    .await
                    .unwrap()
                    .unwrap()
                    .to_bytes(),
                "tail!"
            );
            assert_eq!(requests.load(Ordering::Relaxed), 1);
            server.abort();
        }
    }

    struct Resolver(Mutex<SocketAddr>);

    impl Resolve for Resolver {
        fn resolve(&self, name: Name) -> Resolving {
            assert_eq!(name.as_str(), "rotation.test");
            let address = *self.0.lock().unwrap();
            Box::pin(async move { Ok(Box::new(std::iter::once(address)) as Addrs) })
        }
    }

    struct Server {
        address: SocketAddr,
        release_body: Arc<Notify>,
        connections: Arc<AtomicUsize>,
        task: JoinHandle<()>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Server {
        async fn start(name: &'static str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let release_body = Arc::new(Notify::new());
            let connections = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn({
                let release_body = release_body.clone();
                let connections = connections.clone();
                async move {
                    let mut requests = JoinSet::new();
                    loop {
                        tokio::select! {
                            accepted = listener.accept() => {
                                let (socket, _) = accepted.unwrap();
                                connections.fetch_add(1, Ordering::Relaxed);
                                let release_body = release_body.clone();
                                requests.spawn(async move {
                                    let mut socket = BufReader::new(socket);
                                    loop {
                                        let mut request = String::new();
                                        loop {
                                            let mut line = String::new();
                                            match socket.read_line(&mut line).await {
                                                Ok(0) | Err(_) => return,
                                                Ok(_) => {}
                                            }
                                            request.push_str(&line);
                                            if line == "\r\n" { break; }
                                        }
                                        let headers = request.to_ascii_lowercase();
                                        assert!(headers.contains("host: rotation.test\r\n"));
                                        assert!(headers.contains("authorization: test-signature\r\n"));
                                        assert!(headers.contains("range: bytes=0-5\r\n"));
                                        let held = request.starts_with("GET /held ");
                                        let broken = request.starts_with("GET /broken ");
                                        let length = if held { 6 } else if broken { 100 } else { 1 };
                                        let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nx-backend: {name}\r\n\r\n");
                                        socket.write_all(response.as_bytes()).await.unwrap();
                                        if request.starts_with("HEAD ") { continue; }
                                        socket.write_all(name.as_bytes()).await.unwrap();
                                        if broken { return; }
                                        if held {
                                            release_body.notified().await;
                                            socket.write_all(b"tail!").await.unwrap();
                                        }
                                    }
                                });
                            }
                            result = requests.join_next(), if !requests.is_empty() => {
                                result.unwrap().unwrap();
                            }
                        }
                    }
                }
            });
            Self {
                address,
                release_body,
                connections,
                task,
            }
        }
    }

    fn request(path: &str) -> Request<Buffer> {
        Request::builder()
            .uri(format!("http://rotation.test/{path}"))
            .header("authorization", "test-signature")
            .header("range", "bytes=0-5")
            .body(Buffer::new())
            .unwrap()
    }

    async fn fetch_backend(transport: &impl HttpFetch) -> String {
        let response = transport.fetch(request("object")).await.unwrap();
        assert_eq!(response.status(), 200);
        let expected = response.headers()["x-backend"].to_str().unwrap().to_owned();
        let data = response.into_body().to_buffer().await.unwrap();
        assert_eq!(data.to_bytes(), expected);
        expected
    }

    #[tokio::test]
    async fn dns_rotation_preserves_old_streams_and_releases_clients() {
        let a = Server::start("A").await;
        let b = Server::start("B").await;
        let resolver = Arc::new(Resolver(Mutex::new(a.address)));
        let fail = Arc::new(AtomicBool::new(false));
        let transport = RotatingTransport::new(
            {
                let resolver = resolver.clone();
                let fail = fail.clone();
                move || {
                    if fail.load(Ordering::Relaxed) {
                        return Err(Error::new(ErrorKind::Unexpected, "injected build failure"));
                    }
                    Ok(reqwest::Client::builder()
                        .no_proxy()
                        .dns_resolver(resolver.clone())
                        .build()
                        .unwrap())
                }
            },
            INTERVAL,
        )
        .unwrap();

        for _ in 0..3 {
            assert_eq!(fetch_backend(&transport).await, "A");
        }
        assert_eq!(a.connections.load(Ordering::Relaxed), 1);
        let single_transport = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver.clone())
            .build()
            .unwrap();
        assert_eq!(fetch_backend(&single_transport).await, "A");
        *resolver.0.lock().unwrap() = b.address;
        // DNS alone leaves a continuously reused healthy connection on A.
        for _ in 0..3 {
            assert_eq!(fetch_backend(&transport).await, "A");
        }
        let original: Weak<reqwest::Client> = Arc::downgrade(&transport.current.load_full());
        let mut held = transport.fetch(request("held")).await.unwrap().into_body();
        assert_eq!(held.read().await.unwrap().to_bytes(), "A");

        fail.store(true, Ordering::Relaxed);
        advance_time(INTERVAL * 2).await;
        // The single-client control keeps reusing A while a failed refresh leaves
        // the rotating transport's current client available to new requests.
        assert_eq!(fetch_backend(&single_transport).await, "A");
        assert!(Arc::ptr_eq(
            &original.upgrade().unwrap(),
            &transport.current.load_full()
        ));
        fetch_backend(&transport).await;
        fail.store(false, Ordering::Relaxed);
        advance_time(INTERVAL * 2).await;
        assert_eq!(fetch_backend(&transport).await, "B");
        assert!(original.upgrade().is_none());
        a.release_body.notify_one();
        assert_eq!(held.to_buffer().await.unwrap().to_bytes(), "tail!");
        let mut head = request("object");
        *head.method_mut() = http::Method::HEAD;
        assert!(
            transport
                .fetch(head)
                .await
                .unwrap()
                .into_body()
                .to_buffer()
                .await
                .unwrap()
                .is_empty()
        );

        let mut broken = transport
            .fetch(request("broken"))
            .await
            .unwrap()
            .into_body();
        let error = broken.to_buffer().await.unwrap_err();
        assert!(error.is_temporary());
    }
}
