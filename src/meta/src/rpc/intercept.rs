// Copyright 2022 RisingWave Labs
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
use std::task::{Context, Poll};

use futures::Future;
use tonic::body::BoxBody;
use tower::{Layer, Service};

use crate::manager::MetaSrvEnv;
use crate::rpc::audit_log::{extract_audit_context, should_audit_path, write_audit_log};
use crate::rpc::metrics::MetaMetrics;

#[derive(Clone)]
pub struct MetricsMiddlewareLayer {
    metrics: Arc<MetaMetrics>,
}

impl MetricsMiddlewareLayer {
    pub fn new(metrics: Arc<MetaMetrics>) -> Self {
        Self { metrics }
    }
}

#[derive(Clone)]
pub struct AuditLogLayer {
    env: MetaSrvEnv,
}

impl AuditLogLayer {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self { env }
    }
}

impl<S> Layer<S> for AuditLogLayer {
    type Service = AuditLogMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuditLogMiddleware {
            inner: service,
            env: self.env.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuditLogMiddleware<S> {
    inner: S,
    env: MetaSrvEnv,
}

impl<S> Service<http::Request<BoxBody>> for AuditLogMiddleware<S>
where
    S: Service<http::Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let env = self.env.clone();

        async move {
            let path = req.uri().path().to_owned();
            let audit_context = if should_audit_path(&path) {
                extract_audit_context(req.headers())
            } else {
                None
            };
            let response = inner.call(req).await?;
            if let Some(ctx) = audit_context {
                write_audit_log(&env, ctx, &path).await;
            }
            Ok(response)
        }
    }
}

impl<S> Layer<S> for MetricsMiddlewareLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddleware {
            inner: service,
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsMiddleware<S> {
    inner: S,
    metrics: Arc<MetaMetrics>,
}

impl<S> Service<http::Request<BoxBody>> for MetricsMiddleware<S>
where
    S: Service<http::Request<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Error = S::Error;
    type Response = S::Response;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let metrics = self.metrics.clone();

        async move {
            let path = req.uri().path();
            let timer = metrics
                .grpc_latency
                .with_label_values(&[path])
                .start_timer();

            let response = inner.call(req).await?;

            timer.observe_duration();

            Ok(response)
        }
    }
}
