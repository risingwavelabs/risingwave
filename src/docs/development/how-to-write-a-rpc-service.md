# How to write a service

Quick example on how to write a service.

## Define actual service

Add your service definition under `src/<component>/src/rcp/service/<service_name>.rs`, e.g. `src/meta/src/rpc/service/health_service.rs`.

```rust 
pub struct HealthServiceImpl {}
impl HealthServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}
#[async_trait::async_trait]
impl Health for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        // Reply serving as long as tonic service is started
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }
}
```

Also see the other services in `src/<component>/src/rcp/service/` for further examples. Above service defines a `HealthCheckRequest` and a `HealthCheckResponse`, which use proto messages. We have to define these proto messages.

## Proto definitions

Add definition in `proto/health.proto`. For above service it is

```
package health;

message HealthCheckRequest {
  string service = 1;
}

message HealthCheckResponse {
  enum ServingStatus {
    UNKNOWN = 0;
    SERVING = 1;
    NOT_SERVING = 2;
  }
  ServingStatus status = 1;
}

service Health {
  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
}
```

Make sure to lint your file using [buf](https://docs.buf.build/installation).

## Use service 


Add your module in `src/prost/src/lib.rs`, like so: 

```rust
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/health.rs")]
pub mod health;
```

Add your proto file `"health"` in `src/prost/build.rs`.

Add the module in `src/meta/src/rpc/service/mod.rs`.

Use your service in `src/meta/src/rpc/server.rs`, like 

```rust
let health_srv = HealthServiceImpl::new();
tokio::spawn(async move {
     tonic::transport::Server::builder()
         .layer(MetricsMiddlewareLayer::new(meta_metrics.clone()))
         .add_service(HealthServer::new(health_srv))
         .serve(address_info.listen_addr)
         .await
         .unwrap();
});
```

To use your service you need to generate some boilerplate code, like e.g. the service definition. Running `risedev d` should generate this boilerplate code.

