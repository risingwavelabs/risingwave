# How to write a service

Quick example on how to write a service. The example leader service is implemented [in this PR](https://github.com/risingwavelabs/risingwave/pull/6466)

## Define actual service

Add your service definition under `src/<component>/src/rcp/service/<service_name>.rs`. 

```rust 
#[derive(Clone)]
pub struct LeaderServiceImpl {
    leader_rx: Receiver<(HostAddress, bool)>,
}
impl LeaderServiceImpl {
    pub fn new(cluster_manager: Receiver<(HostAddress, bool)>) -> Self {
        LeaderServiceImpl {
            leader_rx: cluster_manager,
        }
    }
}
#[async_trait::async_trait]
impl LeaderService for LeaderServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn leader(
        &self,
        _request: Request<LeaderRequest>,
    ) -> Result<Response<LeaderResponse>, Status> {
        let leader_addr = self.leader_rx.borrow().0.clone();
        Ok(Response::new(LeaderResponse {
            leader_addr: Some(leader_addr),
        }))
    }
}
```

Also see the other services in `src/<component>/src/rcp/service/` for further examples. Above service defines a `LeaderRequest` and a `LeaderResponse`, which use proto messages. We have to define these proto messages

## Proto definitions

Add definition in `proto/meta.proto`. For above service I added 

```
message LeaderRequest {}

message LeaderResponse {
  common.HostAddress leaderAddr = 1;
}

service LeaderService {
  rpc Leader(LeaderRequest) returns (LeaderResponse);
}
```

Make sure to lint your file using [buf](https://docs.buf.build/installation)

## Use service 

To use your service you need to generate some boilerplate code, like e.g. the service definition. 

Add your module in `src/prost/src/lib.rs`, like so: 

```rust
#[cfg_attr(madsim, path = "sim/leader.rs")]
pub mod leader;
#[rustfmt::skip]
```

Add your proto file `"leader"` in `src/prost/build.rs`.

Add the module in `src/meta/src/rpc/service/mod.rs`.

Use your service in `src/meta/src/rpc/server.rs`, like 

```rust
let leader_srv = LeaderServiceImpl::new(f_leader_svc_leader_rx);
tokio::spawn(async move {
     tonic::transport::Server::builder()
         .layer(MetricsMiddlewareLayer::new(meta_metrics.clone()))
         .add_service(LeaderServiceServer::new(leader_srv))
         .serve(address_info.listen_addr)
         .await
         .unwrap();
});
```

Running `risedev d` should generate the needed boilerplate code

