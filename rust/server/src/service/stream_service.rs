use crate::stream::StreamManager;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use risingwave_proto::stream_service::{
    ActorInfoTable, BroadcastActorInfoTableResponse, CreateActorRequest, CreateActorResponse,
    DropActorRequest, DropActorResponse,
};
use risingwave_proto::stream_service_grpc::StreamService;
use std::sync::Arc;

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<StreamManager>,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<StreamManager>) -> Self {
        StreamServiceImpl { mgr }
    }
}

impl StreamService for StreamServiceImpl {
    fn create_actor(
        &mut self,
        ctx: RpcContext,
        req: CreateActorRequest,
        sink: UnarySink<CreateActorResponse>,
    ) {
        let fragment = req.get_fragment().clone();
        let res = self.mgr.create_fragment(fragment);
        ctx.spawn(async move {
            match res {
                Err(e) => {
                    error!("failed to create stream actor {}", e);
                    sink.fail(RpcStatus::with_message(
                        RpcStatusCode::INTERNAL,
                        e.to_string(),
                    ));
                }
                Ok(()) => {
                    sink.success(CreateActorResponse::default());
                }
            }
        });
    }

    fn drop_actor(
        &mut self,
        _ctx: RpcContext,
        _req: DropActorRequest,
        _sink: UnarySink<DropActorResponse>,
    ) {
        todo!()
    }

    fn broadcast_actor_info_table(
        &mut self,
        _ctx: RpcContext,
        _req: ActorInfoTable,
        _sink: UnarySink<BroadcastActorInfoTableResponse>,
    ) {
        todo!()
    }
}
