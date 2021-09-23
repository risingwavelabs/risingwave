use crate::stream::StreamManager;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use risingwave_proto::stream_service::{
    ActorInfoTable, BroadcastActorInfoTableResponse, BuildFragmentRequest, BuildFragmentResponse,
    UpdateFragmentRequest, UpdateFragmentResponse,
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
    fn update_fragment(
        &mut self,
        ctx: RpcContext,
        req: UpdateFragmentRequest,
        sink: UnarySink<UpdateFragmentResponse>,
    ) {
        let fragment = req.get_fragment();
        let res = self.mgr.update_fragment(fragment);
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
                    sink.success(UpdateFragmentResponse::default());
                }
            }
        });
    }

    fn build_fragment(
        &mut self,
        _ctx: RpcContext,
        _req: BuildFragmentRequest,
        _sink: UnarySink<BuildFragmentResponse>,
    ) {
        todo!()
    }

    fn broadcast_actor_info_table(
        &mut self,
        ctx: RpcContext,
        req: ActorInfoTable,
        sink: UnarySink<BroadcastActorInfoTableResponse>,
    ) {
        let res = self.mgr.update_actor_info(req);
        ctx.spawn(async move {
            match res {
                Err(e) => {
                    error!("failed to update actor info table actor {}", e);
                    sink.fail(RpcStatus::with_message(
                        RpcStatusCode::INTERNAL,
                        e.to_string(),
                    ));
                }
                Ok(()) => {
                    sink.success(BroadcastActorInfoTableResponse::default());
                }
            }
        });
    }
}
