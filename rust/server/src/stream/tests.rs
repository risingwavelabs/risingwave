use futures::SinkExt;
use pb_construct::make_proto;
use risingwave_proto::stream_plan::*;
use risingwave_proto::stream_service::*;
use risingwave_proto::task_service::HostAddress;

use crate::stream_op::Message;

use super::*;

fn helper_make_local_actor(fragment_id: u32) -> ActorInfo {
    make_proto!(ActorInfo, {
      fragment_id: fragment_id,
      host: make_proto!(HostAddress, {
        host: "127.0.0.1".into(),
        port: 2333
      })
    })
}

/// This test creates stream plan protos and feed them into `StreamManager`.
/// There are 5 actors in total, where:
/// * 1 = mock source
/// * 3 = pipe with RR dispatcher
/// * 7, 11 = pipe after dispatcher
/// * 13 = pipe merger
/// * 233 = mock sink
///
/// ```plain
///            /--- 7  ---\
/// 1 --- 3 ---            --- 13 --- 233
///            \--- 11 ---/
/// ```
#[tokio::test]
async fn test_stream_proto() {
    let stream_manager = StreamManager::new();
    let info = [1, 3, 7, 11, 13, 233]
        .iter()
        .cloned()
        .map(helper_make_local_actor)
        .collect::<Vec<_>>()
        .into();
    stream_manager
        .update_actor_info(make_proto!(ActorInfoTable, { info: info }))
        .unwrap();
    // create 0 -> (1) -> 3
    stream_manager
        .create_fragment(make_proto!(StreamFragment, {
          fragment_id: 1,
          nodes: make_proto!(StreamNode, {
            node_type: StreamNode_StreamNodeType::PROJECTION
          }),
          upstream_fragment_id: vec![0].into(),
          dispatcher: make_proto!(Dispatcher, {
            field_type: Dispatcher_DispatcherType::ROUND_ROBIN
          }),
          downstream_fragment_id: vec![3].into()
        }))
        .unwrap();
    // create 1 -> (3) -> 7, 11
    stream_manager
        .create_fragment(make_proto!(StreamFragment, {
          fragment_id: 3,
          nodes: make_proto!(StreamNode, {
            node_type: StreamNode_StreamNodeType::PROJECTION
          }),
          upstream_fragment_id: vec![1].into(),
          dispatcher: make_proto!(Dispatcher, {
            field_type: Dispatcher_DispatcherType::ROUND_ROBIN
          }),
          downstream_fragment_id: vec![7, 11].into()
        }))
        .unwrap();
    // create 3 -> (7) -> 13
    stream_manager
        .create_fragment(make_proto!(StreamFragment, {
          fragment_id: 7,
          nodes: make_proto!(StreamNode, {
            node_type: StreamNode_StreamNodeType::PROJECTION
          }),
          upstream_fragment_id: vec![3].into(),
          dispatcher: make_proto!(Dispatcher, {
            field_type: Dispatcher_DispatcherType::SIMPLE
          }),
          downstream_fragment_id: vec![13].into()
        }))
        .unwrap();
    // create 3 -> (11) -> 13
    stream_manager
        .create_fragment(make_proto!(StreamFragment, {
          fragment_id: 11,
          nodes: make_proto!(StreamNode, {
            node_type: StreamNode_StreamNodeType::PROJECTION
          }),
          upstream_fragment_id: vec![3].into(),
          dispatcher: make_proto!(Dispatcher, {
            field_type: Dispatcher_DispatcherType::SIMPLE
          }),
          downstream_fragment_id: vec![13].into()
        }))
        .unwrap();
    // create 7, 11 -> (13) -> 233
    stream_manager
        .create_fragment(make_proto!(StreamFragment, {
          fragment_id: 13,
          nodes: make_proto!(StreamNode, {
            node_type: StreamNode_StreamNodeType::PROJECTION
          }),
          upstream_fragment_id: vec![7, 11].into(),
          dispatcher: make_proto!(Dispatcher, {
            field_type: Dispatcher_DispatcherType::SIMPLE
          }),
          downstream_fragment_id: vec![233].into()
        }))
        .unwrap();

    let mut source = stream_manager.take_source();
    let _sink = stream_manager.take_sink();

    source.send(Message::Terminate).await.unwrap();

    // TODO: merger should passthrough terminate message, but this is not yet implemented
    // assert!(matches!(sink.next().await.unwrap(), Message::Terminate));

    stream_manager.wait_all().await.unwrap();
}
