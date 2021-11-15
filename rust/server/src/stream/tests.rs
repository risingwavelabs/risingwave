use futures::SinkExt;
use futures::StreamExt;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::Barrier;
use risingwave_pb::data::DataType;
use risingwave_pb::plan::ColumnDesc;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::*;
use risingwave_pb::stream_service::*;
use risingwave_pb::task_service::HostAddress;
use std::sync::Arc;

use crate::storage::SimpleTableManager;
use crate::stream_op::Message;
use risingwave_common::util::addr::get_host_port;

use super::*;

const PORT: i32 = 2333;

fn helper_make_local_actor(fragment_id: u32) -> ActorInfo {
    ActorInfo {
        fragment_id,
        host: Some(HostAddress {
            host: "127.0.0.1".into(),
            port: PORT,
        }),
    }
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
    let socket_addr = get_host_port(&format!("127.0.0.1:{}", PORT)).unwrap();
    let stream_manager = StreamManager::new(socket_addr);
    let info = [1, 3, 7, 11, 13, 233]
        .iter()
        .cloned()
        .map(helper_make_local_actor)
        .collect::<Vec<_>>();
    stream_manager
        .update_actor_info(BroadcastActorInfoTableRequest { info })
        .unwrap();

    stream_manager
        .update_fragment(&[
            // create 0 -> (1) -> 3
            StreamFragment {
                fragment_id: 1,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: None,
                }),
                upstream_fragment_id: vec![0],
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::RoundRobin as i32,
                    column_idx: 0,
                }),
                downstream_fragment_id: vec![3],
                input_column_descs: vec![ColumnDesc {
                    column_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
            // create 1 -> (3) -> 7, 11
            StreamFragment {
                fragment_id: 3,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: None,
                }),
                upstream_fragment_id: vec![1],
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::RoundRobin as i32,
                    column_idx: 0,
                }),
                downstream_fragment_id: vec![7, 11],
                input_column_descs: vec![ColumnDesc {
                    column_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
            // create 3 -> (7) -> 13
            StreamFragment {
                fragment_id: 7,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: None,
                }),
                upstream_fragment_id: vec![3],
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::RoundRobin as i32,
                    column_idx: 0,
                }),
                downstream_fragment_id: vec![13],
                input_column_descs: vec![ColumnDesc {
                    column_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
            // create 3 -> (11) -> 13
            StreamFragment {
                fragment_id: 11,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: None,
                }),
                upstream_fragment_id: vec![3],
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Simple as i32,
                    column_idx: 0,
                }),
                downstream_fragment_id: vec![13],
                input_column_descs: vec![ColumnDesc {
                    column_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
            // create 7, 11 -> (13) -> 233
            StreamFragment {
                fragment_id: 13,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: None,
                }),
                upstream_fragment_id: vec![7, 11],
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Simple as i32,
                    column_idx: 0,
                }),
                downstream_fragment_id: vec![233],
                input_column_descs: vec![ColumnDesc {
                    column_type: Some(DataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
        ])
        .unwrap();

    let table_manager = Arc::new(SimpleTableManager::new());
    stream_manager
        .build_fragment(&[1, 3, 7, 11, 13], table_manager)
        .unwrap();

    let mut source = stream_manager.take_source();
    let mut sink = stream_manager.take_sink((13, 233));

    let consumer = tokio::spawn(async move {
        for _epoch in 0..100 {
            assert!(matches!(
                sink.next().await.unwrap(),
                Message::Barrier(Barrier {
                    epoch: _,
                    stop: false
                })
            ));
        }
        assert!(matches!(
            sink.next().await.unwrap(),
            Message::Barrier(Barrier {
                epoch: 0,
                stop: true
            })
        ));
    });

    let timeout = tokio::time::Duration::from_millis(10);

    for epoch in 0..100 {
        tokio::time::timeout(
            timeout,
            source.send(Message::Barrier(Barrier { epoch, stop: false })),
        )
        .await
        .expect("timeout while sending barrier message")
        .unwrap();
    }

    tokio::time::timeout(
        timeout,
        source.send(Message::Barrier(Barrier {
            epoch: 0,
            stop: true,
        })),
    )
    .await
    .expect("timeout while sending terminate message")
    .unwrap();

    tokio::time::timeout(timeout, consumer)
        .await
        .expect("timeout while waiting for sink")
        .unwrap();

    tokio::time::timeout(timeout, stream_manager.wait_all())
        .await
        .expect("timeout while waiting for processor stop")
        .unwrap();
}
