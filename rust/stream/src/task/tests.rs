use std::collections::HashSet;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use risingwave_common::worker_id::WorkerIdRef;
use risingwave_pb::common::{ActorInfo, HostAddress};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan::ColumnDesc;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::*;
use risingwave_pb::stream_service::*;
use risingwave_source::MemSourceManager;
use risingwave_storage::table::SimpleTableManager;

use super::*;
use crate::executor::{Barrier, Message, Mutation};
use crate::task::env::StreamTaskEnv;

fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
    ActorInfo {
        actor_id,
        host: Some(HostAddress {
            host: LOCAL_TEST_ADDR.ip().to_string(),
            port: LOCAL_TEST_ADDR.port() as i32,
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
    let stream_manager = StreamManager::for_test();
    let info = [1, 3, 7, 11, 13, 233]
        .iter()
        .cloned()
        .map(helper_make_local_actor)
        .collect::<Vec<_>>();
    stream_manager
        .update_actor_info(BroadcastActorInfoTableRequest { info })
        .unwrap();

    stream_manager
        .update_actors(&[
            // create 0 -> (1) -> 3
            StreamActor {
                actor_id: 1,
                fragment_id: 1,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: vec![StreamNode {
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: vec![0],
                            input_column_descs: vec![ColumnDesc {
                                column_type: Some(DataType {
                                    type_name: TypeName::Int32 as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        })),
                        input: vec![],
                        pk_indices: vec![],
                        operator_id: 1,
                    }],
                    pk_indices: vec![],
                    operator_id: 2,
                }),
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Hash as i32,
                    column_indices: vec![0],
                }),
                downstream_actor_id: vec![3],
                upstream_actor_id: vec![0],
            },
            // create 1 -> (3) -> 7, 11
            StreamActor {
                actor_id: 3,
                fragment_id: 1,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: vec![StreamNode {
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: vec![1],
                            input_column_descs: vec![ColumnDesc {
                                column_type: Some(DataType {
                                    type_name: TypeName::Int32 as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        })),
                        input: vec![],
                        pk_indices: vec![],
                        operator_id: 3,
                    }],
                    pk_indices: vec![],
                    operator_id: 4,
                }),
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Hash as i32,
                    column_indices: vec![0],
                }),
                downstream_actor_id: vec![7, 11],
                upstream_actor_id: vec![1],
            },
            // create 3 -> (7) -> 13
            StreamActor {
                actor_id: 7,
                fragment_id: 2,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: vec![StreamNode {
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: vec![3],
                            input_column_descs: vec![ColumnDesc {
                                column_type: Some(DataType {
                                    type_name: TypeName::Int32 as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        })),
                        input: vec![],
                        pk_indices: vec![],
                        operator_id: 5,
                    }],
                    pk_indices: vec![],
                    operator_id: 6,
                }),
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Hash as i32,
                    column_indices: vec![0],
                }),
                downstream_actor_id: vec![13],
                upstream_actor_id: vec![3],
            },
            // create 3 -> (11) -> 13
            StreamActor {
                actor_id: 11,
                fragment_id: 2,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: vec![StreamNode {
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: vec![3],
                            input_column_descs: vec![ColumnDesc {
                                column_type: Some(DataType {
                                    type_name: TypeName::Int32 as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        })),
                        input: vec![],
                        pk_indices: vec![],
                        operator_id: 7,
                    }],
                    pk_indices: vec![],
                    operator_id: 8,
                }),
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Simple as i32,
                    ..Default::default()
                }),
                downstream_actor_id: vec![13],
                upstream_actor_id: vec![3],
            },
            // create 7, 11 -> (13) -> 233
            StreamActor {
                actor_id: 13,
                fragment_id: 3,
                nodes: Some(StreamNode {
                    node: Some(Node::ProjectNode(ProjectNode::default())),
                    input: vec![StreamNode {
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: vec![7, 11],
                            input_column_descs: vec![ColumnDesc {
                                column_type: Some(DataType {
                                    type_name: TypeName::Int32 as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                        })),
                        input: vec![],
                        pk_indices: vec![],
                        operator_id: 9,
                    }],
                    pk_indices: vec![],
                    operator_id: 10,
                }),
                dispatcher: Some(Dispatcher {
                    r#type: dispatcher::DispatcherType::Simple as i32,
                    ..Default::default()
                }),
                downstream_actor_id: vec![233],
                upstream_actor_id: vec![11],
            },
        ])
        .unwrap();

    let env = StreamTaskEnv::new(
        Arc::new(SimpleTableManager::with_in_memory_store()),
        Arc::new(MemSourceManager::new()),
        std::net::SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
        WorkerIdRef::for_test(),
    );
    stream_manager
        .build_actors(&[1, 3, 7, 11, 13], env)
        .unwrap();

    let mut source = stream_manager.take_source();
    let mut sink = stream_manager.take_sink((13, 233));

    let consumer = tokio::spawn(async move {
        for _epoch in 0..100 {
            assert!(matches!(
                sink.next().await.unwrap(),
                Message::Barrier(Barrier { mutation: None, .. })
            ));
        }
        assert!(matches!(
          sink.next().await.unwrap(),
          Message::Barrier(Barrier {
            epoch: 114514,
            mutation,
            ..
          }) if mutation.as_deref().unwrap().is_stop()
        ));
    });

    let timeout = tokio::time::Duration::from_millis(10);

    for epoch in 0..100 {
        tokio::time::timeout(
            timeout,
            source.send(Message::Barrier(Barrier {
                epoch,
                ..Barrier::default()
            })),
        )
        .await
        .expect("timeout while sending barrier message")
        .unwrap();
    }

    tokio::time::timeout(
        timeout,
        source.send(Message::Barrier(Barrier::new(114514).with_mutation(
            Mutation::Stop(HashSet::from([1, 3, 7, 11, 13, 233])),
        ))),
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
