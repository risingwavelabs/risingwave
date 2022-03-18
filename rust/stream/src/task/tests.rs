use std::collections::HashSet;

use futures::{SinkExt, StreamExt};
use risingwave_common::hash::VIRTUAL_KEY_COUNT;
use risingwave_pb::common::{ActorInfo, HashMapping, HostAddress};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan::Field;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::*;
use risingwave_pb::stream_service::*;

use super::*;
use crate::executor::{Barrier, Epoch, Message, Mutation};
use crate::task::env::StreamEnvironment;

fn helper_make_local_actor(actor_id: ActorId) -> ActorInfo {
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
        .update_actors(
            &[
                // create 0 -> (1) -> 3
                StreamActor {
                    actor_id: 1,
                    fragment_id: 1,
                    nodes: Some(StreamNode {
                        node: Some(Node::ProjectNode(ProjectNode::default())),
                        input: vec![StreamNode {
                            node: Some(Node::MergeNode(MergeNode {
                                upstream_actor_id: vec![0],
                                fields: vec![Field {
                                    data_type: Some(DataType {
                                        type_name: TypeName::Int32 as i32,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }],
                            })),
                            input: vec![],
                            pk_indices: vec![],
                            operator_id: 1,
                            identity: "MergeExecutor".to_string(),
                        }],
                        pk_indices: vec![],
                        operator_id: 2,
                        identity: "ProjectExecutor".to_string(),
                    }),
                    dispatcher: vec![Dispatcher {
                        r#type: DispatcherType::Hash as i32,
                        column_indices: vec![0],
                        hash_mapping: Some(HashMapping {
                            hash_mapping: vec![3; VIRTUAL_KEY_COUNT],
                        }),
                        downstream_actor_id: vec![3],
                    }],
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
                                fields: vec![Field {
                                    data_type: Some(DataType {
                                        type_name: TypeName::Int32 as i32,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }],
                            })),
                            input: vec![],
                            pk_indices: vec![],
                            operator_id: 3,
                            identity: "MergeExecutor".to_string(),
                        }],
                        pk_indices: vec![],
                        operator_id: 4,
                        identity: "ProjectExecutor".to_string(),
                    }),
                    dispatcher: vec![Dispatcher {
                        r#type: DispatcherType::Hash as i32,
                        column_indices: vec![0],
                        hash_mapping: {
                            let mut hash_mapping = vec![7; VIRTUAL_KEY_COUNT / 2];
                            hash_mapping.resize(VIRTUAL_KEY_COUNT, 11);
                            Some(HashMapping { hash_mapping })
                        },
                        downstream_actor_id: vec![7, 11],
                    }],
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
                                fields: vec![Field {
                                    data_type: Some(DataType {
                                        type_name: TypeName::Int32 as i32,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }],
                            })),
                            input: vec![],
                            pk_indices: vec![],
                            operator_id: 5,
                            identity: "MergeExecutor".to_string(),
                        }],
                        pk_indices: vec![],
                        operator_id: 6,
                        identity: "ProjectExecutor".to_string(),
                    }),
                    dispatcher: vec![Dispatcher {
                        r#type: DispatcherType::Hash as i32,
                        column_indices: vec![0],
                        hash_mapping: Some(HashMapping {
                            hash_mapping: vec![13; VIRTUAL_KEY_COUNT],
                        }),
                        downstream_actor_id: vec![13],
                    }],
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
                                fields: vec![Field {
                                    data_type: Some(DataType {
                                        type_name: TypeName::Int32 as i32,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }],
                            })),
                            input: vec![],
                            pk_indices: vec![],
                            operator_id: 7,
                            identity: "MergeExecutor".to_string(),
                        }],
                        pk_indices: vec![],
                        operator_id: 8,
                        identity: "ProjectExecutor".to_string(),
                    }),
                    dispatcher: vec![Dispatcher {
                        r#type: DispatcherType::Simple as i32,
                        downstream_actor_id: vec![13],
                        ..Default::default()
                    }],
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
                                fields: vec![Field {
                                    data_type: Some(DataType {
                                        type_name: TypeName::Int32 as i32,
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                }],
                            })),
                            input: vec![],
                            pk_indices: vec![],
                            operator_id: 9,
                            identity: "MergeExecutor".to_string(),
                        }],
                        pk_indices: vec![],
                        operator_id: 10,
                        identity: "ProjectExecutor".to_string(),
                    }),
                    dispatcher: vec![Dispatcher {
                        r#type: DispatcherType::Simple as i32,
                        downstream_actor_id: vec![233],
                        ..Default::default()
                    }],
                    upstream_actor_id: vec![11],
                },
            ],
            &[],
        )
        .unwrap();

    let env = StreamEnvironment::for_test();
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
        let barrier_epoch = Epoch::new_test_epoch(114514);
        assert!(matches!(
            sink.next().await.unwrap(),
            Message::Barrier(Barrier {
                epoch,
                mutation,
                ..
            }) if mutation.as_deref().unwrap().is_stop() && epoch == barrier_epoch
        ));
    });

    let timeout = tokio::time::Duration::from_millis(10);

    for epoch in 0..100 {
        tokio::time::timeout(
            timeout,
            source.send(Message::Barrier(Barrier::new_test_barrier(epoch + 1))),
        )
        .await
        .expect("timeout while sending barrier message")
        .unwrap();
    }

    tokio::time::timeout(
        timeout,
        source.send(Message::Barrier(
            Barrier::new_test_barrier(114514)
                .with_mutation(Mutation::Stop(HashSet::from([1, 3, 7, 11, 13, 233]))),
        )),
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
