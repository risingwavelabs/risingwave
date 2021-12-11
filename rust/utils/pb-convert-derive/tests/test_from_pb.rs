mod proto;

use pb_convert::FromProtobuf;
use proto::gen::message::{OuterEnumProto, OuterMessageProto, SubMessageProto};

use crate::proto::{OuterEnum, OuterMessage, SubMessage};

#[test]
fn test_from_pb() {
    let mut sub_msg_proto = SubMessageProto::new();
    sub_msg_proto.set_inner_id(5);
    sub_msg_proto.set_inner_name("abc".to_string());

    let mut outer_msg_proto = OuterMessageProto::new();
    outer_msg_proto.set_float64_data(1.0);
    outer_msg_proto.set_float32_data(2.0);
    outer_msg_proto.set_int32_data(3);
    outer_msg_proto.set_int64_data(4);
    outer_msg_proto.set_uint32_data(5);
    outer_msg_proto.set_uint64_data(6);
    outer_msg_proto.set_bool_data(false);
    outer_msg_proto.set_string_data("def".to_string());
    outer_msg_proto.set_bytes_data(vec![0x07, 0x05]);
    outer_msg_proto.set_message_data(sub_msg_proto);
    outer_msg_proto.set_enum_data(OuterEnumProto::ERROR);

    let outer_msg = OuterMessage::from_protobuf(&outer_msg_proto)
        .expect("Failed to parse outer message from pb!");

    let expected_outer_msg = OuterMessage {
        float64_data: 1.0,
        float32_data: 2.0,
        int32_data: 3,
        int64_data: 4,
        uint32_data: 5,
        uint64_data: 6,
        bool_data: false,
        string_data: "def".to_string(),
        bytes: vec![0x07, 0x05],
        sub_message: SubMessage {
            id: 5,
            name: "abc".to_string(),
            other: "".to_string(),
        },
        enum_data: OuterEnum::ERROR2,
        other_skip: false,
    };

    assert_eq!(expected_outer_msg, outer_msg);
}
