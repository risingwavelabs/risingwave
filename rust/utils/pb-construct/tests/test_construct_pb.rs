mod proto;

use proto::gen::message::OuterEnumProto;
use proto::gen::message::OuterMessageProto;
use proto::gen::message::SubMessageProto;

use pb_construct::make_proto;

#[test]
fn test_into_pb() {
    let generated_proto = make_proto!(OuterMessageProto, {
      float64_data: 1.0,
      float32_data: 2.0,
      int32_data: 3,
      int64_data: 4,
      uint32_data: 5,
      uint64_data: 6,
      bool_data: false,
      string_data: "def".to_string(),
      bytes_data: vec![0x07, 0x05],
      message_data: make_proto!(SubMessageProto, {
        inner_id: 5,
        inner_name: "abc".to_string()
      }),
      enum_data: OuterEnumProto::ERROR
    });

    let expected_out_msg_proto = {
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

        outer_msg_proto
    };

    assert_eq!(generated_proto, expected_out_msg_proto);
}
