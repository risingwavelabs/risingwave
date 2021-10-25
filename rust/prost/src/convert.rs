use bytes::{buf::IntoBuf, Bytes};

pub trait ToProst: protobuf::Message {
    fn to_prost<T: prost::Message + Default>(&self) -> T;
}

impl<F: protobuf::Message> ToProst for F {
    fn to_prost<T: prost::Message + Default>(&self) -> T {
        let bytes = self.write_to_bytes().unwrap();
        T::decode(Bytes::from(bytes.as_slice()).into_buf()).unwrap()
    }
}

pub trait ToProto: prost::Message {
    fn to_proto<T: protobuf::Message>(&self) -> T;
}

impl<F: prost::Message> ToProto for F {
    fn to_proto<T: protobuf::Message>(&self) -> T {
        T::parse_from_bytes(self.encode_to_vec().as_slice()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::ToProst;
    use crate::ToProto;

    #[test]
    fn test_to_prost() {
        let mut id = risingwave_proto::plan::DatabaseRefId::new();
        id.database_id = 3;
        let new_id = id.to_prost::<crate::plan::DatabaseRefId>();
        assert_eq!(new_id.database_id, 3);
    }

    #[test]
    fn test_to_proto() {
        let id = crate::plan::DatabaseRefId { database_id: 3 };
        let new_id = id.to_proto::<risingwave_proto::plan::DatabaseRefId>();
        assert_eq!(new_id.database_id, 3);
    }
}
