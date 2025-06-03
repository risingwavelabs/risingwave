use crate::common::*;
impl serde::Serialize for ActorInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.actor_id != 0 {
            len += 1;
        }
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.ActorInfo", len)?;
        if self.actor_id != 0 {
            struct_ser.serialize_field("actorId", &self.actor_id)?;
        }
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActorInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "actor_id",
            "actorId",
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ActorId,
            Host,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "actorId" | "actor_id" => Ok(GeneratedField::ActorId),
                            "host" => Ok(GeneratedField::Host),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ActorInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.ActorInfo")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ActorInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut actor_id__ = None;
                let mut host__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ActorId => {
                            if actor_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorId"));
                            }
                            actor_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ActorInfo {
                    actor_id: actor_id__.unwrap_or_default(),
                    host: host__,
                })
            }
        }
        deserializer.deserialize_struct("common.ActorInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ActorLocation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.worker_node_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.ActorLocation", len)?;
        if self.worker_node_id != 0 {
            struct_ser.serialize_field("workerNodeId", &self.worker_node_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActorLocation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "worker_node_id",
            "workerNodeId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorkerNodeId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "workerNodeId" | "worker_node_id" => Ok(GeneratedField::WorkerNodeId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ActorLocation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.ActorLocation")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ActorLocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worker_node_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::WorkerNodeId => {
                            if worker_node_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("workerNodeId"));
                            }
                            worker_node_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ActorLocation {
                    worker_node_id: worker_node_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.ActorLocation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BatchQueryCommittedEpoch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.epoch != 0 {
            len += 1;
        }
        if self.hummock_version_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.BatchQueryCommittedEpoch", len)?;
        if self.epoch != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        if self.hummock_version_id != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hummockVersionId", ToString::to_string(&self.hummock_version_id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BatchQueryCommittedEpoch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
            "hummock_version_id",
            "hummockVersionId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Epoch,
            HummockVersionId,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "epoch" => Ok(GeneratedField::Epoch),
                            "hummockVersionId" | "hummock_version_id" => Ok(GeneratedField::HummockVersionId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BatchQueryCommittedEpoch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.BatchQueryCommittedEpoch")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BatchQueryCommittedEpoch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch__ = None;
                let mut hummock_version_id__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Epoch => {
                            if epoch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("epoch"));
                            }
                            epoch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::HummockVersionId => {
                            if hummock_version_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hummockVersionId"));
                            }
                            hummock_version_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(BatchQueryCommittedEpoch {
                    epoch: epoch__.unwrap_or_default(),
                    hummock_version_id: hummock_version_id__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.BatchQueryCommittedEpoch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BatchQueryEpoch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.epoch.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.BatchQueryEpoch", len)?;
        if let Some(v) = self.epoch.as_ref() {
            match v {
                batch_query_epoch::Epoch::Committed(v) => {
                    struct_ser.serialize_field("committed", v)?;
                }
                batch_query_epoch::Epoch::Current(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("current", ToString::to_string(&v).as_str())?;
                }
                batch_query_epoch::Epoch::Backup(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("backup", ToString::to_string(&v).as_str())?;
                }
                batch_query_epoch::Epoch::TimeTravel(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("timeTravel", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BatchQueryEpoch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "committed",
            "current",
            "backup",
            "time_travel",
            "timeTravel",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Committed,
            Current,
            Backup,
            TimeTravel,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "committed" => Ok(GeneratedField::Committed),
                            "current" => Ok(GeneratedField::Current),
                            "backup" => Ok(GeneratedField::Backup),
                            "timeTravel" | "time_travel" => Ok(GeneratedField::TimeTravel),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BatchQueryEpoch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.BatchQueryEpoch")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BatchQueryEpoch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Committed => {
                            if epoch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("committed"));
                            }
                            epoch__ = map_.next_value::<::std::option::Option<_>>()?.map(batch_query_epoch::Epoch::Committed)
;
                        }
                        GeneratedField::Current => {
                            if epoch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("current"));
                            }
                            epoch__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| batch_query_epoch::Epoch::Current(x.0));
                        }
                        GeneratedField::Backup => {
                            if epoch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("backup"));
                            }
                            epoch__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| batch_query_epoch::Epoch::Backup(x.0));
                        }
                        GeneratedField::TimeTravel => {
                            if epoch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeTravel"));
                            }
                            epoch__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| batch_query_epoch::Epoch::TimeTravel(x.0));
                        }
                    }
                }
                Ok(BatchQueryEpoch {
                    epoch: epoch__,
                })
            }
        }
        deserializer.deserialize_struct("common.BatchQueryEpoch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Buffer {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compression != 0 {
            len += 1;
        }
        if !self.body.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.Buffer", len)?;
        if self.compression != 0 {
            let v = buffer::CompressionType::try_from(self.compression)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        if !self.body.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("body", pbjson::private::base64::encode(&self.body).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Buffer {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compression",
            "body",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Compression,
            Body,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "compression" => Ok(GeneratedField::Compression),
                            "body" => Ok(GeneratedField::Body),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Buffer;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.Buffer")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Buffer, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compression__ = None;
                let mut body__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Compression => {
                            if compression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression__ = Some(map_.next_value::<buffer::CompressionType>()? as i32);
                        }
                        GeneratedField::Body => {
                            if body__.is_some() {
                                return Err(serde::de::Error::duplicate_field("body"));
                            }
                            body__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Buffer {
                    compression: compression__.unwrap_or_default(),
                    body: body__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.Buffer", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for buffer::CompressionType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::None => "NONE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for buffer::CompressionType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "NONE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = buffer::CompressionType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "UNSPECIFIED" => Ok(buffer::CompressionType::Unspecified),
                    "NONE" => Ok(buffer::CompressionType::None),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnOrder {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column_index != 0 {
            len += 1;
        }
        if self.order_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.ColumnOrder", len)?;
        if self.column_index != 0 {
            struct_ser.serialize_field("columnIndex", &self.column_index)?;
        }
        if let Some(v) = self.order_type.as_ref() {
            struct_ser.serialize_field("orderType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnOrder {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column_index",
            "columnIndex",
            "order_type",
            "orderType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnIndex,
            OrderType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columnIndex" | "column_index" => Ok(GeneratedField::ColumnIndex),
                            "orderType" | "order_type" => Ok(GeneratedField::OrderType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnOrder;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.ColumnOrder")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnOrder, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_index__ = None;
                let mut order_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ColumnIndex => {
                            if column_index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndex"));
                            }
                            column_index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::OrderType => {
                            if order_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderType"));
                            }
                            order_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ColumnOrder {
                    column_index: column_index__.unwrap_or_default(),
                    order_type: order_type__,
                })
            }
        }
        deserializer.deserialize_struct("common.ColumnOrder", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Direction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "DIRECTION_UNSPECIFIED",
            Self::Ascending => "DIRECTION_ASCENDING",
            Self::Descending => "DIRECTION_DESCENDING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Direction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "DIRECTION_UNSPECIFIED",
            "DIRECTION_ASCENDING",
            "DIRECTION_DESCENDING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Direction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "DIRECTION_UNSPECIFIED" => Ok(Direction::Unspecified),
                    "DIRECTION_ASCENDING" => Ok(Direction::Ascending),
                    "DIRECTION_DESCENDING" => Ok(Direction::Descending),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for HostAddress {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.host.is_empty() {
            len += 1;
        }
        if self.port != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.HostAddress", len)?;
        if !self.host.is_empty() {
            struct_ser.serialize_field("host", &self.host)?;
        }
        if self.port != 0 {
            struct_ser.serialize_field("port", &self.port)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HostAddress {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "host",
            "port",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Host,
            Port,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "host" => Ok(GeneratedField::Host),
                            "port" => Ok(GeneratedField::Port),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HostAddress;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.HostAddress")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HostAddress, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut host__ = None;
                let mut port__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Port => {
                            if port__.is_some() {
                                return Err(serde::de::Error::duplicate_field("port"));
                            }
                            port__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(HostAddress {
                    host: host__.unwrap_or_default(),
                    port: port__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.HostAddress", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NullsAre {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "NULLS_ARE_UNSPECIFIED",
            Self::Largest => "NULLS_ARE_LARGEST",
            Self::Smallest => "NULLS_ARE_SMALLEST",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for NullsAre {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NULLS_ARE_UNSPECIFIED",
            "NULLS_ARE_LARGEST",
            "NULLS_ARE_SMALLEST",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NullsAre;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NULLS_ARE_UNSPECIFIED" => Ok(NullsAre::Unspecified),
                    "NULLS_ARE_LARGEST" => Ok(NullsAre::Largest),
                    "NULLS_ARE_SMALLEST" => Ok(NullsAre::Smallest),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for OptionalUint32 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.OptionalUint32", len)?;
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptionalUint32 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptionalUint32;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.OptionalUint32")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OptionalUint32, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(OptionalUint32 {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("common.OptionalUint32", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OptionalUint64 {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.OptionalUint64", len)?;
        if let Some(v) = self.value.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("value", ToString::to_string(&v).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptionalUint64 {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptionalUint64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.OptionalUint64")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OptionalUint64, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(OptionalUint64 {
                    value: value__,
                })
            }
        }
        deserializer.deserialize_struct("common.OptionalUint64", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OrderType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.direction != 0 {
            len += 1;
        }
        if self.nulls_are != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.OrderType", len)?;
        if self.direction != 0 {
            let v = Direction::try_from(self.direction)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.direction)))?;
            struct_ser.serialize_field("direction", &v)?;
        }
        if self.nulls_are != 0 {
            let v = NullsAre::try_from(self.nulls_are)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.nulls_are)))?;
            struct_ser.serialize_field("nullsAre", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OrderType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "direction",
            "nulls_are",
            "nullsAre",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Direction,
            NullsAre,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "direction" => Ok(GeneratedField::Direction),
                            "nullsAre" | "nulls_are" => Ok(GeneratedField::NullsAre),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OrderType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.OrderType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OrderType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut direction__ = None;
                let mut nulls_are__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Direction => {
                            if direction__.is_some() {
                                return Err(serde::de::Error::duplicate_field("direction"));
                            }
                            direction__ = Some(map_.next_value::<Direction>()? as i32);
                        }
                        GeneratedField::NullsAre => {
                            if nulls_are__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullsAre"));
                            }
                            nulls_are__ = Some(map_.next_value::<NullsAre>()? as i32);
                        }
                    }
                }
                Ok(OrderType {
                    direction: direction__.unwrap_or_default(),
                    nulls_are: nulls_are__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.OrderType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Status {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.code != 0 {
            len += 1;
        }
        if !self.message.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.Status", len)?;
        if self.code != 0 {
            let v = status::Code::try_from(self.code)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.code)))?;
            struct_ser.serialize_field("code", &v)?;
        }
        if !self.message.is_empty() {
            struct_ser.serialize_field("message", &self.message)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Status {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "code",
            "message",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Code,
            Message,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "code" => Ok(GeneratedField::Code),
                            "message" => Ok(GeneratedField::Message),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Status;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.Status")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Status, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut code__ = None;
                let mut message__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Code => {
                            if code__.is_some() {
                                return Err(serde::de::Error::duplicate_field("code"));
                            }
                            code__ = Some(map_.next_value::<status::Code>()? as i32);
                        }
                        GeneratedField::Message => {
                            if message__.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Status {
                    code: code__.unwrap_or_default(),
                    message: message__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.Status", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for status::Code {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Ok => "OK",
            Self::UnknownWorker => "UNKNOWN_WORKER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for status::Code {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "OK",
            "UNKNOWN_WORKER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = status::Code;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "UNSPECIFIED" => Ok(status::Code::Unspecified),
                    "OK" => Ok(status::Code::Ok),
                    "UNKNOWN_WORKER" => Ok(status::Code::UnknownWorker),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Uint32Vector {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.Uint32Vector", len)?;
        if !self.data.is_empty() {
            struct_ser.serialize_field("data", &self.data)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Uint32Vector {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Data,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Uint32Vector;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.Uint32Vector")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Uint32Vector, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(Uint32Vector {
                    data: data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.Uint32Vector", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WorkerNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.id != 0 {
            len += 1;
        }
        if self.r#type != 0 {
            len += 1;
        }
        if self.host.is_some() {
            len += 1;
        }
        if self.state != 0 {
            len += 1;
        }
        if self.property.is_some() {
            len += 1;
        }
        if self.transactional_id.is_some() {
            len += 1;
        }
        if self.resource.is_some() {
            len += 1;
        }
        if self.started_at.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.WorkerNode", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if self.r#type != 0 {
            let v = WorkerType::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        if self.state != 0 {
            let v = worker_node::State::try_from(self.state)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.state)))?;
            struct_ser.serialize_field("state", &v)?;
        }
        if let Some(v) = self.property.as_ref() {
            struct_ser.serialize_field("property", v)?;
        }
        if let Some(v) = self.transactional_id.as_ref() {
            struct_ser.serialize_field("transactionalId", v)?;
        }
        if let Some(v) = self.resource.as_ref() {
            struct_ser.serialize_field("resource", v)?;
        }
        if let Some(v) = self.started_at.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("startedAt", ToString::to_string(&v).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WorkerNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "type",
            "host",
            "state",
            "property",
            "transactional_id",
            "transactionalId",
            "resource",
            "started_at",
            "startedAt",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Type,
            Host,
            State,
            Property,
            TransactionalId,
            Resource,
            StartedAt,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "type" => Ok(GeneratedField::Type),
                            "host" => Ok(GeneratedField::Host),
                            "state" => Ok(GeneratedField::State),
                            "property" => Ok(GeneratedField::Property),
                            "transactionalId" | "transactional_id" => Ok(GeneratedField::TransactionalId),
                            "resource" => Ok(GeneratedField::Resource),
                            "startedAt" | "started_at" => Ok(GeneratedField::StartedAt),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WorkerNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.WorkerNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WorkerNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut r#type__ = None;
                let mut host__ = None;
                let mut state__ = None;
                let mut property__ = None;
                let mut transactional_id__ = None;
                let mut resource__ = None;
                let mut started_at__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<WorkerType>()? as i32);
                        }
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = map_.next_value()?;
                        }
                        GeneratedField::State => {
                            if state__.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state__ = Some(map_.next_value::<worker_node::State>()? as i32);
                        }
                        GeneratedField::Property => {
                            if property__.is_some() {
                                return Err(serde::de::Error::duplicate_field("property"));
                            }
                            property__ = map_.next_value()?;
                        }
                        GeneratedField::TransactionalId => {
                            if transactional_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("transactionalId"));
                            }
                            transactional_id__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Resource => {
                            if resource__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resource"));
                            }
                            resource__ = map_.next_value()?;
                        }
                        GeneratedField::StartedAt => {
                            if started_at__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startedAt"));
                            }
                            started_at__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(WorkerNode {
                    id: id__.unwrap_or_default(),
                    r#type: r#type__.unwrap_or_default(),
                    host: host__,
                    state: state__.unwrap_or_default(),
                    property: property__,
                    transactional_id: transactional_id__,
                    resource: resource__,
                    started_at: started_at__,
                })
            }
        }
        deserializer.deserialize_struct("common.WorkerNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for worker_node::Property {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.is_streaming {
            len += 1;
        }
        if self.is_serving {
            len += 1;
        }
        if self.is_unschedulable {
            len += 1;
        }
        if !self.internal_rpc_host_addr.is_empty() {
            len += 1;
        }
        if self.parallelism != 0 {
            len += 1;
        }
        if self.resource_group.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.WorkerNode.Property", len)?;
        if self.is_streaming {
            struct_ser.serialize_field("isStreaming", &self.is_streaming)?;
        }
        if self.is_serving {
            struct_ser.serialize_field("isServing", &self.is_serving)?;
        }
        if self.is_unschedulable {
            struct_ser.serialize_field("isUnschedulable", &self.is_unschedulable)?;
        }
        if !self.internal_rpc_host_addr.is_empty() {
            struct_ser.serialize_field("internalRpcHostAddr", &self.internal_rpc_host_addr)?;
        }
        if self.parallelism != 0 {
            struct_ser.serialize_field("parallelism", &self.parallelism)?;
        }
        if let Some(v) = self.resource_group.as_ref() {
            struct_ser.serialize_field("resourceGroup", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for worker_node::Property {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "is_streaming",
            "isStreaming",
            "is_serving",
            "isServing",
            "is_unschedulable",
            "isUnschedulable",
            "internal_rpc_host_addr",
            "internalRpcHostAddr",
            "parallelism",
            "resource_group",
            "resourceGroup",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            IsStreaming,
            IsServing,
            IsUnschedulable,
            InternalRpcHostAddr,
            Parallelism,
            ResourceGroup,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "isStreaming" | "is_streaming" => Ok(GeneratedField::IsStreaming),
                            "isServing" | "is_serving" => Ok(GeneratedField::IsServing),
                            "isUnschedulable" | "is_unschedulable" => Ok(GeneratedField::IsUnschedulable),
                            "internalRpcHostAddr" | "internal_rpc_host_addr" => Ok(GeneratedField::InternalRpcHostAddr),
                            "parallelism" => Ok(GeneratedField::Parallelism),
                            "resourceGroup" | "resource_group" => Ok(GeneratedField::ResourceGroup),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = worker_node::Property;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.WorkerNode.Property")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<worker_node::Property, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut is_streaming__ = None;
                let mut is_serving__ = None;
                let mut is_unschedulable__ = None;
                let mut internal_rpc_host_addr__ = None;
                let mut parallelism__ = None;
                let mut resource_group__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::IsStreaming => {
                            if is_streaming__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isStreaming"));
                            }
                            is_streaming__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IsServing => {
                            if is_serving__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isServing"));
                            }
                            is_serving__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IsUnschedulable => {
                            if is_unschedulable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isUnschedulable"));
                            }
                            is_unschedulable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InternalRpcHostAddr => {
                            if internal_rpc_host_addr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("internalRpcHostAddr"));
                            }
                            internal_rpc_host_addr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Parallelism => {
                            if parallelism__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parallelism"));
                            }
                            parallelism__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ResourceGroup => {
                            if resource_group__.is_some() {
                                return Err(serde::de::Error::duplicate_field("resourceGroup"));
                            }
                            resource_group__ = map_.next_value()?;
                        }
                    }
                }
                Ok(worker_node::Property {
                    is_streaming: is_streaming__.unwrap_or_default(),
                    is_serving: is_serving__.unwrap_or_default(),
                    is_unschedulable: is_unschedulable__.unwrap_or_default(),
                    internal_rpc_host_addr: internal_rpc_host_addr__.unwrap_or_default(),
                    parallelism: parallelism__.unwrap_or_default(),
                    resource_group: resource_group__,
                })
            }
        }
        deserializer.deserialize_struct("common.WorkerNode.Property", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for worker_node::Resource {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.rw_version.is_empty() {
            len += 1;
        }
        if self.total_memory_bytes != 0 {
            len += 1;
        }
        if self.total_cpu_cores != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.WorkerNode.Resource", len)?;
        if !self.rw_version.is_empty() {
            struct_ser.serialize_field("rwVersion", &self.rw_version)?;
        }
        if self.total_memory_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("totalMemoryBytes", ToString::to_string(&self.total_memory_bytes).as_str())?;
        }
        if self.total_cpu_cores != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("totalCpuCores", ToString::to_string(&self.total_cpu_cores).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for worker_node::Resource {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "rw_version",
            "rwVersion",
            "total_memory_bytes",
            "totalMemoryBytes",
            "total_cpu_cores",
            "totalCpuCores",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RwVersion,
            TotalMemoryBytes,
            TotalCpuCores,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "rwVersion" | "rw_version" => Ok(GeneratedField::RwVersion),
                            "totalMemoryBytes" | "total_memory_bytes" => Ok(GeneratedField::TotalMemoryBytes),
                            "totalCpuCores" | "total_cpu_cores" => Ok(GeneratedField::TotalCpuCores),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = worker_node::Resource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.WorkerNode.Resource")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<worker_node::Resource, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut rw_version__ = None;
                let mut total_memory_bytes__ = None;
                let mut total_cpu_cores__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RwVersion => {
                            if rw_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rwVersion"));
                            }
                            rw_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TotalMemoryBytes => {
                            if total_memory_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalMemoryBytes"));
                            }
                            total_memory_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::TotalCpuCores => {
                            if total_cpu_cores__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalCpuCores"));
                            }
                            total_cpu_cores__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(worker_node::Resource {
                    rw_version: rw_version__.unwrap_or_default(),
                    total_memory_bytes: total_memory_bytes__.unwrap_or_default(),
                    total_cpu_cores: total_cpu_cores__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.WorkerNode.Resource", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for worker_node::State {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for worker_node::State {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "STARTING",
            "RUNNING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = worker_node::State;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "UNSPECIFIED" => Ok(worker_node::State::Unspecified),
                    "STARTING" => Ok(worker_node::State::Starting),
                    "RUNNING" => Ok(worker_node::State::Running),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WorkerSlotMapping {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.original_indices.is_empty() {
            len += 1;
        }
        if !self.data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("common.WorkerSlotMapping", len)?;
        if !self.original_indices.is_empty() {
            struct_ser.serialize_field("originalIndices", &self.original_indices)?;
        }
        if !self.data.is_empty() {
            struct_ser.serialize_field("data", &self.data.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WorkerSlotMapping {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "original_indices",
            "originalIndices",
            "data",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OriginalIndices,
            Data,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "originalIndices" | "original_indices" => Ok(GeneratedField::OriginalIndices),
                            "data" => Ok(GeneratedField::Data),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WorkerSlotMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct common.WorkerSlotMapping")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WorkerSlotMapping, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut original_indices__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OriginalIndices => {
                            if original_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("originalIndices"));
                            }
                            original_indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Data => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("data"));
                            }
                            data__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(WorkerSlotMapping {
                    original_indices: original_indices__.unwrap_or_default(),
                    data: data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("common.WorkerSlotMapping", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WorkerType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "WORKER_TYPE_UNSPECIFIED",
            Self::Frontend => "WORKER_TYPE_FRONTEND",
            Self::ComputeNode => "WORKER_TYPE_COMPUTE_NODE",
            Self::RiseCtl => "WORKER_TYPE_RISE_CTL",
            Self::Compactor => "WORKER_TYPE_COMPACTOR",
            Self::Meta => "WORKER_TYPE_META",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for WorkerType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "WORKER_TYPE_UNSPECIFIED",
            "WORKER_TYPE_FRONTEND",
            "WORKER_TYPE_COMPUTE_NODE",
            "WORKER_TYPE_RISE_CTL",
            "WORKER_TYPE_COMPACTOR",
            "WORKER_TYPE_META",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WorkerType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "WORKER_TYPE_UNSPECIFIED" => Ok(WorkerType::Unspecified),
                    "WORKER_TYPE_FRONTEND" => Ok(WorkerType::Frontend),
                    "WORKER_TYPE_COMPUTE_NODE" => Ok(WorkerType::ComputeNode),
                    "WORKER_TYPE_RISE_CTL" => Ok(WorkerType::RiseCtl),
                    "WORKER_TYPE_COMPACTOR" => Ok(WorkerType::Compactor),
                    "WORKER_TYPE_META" => Ok(WorkerType::Meta),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
