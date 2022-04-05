use crate::stream_service::*;
impl serde::Serialize for BroadcastActorInfoTableRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.info.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.BroadcastActorInfoTableRequest", len)?;
        if !self.info.is_empty() {
            struct_ser.serialize_field("info", &self.info)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BroadcastActorInfoTableRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "info",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Info,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "info" => Ok(GeneratedField::Info),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BroadcastActorInfoTableRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.BroadcastActorInfoTableRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BroadcastActorInfoTableRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut info = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Info => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info = Some(map.next_value()?);
                        }
                    }
                }
                Ok(BroadcastActorInfoTableRequest {
                    info: info.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.BroadcastActorInfoTableRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BroadcastActorInfoTableResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.BroadcastActorInfoTableResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BroadcastActorInfoTableResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BroadcastActorInfoTableResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.BroadcastActorInfoTableResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BroadcastActorInfoTableResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(BroadcastActorInfoTableResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.BroadcastActorInfoTableResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuildActorsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if !self.actor_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.BuildActorsRequest", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if !self.actor_id.is_empty() {
            struct_ser.serialize_field("actorId", &self.actor_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BuildActorsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "actorId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            ActorId,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "actorId" => Ok(GeneratedField::ActorId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuildActorsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.BuildActorsRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BuildActorsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut actor_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::ActorId => {
                            if actor_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorId"));
                            }
                            actor_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(BuildActorsRequest {
                    request_id: request_id.unwrap_or_default(),
                    actor_id: actor_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.BuildActorsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuildActorsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.BuildActorsResponse", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BuildActorsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuildActorsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.BuildActorsResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BuildActorsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(BuildActorsResponse {
                    request_id: request_id.unwrap_or_default(),
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.BuildActorsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateSourceRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.source.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.CreateSourceRequest", len)?;
        if let Some(v) = self.source.as_ref() {
            struct_ser.serialize_field("source", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateSourceRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Source,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "source" => Ok(GeneratedField::Source),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateSourceRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.CreateSourceRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSourceRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Source => {
                            if source.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateSourceRequest {
                    source,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.CreateSourceRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateSourceResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.CreateSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateSourceResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.CreateSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSourceResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateSourceResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.CreateSourceResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropActorsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if !self.actor_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.DropActorsRequest", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.actor_ids.is_empty() {
            struct_ser.serialize_field("actorIds", &self.actor_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropActorsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "tableRefId",
            "actorIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            TableRefId,
            ActorIds,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "actorIds" => Ok(GeneratedField::ActorIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropActorsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.DropActorsRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropActorsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut table_ref_id = None;
                let mut actor_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::ActorIds => {
                            if actor_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorIds"));
                            }
                            actor_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(DropActorsRequest {
                    request_id: request_id.unwrap_or_default(),
                    table_ref_id,
                    actor_ids: actor_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.DropActorsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropActorsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.DropActorsResponse", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropActorsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropActorsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.DropActorsResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropActorsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DropActorsResponse {
                    request_id: request_id.unwrap_or_default(),
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.DropActorsResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropSourceRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.source_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.DropSourceRequest", len)?;
        if self.source_id != 0 {
            struct_ser.serialize_field("sourceId", &self.source_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropSourceRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sourceId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SourceId,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "sourceId" => Ok(GeneratedField::SourceId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropSourceRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.DropSourceRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSourceRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SourceId => {
                            if source_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceId"));
                            }
                            source_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropSourceRequest {
                    source_id: source_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.DropSourceRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropSourceResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.DropSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropSourceResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.DropSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSourceResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DropSourceResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.DropSourceResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HangingChannel {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.upstream.is_some() {
            len += 1;
        }
        if self.downstream.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.HangingChannel", len)?;
        if let Some(v) = self.upstream.as_ref() {
            struct_ser.serialize_field("upstream", v)?;
        }
        if let Some(v) = self.downstream.as_ref() {
            struct_ser.serialize_field("downstream", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HangingChannel {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "upstream",
            "downstream",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Upstream,
            Downstream,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "upstream" => Ok(GeneratedField::Upstream),
                            "downstream" => Ok(GeneratedField::Downstream),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HangingChannel;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.HangingChannel")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HangingChannel, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut upstream = None;
                let mut downstream = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Upstream => {
                            if upstream.is_some() {
                                return Err(serde::de::Error::duplicate_field("upstream"));
                            }
                            upstream = Some(map.next_value()?);
                        }
                        GeneratedField::Downstream => {
                            if downstream.is_some() {
                                return Err(serde::de::Error::duplicate_field("downstream"));
                            }
                            downstream = Some(map.next_value()?);
                        }
                    }
                }
                Ok(HangingChannel {
                    upstream,
                    downstream,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.HangingChannel", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InjectBarrierRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if self.barrier.is_some() {
            len += 1;
        }
        if !self.actor_ids_to_send.is_empty() {
            len += 1;
        }
        if !self.actor_ids_to_collect.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.InjectBarrierRequest", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if let Some(v) = self.barrier.as_ref() {
            struct_ser.serialize_field("barrier", v)?;
        }
        if !self.actor_ids_to_send.is_empty() {
            struct_ser.serialize_field("actorIdsToSend", &self.actor_ids_to_send)?;
        }
        if !self.actor_ids_to_collect.is_empty() {
            struct_ser.serialize_field("actorIdsToCollect", &self.actor_ids_to_collect)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InjectBarrierRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "barrier",
            "actorIdsToSend",
            "actorIdsToCollect",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Barrier,
            ActorIdsToSend,
            ActorIdsToCollect,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "barrier" => Ok(GeneratedField::Barrier),
                            "actorIdsToSend" => Ok(GeneratedField::ActorIdsToSend),
                            "actorIdsToCollect" => Ok(GeneratedField::ActorIdsToCollect),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InjectBarrierRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.InjectBarrierRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<InjectBarrierRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut barrier = None;
                let mut actor_ids_to_send = None;
                let mut actor_ids_to_collect = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::Barrier => {
                            if barrier.is_some() {
                                return Err(serde::de::Error::duplicate_field("barrier"));
                            }
                            barrier = Some(map.next_value()?);
                        }
                        GeneratedField::ActorIdsToSend => {
                            if actor_ids_to_send.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorIdsToSend"));
                            }
                            actor_ids_to_send = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::ActorIdsToCollect => {
                            if actor_ids_to_collect.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorIdsToCollect"));
                            }
                            actor_ids_to_collect = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(InjectBarrierRequest {
                    request_id: request_id.unwrap_or_default(),
                    barrier,
                    actor_ids_to_send: actor_ids_to_send.unwrap_or_default(),
                    actor_ids_to_collect: actor_ids_to_collect.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.InjectBarrierRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InjectBarrierResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.InjectBarrierResponse", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InjectBarrierResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InjectBarrierResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.InjectBarrierResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<InjectBarrierResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(InjectBarrierResponse {
                    request_id: request_id.unwrap_or_default(),
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.InjectBarrierResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShutdownRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("stream_service.ShutdownRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShutdownRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        Err(serde::de::Error::unknown_field(value, FIELDS))
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShutdownRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.ShutdownRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ShutdownRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(ShutdownRequest {
                })
            }
        }
        deserializer.deserialize_struct("stream_service.ShutdownRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ShutdownResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.ShutdownResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ShutdownResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ShutdownResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.ShutdownResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ShutdownResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ShutdownResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.ShutdownResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UpdateActorsRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.request_id.is_empty() {
            len += 1;
        }
        if !self.actors.is_empty() {
            len += 1;
        }
        if !self.hanging_channels.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.UpdateActorsRequest", len)?;
        if !self.request_id.is_empty() {
            struct_ser.serialize_field("requestId", &self.request_id)?;
        }
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
        }
        if !self.hanging_channels.is_empty() {
            struct_ser.serialize_field("hangingChannels", &self.hanging_channels)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UpdateActorsRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "requestId",
            "actors",
            "hangingChannels",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RequestId,
            Actors,
            HangingChannels,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "requestId" => Ok(GeneratedField::RequestId),
                            "actors" => Ok(GeneratedField::Actors),
                            "hangingChannels" => Ok(GeneratedField::HangingChannels),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UpdateActorsRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.UpdateActorsRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UpdateActorsRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut request_id = None;
                let mut actors = None;
                let mut hanging_channels = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::RequestId => {
                            if request_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("requestId"));
                            }
                            request_id = Some(map.next_value()?);
                        }
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(map.next_value()?);
                        }
                        GeneratedField::HangingChannels => {
                            if hanging_channels.is_some() {
                                return Err(serde::de::Error::duplicate_field("hangingChannels"));
                            }
                            hanging_channels = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UpdateActorsRequest {
                    request_id: request_id.unwrap_or_default(),
                    actors: actors.unwrap_or_default(),
                    hanging_channels: hanging_channels.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_service.UpdateActorsRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UpdateActorsResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.status.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_service.UpdateActorsResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UpdateActorsResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "status" => Ok(GeneratedField::Status),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UpdateActorsResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_service.UpdateActorsResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UpdateActorsResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UpdateActorsResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("stream_service.UpdateActorsResponse", FIELDS, GeneratedVisitor)
    }
}
