use crate::stream_plan::*;
impl serde::Serialize for ActorMapping {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hash_mapping.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.ActorMapping", len)?;
        if !self.hash_mapping.is_empty() {
            struct_ser.serialize_field("hashMapping", &self.hash_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActorMapping {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hashMapping",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HashMapping,
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
                            "hashMapping" => Ok(GeneratedField::HashMapping),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ActorMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.ActorMapping")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ActorMapping, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hash_mapping = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::HashMapping => {
                            if hash_mapping.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashMapping"));
                            }
                            hash_mapping = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(ActorMapping {
                    hash_mapping: hash_mapping.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.ActorMapping", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BatchPlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if !self.column_descs.is_empty() {
            len += 1;
        }
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.BatchPlanNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.column_descs.is_empty() {
            struct_ser.serialize_field("columnDescs", &self.column_descs)?;
        }
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BatchPlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "columnDescs",
            "distributionKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            ColumnDescs,
            DistributionKeys,
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
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "columnDescs" => Ok(GeneratedField::ColumnDescs),
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BatchPlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.BatchPlanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<BatchPlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut column_descs = None;
                let mut distribution_keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnDescs => {
                            if column_descs.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDescs"));
                            }
                            column_descs = Some(map.next_value()?);
                        }
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(BatchPlanNode {
                    table_ref_id,
                    column_descs: column_descs.unwrap_or_default(),
                    distribution_keys: distribution_keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.BatchPlanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ChainNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if !self.upstream_fields.is_empty() {
            len += 1;
        }
        if !self.column_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.ChainNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.upstream_fields.is_empty() {
            struct_ser.serialize_field("upstreamFields", &self.upstream_fields)?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ChainNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "upstreamFields",
            "columnIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            UpstreamFields,
            ColumnIds,
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
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "upstreamFields" => Ok(GeneratedField::UpstreamFields),
                            "columnIds" => Ok(GeneratedField::ColumnIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ChainNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.ChainNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ChainNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut upstream_fields = None;
                let mut column_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::UpstreamFields => {
                            if upstream_fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("upstreamFields"));
                            }
                            upstream_fields = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnIds => {
                            if column_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIds"));
                            }
                            column_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(ChainNode {
                    table_ref_id,
                    upstream_fields: upstream_fields.unwrap_or_default(),
                    column_ids: column_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.ChainNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DispatchStrategy {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.r#type != 0 {
            len += 1;
        }
        if !self.column_indices.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.DispatchStrategy", len)?;
        if self.r#type != 0 {
            let v = DispatcherType::from_i32(self.r#type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if !self.column_indices.is_empty() {
            struct_ser.serialize_field("columnIndices", &self.column_indices)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DispatchStrategy {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "columnIndices",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            ColumnIndices,
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
                            "type" => Ok(GeneratedField::Type),
                            "columnIndices" => Ok(GeneratedField::ColumnIndices),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DispatchStrategy;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.DispatchStrategy")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DispatchStrategy, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type = None;
                let mut column_indices = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value::<DispatcherType>()? as i32);
                        }
                        GeneratedField::ColumnIndices => {
                            if column_indices.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndices"));
                            }
                            column_indices = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(DispatchStrategy {
                    r#type: r#type.unwrap_or_default(),
                    column_indices: column_indices.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.DispatchStrategy", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Dispatcher {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.r#type != 0 {
            len += 1;
        }
        if !self.column_indices.is_empty() {
            len += 1;
        }
        if self.hash_mapping.is_some() {
            len += 1;
        }
        if !self.downstream_actor_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.Dispatcher", len)?;
        if self.r#type != 0 {
            let v = DispatcherType::from_i32(self.r#type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if !self.column_indices.is_empty() {
            struct_ser.serialize_field("columnIndices", &self.column_indices)?;
        }
        if let Some(v) = self.hash_mapping.as_ref() {
            struct_ser.serialize_field("hashMapping", v)?;
        }
        if !self.downstream_actor_id.is_empty() {
            struct_ser.serialize_field("downstreamActorId", &self.downstream_actor_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Dispatcher {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "columnIndices",
            "hashMapping",
            "downstreamActorId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            ColumnIndices,
            HashMapping,
            DownstreamActorId,
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
                            "type" => Ok(GeneratedField::Type),
                            "columnIndices" => Ok(GeneratedField::ColumnIndices),
                            "hashMapping" => Ok(GeneratedField::HashMapping),
                            "downstreamActorId" => Ok(GeneratedField::DownstreamActorId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Dispatcher;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.Dispatcher")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Dispatcher, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type = None;
                let mut column_indices = None;
                let mut hash_mapping = None;
                let mut downstream_actor_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value::<DispatcherType>()? as i32);
                        }
                        GeneratedField::ColumnIndices => {
                            if column_indices.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndices"));
                            }
                            column_indices = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::HashMapping => {
                            if hash_mapping.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashMapping"));
                            }
                            hash_mapping = Some(map.next_value()?);
                        }
                        GeneratedField::DownstreamActorId => {
                            if downstream_actor_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("downstreamActorId"));
                            }
                            downstream_actor_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(Dispatcher {
                    r#type: r#type.unwrap_or_default(),
                    column_indices: column_indices.unwrap_or_default(),
                    hash_mapping,
                    downstream_actor_id: downstream_actor_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.Dispatcher", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DispatcherType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Hash => "HASH",
            Self::Broadcast => "BROADCAST",
            Self::Simple => "SIMPLE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for DispatcherType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "HASH",
            "BROADCAST",
            "SIMPLE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DispatcherType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(DispatcherType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(DispatcherType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(DispatcherType::Invalid),
                    "HASH" => Ok(DispatcherType::Hash),
                    "BROADCAST" => Ok(DispatcherType::Broadcast),
                    "SIMPLE" => Ok(DispatcherType::Simple),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ExchangeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fields.is_empty() {
            len += 1;
        }
        if self.strategy.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.ExchangeNode", len)?;
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        if let Some(v) = self.strategy.as_ref() {
            struct_ser.serialize_field("strategy", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExchangeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fields",
            "strategy",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Fields,
            Strategy,
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
                            "fields" => Ok(GeneratedField::Fields),
                            "strategy" => Ok(GeneratedField::Strategy),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExchangeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.ExchangeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExchangeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fields = None;
                let mut strategy = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                        GeneratedField::Strategy => {
                            if strategy.is_some() {
                                return Err(serde::de::Error::duplicate_field("strategy"));
                            }
                            strategy = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExchangeNode {
                    fields: fields.unwrap_or_default(),
                    strategy,
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.ExchangeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FilterNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.search_condition.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.FilterNode", len)?;
        if let Some(v) = self.search_condition.as_ref() {
            struct_ser.serialize_field("searchCondition", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FilterNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "searchCondition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SearchCondition,
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
                            "searchCondition" => Ok(GeneratedField::SearchCondition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FilterNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.FilterNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FilterNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut search_condition = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SearchCondition => {
                            if search_condition.is_some() {
                                return Err(serde::de::Error::duplicate_field("searchCondition"));
                            }
                            search_condition = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FilterNode {
                    search_condition,
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.FilterNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashAggNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        if !self.agg_calls.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.HashAggNode", len)?;
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        if !self.agg_calls.is_empty() {
            struct_ser.serialize_field("aggCalls", &self.agg_calls)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashAggNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "distributionKeys",
            "aggCalls",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DistributionKeys,
            AggCalls,
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
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            "aggCalls" => Ok(GeneratedField::AggCalls),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashAggNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.HashAggNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashAggNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut distribution_keys = None;
                let mut agg_calls = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::AggCalls => {
                            if agg_calls.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggCalls"));
                            }
                            agg_calls = Some(map.next_value()?);
                        }
                    }
                }
                Ok(HashAggNode {
                    distribution_keys: distribution_keys.unwrap_or_default(),
                    agg_calls: agg_calls.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.HashAggNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashJoinNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.join_type != 0 {
            len += 1;
        }
        if !self.left_key.is_empty() {
            len += 1;
        }
        if !self.right_key.is_empty() {
            len += 1;
        }
        if self.condition.is_some() {
            len += 1;
        }
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.HashJoinNode", len)?;
        if self.join_type != 0 {
            let v = super::plan::JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if !self.left_key.is_empty() {
            struct_ser.serialize_field("leftKey", &self.left_key)?;
        }
        if !self.right_key.is_empty() {
            struct_ser.serialize_field("rightKey", &self.right_key)?;
        }
        if let Some(v) = self.condition.as_ref() {
            struct_ser.serialize_field("condition", v)?;
        }
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashJoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "joinType",
            "leftKey",
            "rightKey",
            "condition",
            "distributionKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JoinType,
            LeftKey,
            RightKey,
            Condition,
            DistributionKeys,
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
                            "joinType" => Ok(GeneratedField::JoinType),
                            "leftKey" => Ok(GeneratedField::LeftKey),
                            "rightKey" => Ok(GeneratedField::RightKey),
                            "condition" => Ok(GeneratedField::Condition),
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashJoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.HashJoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut join_type = None;
                let mut left_key = None;
                let mut right_key = None;
                let mut condition = None;
                let mut distribution_keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JoinType => {
                            if join_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type = Some(map.next_value::<super::plan::JoinType>()? as i32);
                        }
                        GeneratedField::LeftKey => {
                            if left_key.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftKey"));
                            }
                            left_key = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::RightKey => {
                            if right_key.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightKey"));
                            }
                            right_key = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Condition => {
                            if condition.is_some() {
                                return Err(serde::de::Error::duplicate_field("condition"));
                            }
                            condition = Some(map.next_value()?);
                        }
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(HashJoinNode {
                    join_type: join_type.unwrap_or_default(),
                    left_key: left_key.unwrap_or_default(),
                    right_key: right_key.unwrap_or_default(),
                    condition,
                    distribution_keys: distribution_keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.HashJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaterializeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if self.associated_table_ref_id.is_some() {
            len += 1;
        }
        if !self.column_orders.is_empty() {
            len += 1;
        }
        if !self.column_ids.is_empty() {
            len += 1;
        }
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.MaterializeNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if let Some(v) = self.associated_table_ref_id.as_ref() {
            struct_ser.serialize_field("associatedTableRefId", v)?;
        }
        if !self.column_orders.is_empty() {
            struct_ser.serialize_field("columnOrders", &self.column_orders)?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaterializeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "associatedTableRefId",
            "columnOrders",
            "columnIds",
            "distributionKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            AssociatedTableRefId,
            ColumnOrders,
            ColumnIds,
            DistributionKeys,
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
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "associatedTableRefId" => Ok(GeneratedField::AssociatedTableRefId),
                            "columnOrders" => Ok(GeneratedField::ColumnOrders),
                            "columnIds" => Ok(GeneratedField::ColumnIds),
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaterializeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.MaterializeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MaterializeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut associated_table_ref_id = None;
                let mut column_orders = None;
                let mut column_ids = None;
                let mut distribution_keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::AssociatedTableRefId => {
                            if associated_table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("associatedTableRefId"));
                            }
                            associated_table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnOrders => {
                            if column_orders.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnOrders"));
                            }
                            column_orders = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnIds => {
                            if column_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIds"));
                            }
                            column_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(MaterializeNode {
                    table_ref_id,
                    associated_table_ref_id,
                    column_orders: column_orders.unwrap_or_default(),
                    column_ids: column_ids.unwrap_or_default(),
                    distribution_keys: distribution_keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.MaterializeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MergeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.upstream_actor_id.is_empty() {
            len += 1;
        }
        if !self.fields.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.MergeNode", len)?;
        if !self.upstream_actor_id.is_empty() {
            struct_ser.serialize_field("upstreamActorId", &self.upstream_actor_id)?;
        }
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MergeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "upstreamActorId",
            "fields",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UpstreamActorId,
            Fields,
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
                            "upstreamActorId" => Ok(GeneratedField::UpstreamActorId),
                            "fields" => Ok(GeneratedField::Fields),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MergeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.MergeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MergeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut upstream_actor_id = None;
                let mut fields = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UpstreamActorId => {
                            if upstream_actor_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("upstreamActorId"));
                            }
                            upstream_actor_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MergeNode {
                    upstream_actor_id: upstream_actor_id.unwrap_or_default(),
                    fields: fields.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.MergeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.select_list.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.ProjectNode", len)?;
        if !self.select_list.is_empty() {
            struct_ser.serialize_field("selectList", &self.select_list)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "selectList",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SelectList,
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
                            "selectList" => Ok(GeneratedField::SelectList),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.ProjectNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ProjectNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut select_list = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SelectList => {
                            if select_list.is_some() {
                                return Err(serde::de::Error::duplicate_field("selectList"));
                            }
                            select_list = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ProjectNode {
                    select_list: select_list.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.ProjectNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SimpleAggNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.agg_calls.is_empty() {
            len += 1;
        }
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.SimpleAggNode", len)?;
        if !self.agg_calls.is_empty() {
            struct_ser.serialize_field("aggCalls", &self.agg_calls)?;
        }
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SimpleAggNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "aggCalls",
            "distributionKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AggCalls,
            DistributionKeys,
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
                            "aggCalls" => Ok(GeneratedField::AggCalls),
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SimpleAggNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.SimpleAggNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SimpleAggNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut agg_calls = None;
                let mut distribution_keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AggCalls => {
                            if agg_calls.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggCalls"));
                            }
                            agg_calls = Some(map.next_value()?);
                        }
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(SimpleAggNode {
                    agg_calls: agg_calls.unwrap_or_default(),
                    distribution_keys: distribution_keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.SimpleAggNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SourceNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if !self.column_ids.is_empty() {
            len += 1;
        }
        if self.source_type != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.SourceNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        if self.source_type != 0 {
            let v = source_node::SourceType::from_i32(self.source_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.source_type)))?;
            struct_ser.serialize_field("sourceType", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SourceNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "columnIds",
            "sourceType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            ColumnIds,
            SourceType,
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
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "columnIds" => Ok(GeneratedField::ColumnIds),
                            "sourceType" => Ok(GeneratedField::SourceType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SourceNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.SourceNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SourceNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut column_ids = None;
                let mut source_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnIds => {
                            if column_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIds"));
                            }
                            column_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::SourceType => {
                            if source_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceType"));
                            }
                            source_type = Some(map.next_value::<source_node::SourceType>()? as i32);
                        }
                    }
                }
                Ok(SourceNode {
                    table_ref_id,
                    column_ids: column_ids.unwrap_or_default(),
                    source_type: source_type.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.SourceNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for source_node::SourceType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Table => "TABLE",
            Self::Source => "SOURCE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for source_node::SourceType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "TABLE",
            "SOURCE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = source_node::SourceType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(source_node::SourceType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(source_node::SourceType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "TABLE" => Ok(source_node::SourceType::Table),
                    "SOURCE" => Ok(source_node::SourceType::Source),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for StreamActor {
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
        if self.fragment_id != 0 {
            len += 1;
        }
        if self.nodes.is_some() {
            len += 1;
        }
        if !self.dispatcher.is_empty() {
            len += 1;
        }
        if !self.upstream_actor_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.StreamActor", len)?;
        if self.actor_id != 0 {
            struct_ser.serialize_field("actorId", &self.actor_id)?;
        }
        if self.fragment_id != 0 {
            struct_ser.serialize_field("fragmentId", &self.fragment_id)?;
        }
        if let Some(v) = self.nodes.as_ref() {
            struct_ser.serialize_field("nodes", v)?;
        }
        if !self.dispatcher.is_empty() {
            struct_ser.serialize_field("dispatcher", &self.dispatcher)?;
        }
        if !self.upstream_actor_id.is_empty() {
            struct_ser.serialize_field("upstreamActorId", &self.upstream_actor_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StreamActor {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "actorId",
            "fragmentId",
            "nodes",
            "dispatcher",
            "upstreamActorId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ActorId,
            FragmentId,
            Nodes,
            Dispatcher,
            UpstreamActorId,
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
                            "actorId" => Ok(GeneratedField::ActorId),
                            "fragmentId" => Ok(GeneratedField::FragmentId),
                            "nodes" => Ok(GeneratedField::Nodes),
                            "dispatcher" => Ok(GeneratedField::Dispatcher),
                            "upstreamActorId" => Ok(GeneratedField::UpstreamActorId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamActor;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.StreamActor")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StreamActor, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut actor_id = None;
                let mut fragment_id = None;
                let mut nodes = None;
                let mut dispatcher = None;
                let mut upstream_actor_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ActorId => {
                            if actor_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorId"));
                            }
                            actor_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::FragmentId => {
                            if fragment_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragmentId"));
                            }
                            fragment_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Nodes => {
                            if nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodes"));
                            }
                            nodes = Some(map.next_value()?);
                        }
                        GeneratedField::Dispatcher => {
                            if dispatcher.is_some() {
                                return Err(serde::de::Error::duplicate_field("dispatcher"));
                            }
                            dispatcher = Some(map.next_value()?);
                        }
                        GeneratedField::UpstreamActorId => {
                            if upstream_actor_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("upstreamActorId"));
                            }
                            upstream_actor_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(StreamActor {
                    actor_id: actor_id.unwrap_or_default(),
                    fragment_id: fragment_id.unwrap_or_default(),
                    nodes,
                    dispatcher: dispatcher.unwrap_or_default(),
                    upstream_actor_id: upstream_actor_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.StreamActor", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StreamNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.operator_id != 0 {
            len += 1;
        }
        if !self.input.is_empty() {
            len += 1;
        }
        if !self.pk_indices.is_empty() {
            len += 1;
        }
        if !self.identity.is_empty() {
            len += 1;
        }
        if self.node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.StreamNode", len)?;
        if self.operator_id != 0 {
            struct_ser.serialize_field("operatorId", ToString::to_string(&self.operator_id).as_str())?;
        }
        if !self.input.is_empty() {
            struct_ser.serialize_field("input", &self.input)?;
        }
        if !self.pk_indices.is_empty() {
            struct_ser.serialize_field("pkIndices", &self.pk_indices)?;
        }
        if !self.identity.is_empty() {
            struct_ser.serialize_field("identity", &self.identity)?;
        }
        if let Some(v) = self.node.as_ref() {
            match v {
                stream_node::Node::SourceNode(v) => {
                    struct_ser.serialize_field("sourceNode", v)?;
                }
                stream_node::Node::ProjectNode(v) => {
                    struct_ser.serialize_field("projectNode", v)?;
                }
                stream_node::Node::FilterNode(v) => {
                    struct_ser.serialize_field("filterNode", v)?;
                }
                stream_node::Node::MaterializeNode(v) => {
                    struct_ser.serialize_field("materializeNode", v)?;
                }
                stream_node::Node::LocalSimpleAggNode(v) => {
                    struct_ser.serialize_field("localSimpleAggNode", v)?;
                }
                stream_node::Node::GlobalSimpleAggNode(v) => {
                    struct_ser.serialize_field("globalSimpleAggNode", v)?;
                }
                stream_node::Node::HashAggNode(v) => {
                    struct_ser.serialize_field("hashAggNode", v)?;
                }
                stream_node::Node::AppendOnlyTopNNode(v) => {
                    struct_ser.serialize_field("appendOnlyTopNNode", v)?;
                }
                stream_node::Node::HashJoinNode(v) => {
                    struct_ser.serialize_field("hashJoinNode", v)?;
                }
                stream_node::Node::TopNNode(v) => {
                    struct_ser.serialize_field("topNNode", v)?;
                }
                stream_node::Node::MergeNode(v) => {
                    struct_ser.serialize_field("mergeNode", v)?;
                }
                stream_node::Node::ExchangeNode(v) => {
                    struct_ser.serialize_field("exchangeNode", v)?;
                }
                stream_node::Node::ChainNode(v) => {
                    struct_ser.serialize_field("chainNode", v)?;
                }
                stream_node::Node::BatchPlanNode(v) => {
                    struct_ser.serialize_field("batchPlanNode", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StreamNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "operatorId",
            "input",
            "pkIndices",
            "identity",
            "sourceNode",
            "projectNode",
            "filterNode",
            "materializeNode",
            "localSimpleAggNode",
            "globalSimpleAggNode",
            "hashAggNode",
            "appendOnlyTopNNode",
            "hashJoinNode",
            "topNNode",
            "mergeNode",
            "exchangeNode",
            "chainNode",
            "batchPlanNode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OperatorId,
            Input,
            PkIndices,
            Identity,
            SourceNode,
            ProjectNode,
            FilterNode,
            MaterializeNode,
            LocalSimpleAggNode,
            GlobalSimpleAggNode,
            HashAggNode,
            AppendOnlyTopNNode,
            HashJoinNode,
            TopNNode,
            MergeNode,
            ExchangeNode,
            ChainNode,
            BatchPlanNode,
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
                            "operatorId" => Ok(GeneratedField::OperatorId),
                            "input" => Ok(GeneratedField::Input),
                            "pkIndices" => Ok(GeneratedField::PkIndices),
                            "identity" => Ok(GeneratedField::Identity),
                            "sourceNode" => Ok(GeneratedField::SourceNode),
                            "projectNode" => Ok(GeneratedField::ProjectNode),
                            "filterNode" => Ok(GeneratedField::FilterNode),
                            "materializeNode" => Ok(GeneratedField::MaterializeNode),
                            "localSimpleAggNode" => Ok(GeneratedField::LocalSimpleAggNode),
                            "globalSimpleAggNode" => Ok(GeneratedField::GlobalSimpleAggNode),
                            "hashAggNode" => Ok(GeneratedField::HashAggNode),
                            "appendOnlyTopNNode" => Ok(GeneratedField::AppendOnlyTopNNode),
                            "hashJoinNode" => Ok(GeneratedField::HashJoinNode),
                            "topNNode" => Ok(GeneratedField::TopNNode),
                            "mergeNode" => Ok(GeneratedField::MergeNode),
                            "exchangeNode" => Ok(GeneratedField::ExchangeNode),
                            "chainNode" => Ok(GeneratedField::ChainNode),
                            "batchPlanNode" => Ok(GeneratedField::BatchPlanNode),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.StreamNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StreamNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut operator_id = None;
                let mut input = None;
                let mut pk_indices = None;
                let mut identity = None;
                let mut node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OperatorId => {
                            if operator_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("operatorId"));
                            }
                            operator_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Input => {
                            if input.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input = Some(map.next_value()?);
                        }
                        GeneratedField::PkIndices => {
                            if pk_indices.is_some() {
                                return Err(serde::de::Error::duplicate_field("pkIndices"));
                            }
                            pk_indices = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Identity => {
                            if identity.is_some() {
                                return Err(serde::de::Error::duplicate_field("identity"));
                            }
                            identity = Some(map.next_value()?);
                        }
                        GeneratedField::SourceNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceNode"));
                            }
                            node = Some(stream_node::Node::SourceNode(map.next_value()?));
                        }
                        GeneratedField::ProjectNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("projectNode"));
                            }
                            node = Some(stream_node::Node::ProjectNode(map.next_value()?));
                        }
                        GeneratedField::FilterNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("filterNode"));
                            }
                            node = Some(stream_node::Node::FilterNode(map.next_value()?));
                        }
                        GeneratedField::MaterializeNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("materializeNode"));
                            }
                            node = Some(stream_node::Node::MaterializeNode(map.next_value()?));
                        }
                        GeneratedField::LocalSimpleAggNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("localSimpleAggNode"));
                            }
                            node = Some(stream_node::Node::LocalSimpleAggNode(map.next_value()?));
                        }
                        GeneratedField::GlobalSimpleAggNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("globalSimpleAggNode"));
                            }
                            node = Some(stream_node::Node::GlobalSimpleAggNode(map.next_value()?));
                        }
                        GeneratedField::HashAggNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashAggNode"));
                            }
                            node = Some(stream_node::Node::HashAggNode(map.next_value()?));
                        }
                        GeneratedField::AppendOnlyTopNNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("appendOnlyTopNNode"));
                            }
                            node = Some(stream_node::Node::AppendOnlyTopNNode(map.next_value()?));
                        }
                        GeneratedField::HashJoinNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashJoinNode"));
                            }
                            node = Some(stream_node::Node::HashJoinNode(map.next_value()?));
                        }
                        GeneratedField::TopNNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("topNNode"));
                            }
                            node = Some(stream_node::Node::TopNNode(map.next_value()?));
                        }
                        GeneratedField::MergeNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("mergeNode"));
                            }
                            node = Some(stream_node::Node::MergeNode(map.next_value()?));
                        }
                        GeneratedField::ExchangeNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("exchangeNode"));
                            }
                            node = Some(stream_node::Node::ExchangeNode(map.next_value()?));
                        }
                        GeneratedField::ChainNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("chainNode"));
                            }
                            node = Some(stream_node::Node::ChainNode(map.next_value()?));
                        }
                        GeneratedField::BatchPlanNode => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("batchPlanNode"));
                            }
                            node = Some(stream_node::Node::BatchPlanNode(map.next_value()?));
                        }
                    }
                }
                Ok(StreamNode {
                    operator_id: operator_id.unwrap_or_default(),
                    input: input.unwrap_or_default(),
                    pk_indices: pk_indices.unwrap_or_default(),
                    identity: identity.unwrap_or_default(),
                    node,
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.StreamNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TopNNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.order_types.is_empty() {
            len += 1;
        }
        if self.limit != 0 {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        if !self.distribution_keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("stream_plan.TopNNode", len)?;
        if !self.order_types.is_empty() {
            let v = self.order_types.iter().cloned().map(|v| {
                super::plan::OrderType::from_i32(v)
                    .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", v)))
                }).collect::<Result<Vec<_>, _>>()?;
            struct_ser.serialize_field("orderTypes", &v)?;
        }
        if self.limit != 0 {
            struct_ser.serialize_field("limit", ToString::to_string(&self.limit).as_str())?;
        }
        if self.offset != 0 {
            struct_ser.serialize_field("offset", ToString::to_string(&self.offset).as_str())?;
        }
        if !self.distribution_keys.is_empty() {
            struct_ser.serialize_field("distributionKeys", &self.distribution_keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TopNNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "orderTypes",
            "limit",
            "offset",
            "distributionKeys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OrderTypes,
            Limit,
            Offset,
            DistributionKeys,
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
                            "orderTypes" => Ok(GeneratedField::OrderTypes),
                            "limit" => Ok(GeneratedField::Limit),
                            "offset" => Ok(GeneratedField::Offset),
                            "distributionKeys" => Ok(GeneratedField::DistributionKeys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TopNNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct stream_plan.TopNNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TopNNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut order_types = None;
                let mut limit = None;
                let mut offset = None;
                let mut distribution_keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OrderTypes => {
                            if order_types.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderTypes"));
                            }
                            order_types = Some(map.next_value::<Vec<super::plan::OrderType>>()?.into_iter().map(|x| x as i32).collect());
                        }
                        GeneratedField::Limit => {
                            if limit.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Offset => {
                            if offset.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::DistributionKeys => {
                            if distribution_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionKeys"));
                            }
                            distribution_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(TopNNode {
                    order_types: order_types.unwrap_or_default(),
                    limit: limit.unwrap_or_default(),
                    offset: offset.unwrap_or_default(),
                    distribution_keys: distribution_keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("stream_plan.TopNNode", FIELDS, GeneratedVisitor)
    }
}
