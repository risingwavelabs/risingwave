use crate::meta::*;
impl serde::Serialize for ActivateWorkerNodeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.ActivateWorkerNodeRequest", len)?;
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActivateWorkerNodeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
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
            type Value = ActivateWorkerNodeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.ActivateWorkerNodeRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ActivateWorkerNodeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut host = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Host => {
                            if host.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ActivateWorkerNodeRequest {
                    host,
                })
            }
        }
        deserializer.deserialize_struct("meta.ActivateWorkerNodeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ActivateWorkerNodeResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.ActivateWorkerNodeResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ActivateWorkerNodeResponse {
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
            type Value = ActivateWorkerNodeResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.ActivateWorkerNodeResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ActivateWorkerNodeResponse, V::Error>
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
                Ok(ActivateWorkerNodeResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.ActivateWorkerNodeResponse", FIELDS, GeneratedVisitor)
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
        if self.node.is_some() {
            len += 1;
        }
        if !self.actors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.ActorLocation", len)?;
        if let Some(v) = self.node.as_ref() {
            struct_ser.serialize_field("node", v)?;
        }
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
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
            "node",
            "actors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Actors,
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
                            "node" => Ok(GeneratedField::Node),
                            "actors" => Ok(GeneratedField::Actors),
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
                formatter.write_str("struct meta.ActorLocation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ActorLocation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node = None;
                let mut actors = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node = Some(map.next_value()?);
                        }
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ActorLocation {
                    node,
                    actors: actors.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.ActorLocation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AddWorkerNodeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.worker_type != 0 {
            len += 1;
        }
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.AddWorkerNodeRequest", len)?;
        if self.worker_type != 0 {
            let v = super::common::WorkerType::from_i32(self.worker_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.worker_type)))?;
            struct_ser.serialize_field("workerType", &v)?;
        }
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AddWorkerNodeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "workerType",
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorkerType,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "workerType" => Ok(GeneratedField::WorkerType),
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
            type Value = AddWorkerNodeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.AddWorkerNodeRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AddWorkerNodeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worker_type = None;
                let mut host = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorkerType => {
                            if worker_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("workerType"));
                            }
                            worker_type = Some(map.next_value::<super::common::WorkerType>()? as i32);
                        }
                        GeneratedField::Host => {
                            if host.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AddWorkerNodeRequest {
                    worker_type: worker_type.unwrap_or_default(),
                    host,
                })
            }
        }
        deserializer.deserialize_struct("meta.AddWorkerNodeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AddWorkerNodeResponse {
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
        if self.node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.AddWorkerNodeResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.node.as_ref() {
            struct_ser.serialize_field("node", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AddWorkerNodeResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "node",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Node,
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
                            "node" => Ok(GeneratedField::Node),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AddWorkerNodeResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.AddWorkerNodeResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AddWorkerNodeResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Node => {
                            if node.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AddWorkerNodeResponse {
                    status,
                    node,
                })
            }
        }
        deserializer.deserialize_struct("meta.AddWorkerNodeResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Catalog {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.version != 0 {
            len += 1;
        }
        if !self.databases.is_empty() {
            len += 1;
        }
        if !self.schemas.is_empty() {
            len += 1;
        }
        if !self.tables.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.Catalog", len)?;
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        if !self.databases.is_empty() {
            struct_ser.serialize_field("databases", &self.databases)?;
        }
        if !self.schemas.is_empty() {
            struct_ser.serialize_field("schemas", &self.schemas)?;
        }
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Catalog {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "version",
            "databases",
            "schemas",
            "tables",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Version,
            Databases,
            Schemas,
            Tables,
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
                            "version" => Ok(GeneratedField::Version),
                            "databases" => Ok(GeneratedField::Databases),
                            "schemas" => Ok(GeneratedField::Schemas),
                            "tables" => Ok(GeneratedField::Tables),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Catalog;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.Catalog")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Catalog, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut version = None;
                let mut databases = None;
                let mut schemas = None;
                let mut tables = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Databases => {
                            if databases.is_some() {
                                return Err(serde::de::Error::duplicate_field("databases"));
                            }
                            databases = Some(map.next_value()?);
                        }
                        GeneratedField::Schemas => {
                            if schemas.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemas"));
                            }
                            schemas = Some(map.next_value()?);
                        }
                        GeneratedField::Tables => {
                            if tables.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Catalog {
                    version: version.unwrap_or_default(),
                    databases: databases.unwrap_or_default(),
                    schemas: schemas.unwrap_or_default(),
                    tables: tables.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.Catalog", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateMaterializedViewRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.table_ref_id.is_some() {
            len += 1;
        }
        if self.stream_node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.CreateMaterializedViewRequest", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if let Some(v) = self.stream_node.as_ref() {
            struct_ser.serialize_field("streamNode", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateMaterializedViewRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "tableRefId",
            "streamNode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            TableRefId,
            StreamNode,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "streamNode" => Ok(GeneratedField::StreamNode),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateMaterializedViewRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.CreateMaterializedViewRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedViewRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut table_ref_id = None;
                let mut stream_node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::StreamNode => {
                            if stream_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamNode"));
                            }
                            stream_node = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateMaterializedViewRequest {
                    node_id: node_id.unwrap_or_default(),
                    table_ref_id,
                    stream_node,
                })
            }
        }
        deserializer.deserialize_struct("meta.CreateMaterializedViewRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateMaterializedViewResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.CreateMaterializedViewResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateMaterializedViewResponse {
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
            type Value = CreateMaterializedViewResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.CreateMaterializedViewResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedViewResponse, V::Error>
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
                Ok(CreateMaterializedViewResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.CreateMaterializedViewResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.catalog_body.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.CreateRequest", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if let Some(v) = self.catalog_body.as_ref() {
            match v {
                create_request::CatalogBody::Database(v) => {
                    struct_ser.serialize_field("database", v)?;
                }
                create_request::CatalogBody::Schema(v) => {
                    struct_ser.serialize_field("schema", v)?;
                }
                create_request::CatalogBody::Table(v) => {
                    struct_ser.serialize_field("table", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "database",
            "schema",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            Database,
            Schema,
            Table,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.CreateRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut catalog_body = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Database => {
                            if catalog_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            catalog_body = Some(create_request::CatalogBody::Database(map.next_value()?));
                        }
                        GeneratedField::Schema => {
                            if catalog_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            catalog_body = Some(create_request::CatalogBody::Schema(map.next_value()?));
                        }
                        GeneratedField::Table => {
                            if catalog_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            catalog_body = Some(create_request::CatalogBody::Table(map.next_value()?));
                        }
                    }
                }
                Ok(CreateRequest {
                    node_id: node_id.unwrap_or_default(),
                    catalog_body,
                })
            }
        }
        deserializer.deserialize_struct("meta.CreateRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateResponse {
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
        if self.id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.CreateResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.id != 0 {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "id",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Id,
            Version,
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
                            "id" => Ok(GeneratedField::Id),
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.CreateResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(CreateResponse {
                    status,
                    id: id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.CreateResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Database {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.database_ref_id.is_some() {
            len += 1;
        }
        if !self.database_name.is_empty() {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.Database", len)?;
        if let Some(v) = self.database_ref_id.as_ref() {
            struct_ser.serialize_field("databaseRefId", v)?;
        }
        if !self.database_name.is_empty() {
            struct_ser.serialize_field("databaseName", &self.database_name)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Database {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "databaseRefId",
            "databaseName",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DatabaseRefId,
            DatabaseName,
            Version,
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
                            "databaseRefId" => Ok(GeneratedField::DatabaseRefId),
                            "databaseName" => Ok(GeneratedField::DatabaseName),
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Database;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.Database")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Database, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database_ref_id = None;
                let mut database_name = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DatabaseRefId => {
                            if database_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseRefId"));
                            }
                            database_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::DatabaseName => {
                            if database_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseName"));
                            }
                            database_name = Some(map.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Database {
                    database_ref_id,
                    database_name: database_name.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.Database", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteWorkerNodeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.DeleteWorkerNodeRequest", len)?;
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteWorkerNodeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
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
            type Value = DeleteWorkerNodeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DeleteWorkerNodeRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DeleteWorkerNodeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut host = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Host => {
                            if host.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DeleteWorkerNodeRequest {
                    host,
                })
            }
        }
        deserializer.deserialize_struct("meta.DeleteWorkerNodeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteWorkerNodeResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.DeleteWorkerNodeResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteWorkerNodeResponse {
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
            type Value = DeleteWorkerNodeResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DeleteWorkerNodeResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DeleteWorkerNodeResponse, V::Error>
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
                Ok(DeleteWorkerNodeResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.DeleteWorkerNodeResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropMaterializedViewRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.table_ref_id.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.DropMaterializedViewRequest", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropMaterializedViewRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "tableRefId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            TableRefId,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropMaterializedViewRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DropMaterializedViewRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedViewRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut table_ref_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DropMaterializedViewRequest {
                    node_id: node_id.unwrap_or_default(),
                    table_ref_id,
                })
            }
        }
        deserializer.deserialize_struct("meta.DropMaterializedViewRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropMaterializedViewResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.DropMaterializedViewResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropMaterializedViewResponse {
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
            type Value = DropMaterializedViewResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DropMaterializedViewResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedViewResponse, V::Error>
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
                Ok(DropMaterializedViewResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.DropMaterializedViewResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.catalog_id.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.DropRequest", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if let Some(v) = self.catalog_id.as_ref() {
            match v {
                drop_request::CatalogId::DatabaseId(v) => {
                    struct_ser.serialize_field("databaseId", v)?;
                }
                drop_request::CatalogId::SchemaId(v) => {
                    struct_ser.serialize_field("schemaId", v)?;
                }
                drop_request::CatalogId::TableId(v) => {
                    struct_ser.serialize_field("tableId", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "databaseId",
            "schemaId",
            "tableId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            DatabaseId,
            SchemaId,
            TableId,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "databaseId" => Ok(GeneratedField::DatabaseId),
                            "schemaId" => Ok(GeneratedField::SchemaId),
                            "tableId" => Ok(GeneratedField::TableId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DropRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut catalog_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::DatabaseId => {
                            if catalog_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseId"));
                            }
                            catalog_id = Some(drop_request::CatalogId::DatabaseId(map.next_value()?));
                        }
                        GeneratedField::SchemaId => {
                            if catalog_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaId"));
                            }
                            catalog_id = Some(drop_request::CatalogId::SchemaId(map.next_value()?));
                        }
                        GeneratedField::TableId => {
                            if catalog_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            catalog_id = Some(drop_request::CatalogId::TableId(map.next_value()?));
                        }
                    }
                }
                Ok(DropRequest {
                    node_id: node_id.unwrap_or_default(),
                    catalog_id,
                })
            }
        }
        deserializer.deserialize_struct("meta.DropRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropResponse {
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
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.DropResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Version,
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
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.DropResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.DropResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FlushRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("meta.FlushRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FlushRequest {
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
            type Value = FlushRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.FlushRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FlushRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(FlushRequest {
                })
            }
        }
        deserializer.deserialize_struct("meta.FlushRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FlushResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.FlushResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FlushResponse {
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
            type Value = FlushResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.FlushResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FlushResponse, V::Error>
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
                Ok(FlushResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.FlushResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetCatalogRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("meta.GetCatalogRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetCatalogRequest {
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
            type Value = GetCatalogRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.GetCatalogRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetCatalogRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(GetCatalogRequest {
                })
            }
        }
        deserializer.deserialize_struct("meta.GetCatalogRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetCatalogResponse {
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
        if self.catalog.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.GetCatalogResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.catalog.as_ref() {
            struct_ser.serialize_field("catalog", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetCatalogResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "catalog",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Catalog,
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
                            "catalog" => Ok(GeneratedField::Catalog),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetCatalogResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.GetCatalogResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetCatalogResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut catalog = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Catalog => {
                            if catalog.is_some() {
                                return Err(serde::de::Error::duplicate_field("catalog"));
                            }
                            catalog = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetCatalogResponse {
                    status,
                    catalog,
                })
            }
        }
        deserializer.deserialize_struct("meta.GetCatalogResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetEpochRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("meta.GetEpochRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetEpochRequest {
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
            type Value = GetEpochRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.GetEpochRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetEpochRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(GetEpochRequest {
                })
            }
        }
        deserializer.deserialize_struct("meta.GetEpochRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetEpochResponse {
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
        if self.epoch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.GetEpochResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetEpochResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Epoch,
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
                            "epoch" => Ok(GeneratedField::Epoch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetEpochResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.GetEpochResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetEpochResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Epoch => {
                            if epoch.is_some() {
                                return Err(serde::de::Error::duplicate_field("epoch"));
                            }
                            epoch = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(GetEpochResponse {
                    status,
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.GetEpochResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HeartbeatRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.worker_type != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.HeartbeatRequest", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if self.worker_type != 0 {
            let v = super::common::WorkerType::from_i32(self.worker_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.worker_type)))?;
            struct_ser.serialize_field("workerType", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HeartbeatRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "workerType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            WorkerType,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "workerType" => Ok(GeneratedField::WorkerType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HeartbeatRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.HeartbeatRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HeartbeatRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut worker_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::WorkerType => {
                            if worker_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("workerType"));
                            }
                            worker_type = Some(map.next_value::<super::common::WorkerType>()? as i32);
                        }
                    }
                }
                Ok(HeartbeatRequest {
                    node_id: node_id.unwrap_or_default(),
                    worker_type: worker_type.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.HeartbeatRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HeartbeatResponse {
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
        let mut struct_ser = serializer.serialize_struct("meta.HeartbeatResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HeartbeatResponse {
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
            type Value = HeartbeatResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.HeartbeatResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HeartbeatResponse, V::Error>
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
                Ok(HeartbeatResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("meta.HeartbeatResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListAllNodesRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.worker_type != 0 {
            len += 1;
        }
        if self.include_starting_nodes {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.ListAllNodesRequest", len)?;
        if self.worker_type != 0 {
            let v = super::common::WorkerType::from_i32(self.worker_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.worker_type)))?;
            struct_ser.serialize_field("workerType", &v)?;
        }
        if self.include_starting_nodes {
            struct_ser.serialize_field("includeStartingNodes", &self.include_starting_nodes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListAllNodesRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "workerType",
            "includeStartingNodes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorkerType,
            IncludeStartingNodes,
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
                            "workerType" => Ok(GeneratedField::WorkerType),
                            "includeStartingNodes" => Ok(GeneratedField::IncludeStartingNodes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListAllNodesRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.ListAllNodesRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListAllNodesRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worker_type = None;
                let mut include_starting_nodes = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorkerType => {
                            if worker_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("workerType"));
                            }
                            worker_type = Some(map.next_value::<super::common::WorkerType>()? as i32);
                        }
                        GeneratedField::IncludeStartingNodes => {
                            if include_starting_nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("includeStartingNodes"));
                            }
                            include_starting_nodes = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ListAllNodesRequest {
                    worker_type: worker_type.unwrap_or_default(),
                    include_starting_nodes: include_starting_nodes.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.ListAllNodesRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListAllNodesResponse {
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
        if !self.nodes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.ListAllNodesResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if !self.nodes.is_empty() {
            struct_ser.serialize_field("nodes", &self.nodes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListAllNodesResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "nodes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Nodes,
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
                            "nodes" => Ok(GeneratedField::Nodes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListAllNodesResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.ListAllNodesResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListAllNodesResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut nodes = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Nodes => {
                            if nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodes"));
                            }
                            nodes = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ListAllNodesResponse {
                    status,
                    nodes: nodes.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.ListAllNodesResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MetaSnapshot {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.nodes.is_empty() {
            len += 1;
        }
        if !self.database.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        if !self.view.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.MetaSnapshot", len)?;
        if !self.nodes.is_empty() {
            struct_ser.serialize_field("nodes", &self.nodes)?;
        }
        if !self.database.is_empty() {
            struct_ser.serialize_field("database", &self.database)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        if !self.view.is_empty() {
            struct_ser.serialize_field("view", &self.view)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MetaSnapshot {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodes",
            "database",
            "schema",
            "source",
            "table",
            "view",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Nodes,
            Database,
            Schema,
            Source,
            Table,
            View,
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
                            "nodes" => Ok(GeneratedField::Nodes),
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "source" => Ok(GeneratedField::Source),
                            "table" => Ok(GeneratedField::Table),
                            "view" => Ok(GeneratedField::View),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MetaSnapshot;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.MetaSnapshot")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MetaSnapshot, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut nodes = None;
                let mut database = None;
                let mut schema = None;
                let mut source = None;
                let mut table = None;
                let mut view = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Nodes => {
                            if nodes.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodes"));
                            }
                            nodes = Some(map.next_value()?);
                        }
                        GeneratedField::Database => {
                            if database.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            database = Some(map.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema = Some(map.next_value()?);
                        }
                        GeneratedField::Source => {
                            if source.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source = Some(map.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table = Some(map.next_value()?);
                        }
                        GeneratedField::View => {
                            if view.is_some() {
                                return Err(serde::de::Error::duplicate_field("view"));
                            }
                            view = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MetaSnapshot {
                    nodes: nodes.unwrap_or_default(),
                    database: database.unwrap_or_default(),
                    schema: schema.unwrap_or_default(),
                    source: source.unwrap_or_default(),
                    table: table.unwrap_or_default(),
                    view: view.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.MetaSnapshot", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParallelUnitMapping {
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
        let mut struct_ser = serializer.serialize_struct("meta.ParallelUnitMapping", len)?;
        if !self.hash_mapping.is_empty() {
            struct_ser.serialize_field("hashMapping", &self.hash_mapping)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParallelUnitMapping {
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
            type Value = ParallelUnitMapping;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.ParallelUnitMapping")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ParallelUnitMapping, V::Error>
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
                Ok(ParallelUnitMapping {
                    hash_mapping: hash_mapping.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.ParallelUnitMapping", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Schema {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema_ref_id.is_some() {
            len += 1;
        }
        if !self.schema_name.is_empty() {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.Schema", len)?;
        if let Some(v) = self.schema_ref_id.as_ref() {
            struct_ser.serialize_field("schemaRefId", v)?;
        }
        if !self.schema_name.is_empty() {
            struct_ser.serialize_field("schemaName", &self.schema_name)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Schema {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schemaRefId",
            "schemaName",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaRefId,
            SchemaName,
            Version,
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
                            "schemaRefId" => Ok(GeneratedField::SchemaRefId),
                            "schemaName" => Ok(GeneratedField::SchemaName),
                            "version" => Ok(GeneratedField::Version),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Schema;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.Schema")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Schema, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_ref_id = None;
                let mut schema_name = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SchemaRefId => {
                            if schema_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaRefId"));
                            }
                            schema_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::SchemaName => {
                            if schema_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            schema_name = Some(map.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Schema {
                    schema_ref_id,
                    schema_name: schema_name.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.Schema", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubscribeRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.worker_type != 0 {
            len += 1;
        }
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.SubscribeRequest", len)?;
        if self.worker_type != 0 {
            let v = super::common::WorkerType::from_i32(self.worker_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.worker_type)))?;
            struct_ser.serialize_field("workerType", &v)?;
        }
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubscribeRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "workerType",
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WorkerType,
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "workerType" => Ok(GeneratedField::WorkerType),
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
            type Value = SubscribeRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.SubscribeRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SubscribeRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut worker_type = None;
                let mut host = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::WorkerType => {
                            if worker_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("workerType"));
                            }
                            worker_type = Some(map.next_value::<super::common::WorkerType>()? as i32);
                        }
                        GeneratedField::Host => {
                            if host.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SubscribeRequest {
                    worker_type: worker_type.unwrap_or_default(),
                    host,
                })
            }
        }
        deserializer.deserialize_struct("meta.SubscribeRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubscribeResponse {
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
        if self.operation != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        if self.info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.SubscribeResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.operation != 0 {
            let v = subscribe_response::Operation::from_i32(self.operation)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.operation)))?;
            struct_ser.serialize_field("operation", &v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        if let Some(v) = self.info.as_ref() {
            match v {
                subscribe_response::Info::Node(v) => {
                    struct_ser.serialize_field("node", v)?;
                }
                subscribe_response::Info::Database(v) => {
                    struct_ser.serialize_field("database", v)?;
                }
                subscribe_response::Info::Schema(v) => {
                    struct_ser.serialize_field("schema", v)?;
                }
                subscribe_response::Info::Table(v) => {
                    struct_ser.serialize_field("table", v)?;
                }
                subscribe_response::Info::DatabaseV2(v) => {
                    struct_ser.serialize_field("databaseV2", v)?;
                }
                subscribe_response::Info::SchemaV2(v) => {
                    struct_ser.serialize_field("schemaV2", v)?;
                }
                subscribe_response::Info::TableV2(v) => {
                    struct_ser.serialize_field("tableV2", v)?;
                }
                subscribe_response::Info::Source(v) => {
                    struct_ser.serialize_field("source", v)?;
                }
                subscribe_response::Info::FeSnapshot(v) => {
                    struct_ser.serialize_field("feSnapshot", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubscribeResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "operation",
            "version",
            "node",
            "database",
            "schema",
            "table",
            "databaseV2",
            "schemaV2",
            "tableV2",
            "source",
            "feSnapshot",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Operation,
            Version,
            Node,
            Database,
            Schema,
            Table,
            DatabaseV2,
            SchemaV2,
            TableV2,
            Source,
            FeSnapshot,
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
                            "operation" => Ok(GeneratedField::Operation),
                            "version" => Ok(GeneratedField::Version),
                            "node" => Ok(GeneratedField::Node),
                            "database" => Ok(GeneratedField::Database),
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            "databaseV2" => Ok(GeneratedField::DatabaseV2),
                            "schemaV2" => Ok(GeneratedField::SchemaV2),
                            "tableV2" => Ok(GeneratedField::TableV2),
                            "source" => Ok(GeneratedField::Source),
                            "feSnapshot" => Ok(GeneratedField::FeSnapshot),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubscribeResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.SubscribeResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SubscribeResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut operation = None;
                let mut version = None;
                let mut info = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Operation => {
                            if operation.is_some() {
                                return Err(serde::de::Error::duplicate_field("operation"));
                            }
                            operation = Some(map.next_value::<subscribe_response::Operation>()? as i32);
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Node => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            info = Some(subscribe_response::Info::Node(map.next_value()?));
                        }
                        GeneratedField::Database => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("database"));
                            }
                            info = Some(subscribe_response::Info::Database(map.next_value()?));
                        }
                        GeneratedField::Schema => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            info = Some(subscribe_response::Info::Schema(map.next_value()?));
                        }
                        GeneratedField::Table => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            info = Some(subscribe_response::Info::Table(map.next_value()?));
                        }
                        GeneratedField::DatabaseV2 => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseV2"));
                            }
                            info = Some(subscribe_response::Info::DatabaseV2(map.next_value()?));
                        }
                        GeneratedField::SchemaV2 => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaV2"));
                            }
                            info = Some(subscribe_response::Info::SchemaV2(map.next_value()?));
                        }
                        GeneratedField::TableV2 => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableV2"));
                            }
                            info = Some(subscribe_response::Info::TableV2(map.next_value()?));
                        }
                        GeneratedField::Source => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            info = Some(subscribe_response::Info::Source(map.next_value()?));
                        }
                        GeneratedField::FeSnapshot => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("feSnapshot"));
                            }
                            info = Some(subscribe_response::Info::FeSnapshot(map.next_value()?));
                        }
                    }
                }
                Ok(SubscribeResponse {
                    status,
                    operation: operation.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                    info,
                })
            }
        }
        deserializer.deserialize_struct("meta.SubscribeResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for subscribe_response::Operation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Add => "ADD",
            Self::Delete => "DELETE",
            Self::Update => "UPDATE",
            Self::Snapshot => "SNAPSHOT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for subscribe_response::Operation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "ADD",
            "DELETE",
            "UPDATE",
            "SNAPSHOT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = subscribe_response::Operation;

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
                    .and_then(subscribe_response::Operation::from_i32)
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
                    .and_then(subscribe_response::Operation::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(subscribe_response::Operation::Invalid),
                    "ADD" => Ok(subscribe_response::Operation::Add),
                    "DELETE" => Ok(subscribe_response::Operation::Delete),
                    "UPDATE" => Ok(subscribe_response::Operation::Update),
                    "SNAPSHOT" => Ok(subscribe_response::Operation::Snapshot),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Table {
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
        if !self.table_name.is_empty() {
            len += 1;
        }
        if !self.column_descs.is_empty() {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        if self.info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.Table", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        if !self.column_descs.is_empty() {
            struct_ser.serialize_field("columnDescs", &self.column_descs)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        if let Some(v) = self.info.as_ref() {
            match v {
                table::Info::StreamSource(v) => {
                    struct_ser.serialize_field("streamSource", v)?;
                }
                table::Info::TableSource(v) => {
                    struct_ser.serialize_field("tableSource", v)?;
                }
                table::Info::MaterializedView(v) => {
                    struct_ser.serialize_field("materializedView", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Table {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "tableName",
            "columnDescs",
            "version",
            "streamSource",
            "tableSource",
            "materializedView",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            TableName,
            ColumnDescs,
            Version,
            StreamSource,
            TableSource,
            MaterializedView,
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
                            "tableName" => Ok(GeneratedField::TableName),
                            "columnDescs" => Ok(GeneratedField::ColumnDescs),
                            "version" => Ok(GeneratedField::Version),
                            "streamSource" => Ok(GeneratedField::StreamSource),
                            "tableSource" => Ok(GeneratedField::TableSource),
                            "materializedView" => Ok(GeneratedField::MaterializedView),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Table;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.Table")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Table, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut table_name = None;
                let mut column_descs = None;
                let mut version = None;
                let mut info = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::TableName => {
                            if table_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnDescs => {
                            if column_descs.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDescs"));
                            }
                            column_descs = Some(map.next_value()?);
                        }
                        GeneratedField::Version => {
                            if version.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::StreamSource => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamSource"));
                            }
                            info = Some(table::Info::StreamSource(map.next_value()?));
                        }
                        GeneratedField::TableSource => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSource"));
                            }
                            info = Some(table::Info::TableSource(map.next_value()?));
                        }
                        GeneratedField::MaterializedView => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("materializedView"));
                            }
                            info = Some(table::Info::MaterializedView(map.next_value()?));
                        }
                    }
                }
                Ok(Table {
                    table_ref_id,
                    table_name: table_name.unwrap_or_default(),
                    column_descs: column_descs.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                    info,
                })
            }
        }
        deserializer.deserialize_struct("meta.Table", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableFragments {
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
        if !self.fragments.is_empty() {
            len += 1;
        }
        if !self.actor_status.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.TableFragments", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.fragments.is_empty() {
            struct_ser.serialize_field("fragments", &self.fragments)?;
        }
        if !self.actor_status.is_empty() {
            struct_ser.serialize_field("actorStatus", &self.actor_status)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableFragments {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "fragments",
            "actorStatus",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            Fragments,
            ActorStatus,
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
                            "fragments" => Ok(GeneratedField::Fragments),
                            "actorStatus" => Ok(GeneratedField::ActorStatus),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableFragments;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.TableFragments")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableFragments, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut fragments = None;
                let mut actor_status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::Fragments => {
                            if fragments.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragments"));
                            }
                            fragments = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                        GeneratedField::ActorStatus => {
                            if actor_status.is_some() {
                                return Err(serde::de::Error::duplicate_field("actorStatus"));
                            }
                            actor_status = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                    }
                }
                Ok(TableFragments {
                    table_ref_id,
                    fragments: fragments.unwrap_or_default(),
                    actor_status: actor_status.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.TableFragments", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for table_fragments::ActorState {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Inactive => "INACTIVE",
            Self::Running => "RUNNING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for table_fragments::ActorState {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INACTIVE",
            "RUNNING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_fragments::ActorState;

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
                    .and_then(table_fragments::ActorState::from_i32)
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
                    .and_then(table_fragments::ActorState::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INACTIVE" => Ok(table_fragments::ActorState::Inactive),
                    "RUNNING" => Ok(table_fragments::ActorState::Running),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for table_fragments::ActorStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.node_id != 0 {
            len += 1;
        }
        if self.state != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.TableFragments.ActorStatus", len)?;
        if self.node_id != 0 {
            struct_ser.serialize_field("nodeId", &self.node_id)?;
        }
        if self.state != 0 {
            let v = table_fragments::ActorState::from_i32(self.state)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.state)))?;
            struct_ser.serialize_field("state", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for table_fragments::ActorStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "nodeId",
            "state",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NodeId,
            State,
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
                            "nodeId" => Ok(GeneratedField::NodeId),
                            "state" => Ok(GeneratedField::State),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_fragments::ActorStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.TableFragments.ActorStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<table_fragments::ActorStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node_id = None;
                let mut state = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::NodeId => {
                            if node_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nodeId"));
                            }
                            node_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::State => {
                            if state.is_some() {
                                return Err(serde::de::Error::duplicate_field("state"));
                            }
                            state = Some(map.next_value::<table_fragments::ActorState>()? as i32);
                        }
                    }
                }
                Ok(table_fragments::ActorStatus {
                    node_id: node_id.unwrap_or_default(),
                    state: state.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.TableFragments.ActorStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for table_fragments::Fragment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.fragment_id != 0 {
            len += 1;
        }
        if self.fragment_type != 0 {
            len += 1;
        }
        if self.distribution_type != 0 {
            len += 1;
        }
        if !self.actors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("meta.TableFragments.Fragment", len)?;
        if self.fragment_id != 0 {
            struct_ser.serialize_field("fragmentId", &self.fragment_id)?;
        }
        if self.fragment_type != 0 {
            let v = table_fragments::fragment::FragmentType::from_i32(self.fragment_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.fragment_type)))?;
            struct_ser.serialize_field("fragmentType", &v)?;
        }
        if self.distribution_type != 0 {
            let v = table_fragments::fragment::FragmentDistributionType::from_i32(self.distribution_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.distribution_type)))?;
            struct_ser.serialize_field("distributionType", &v)?;
        }
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for table_fragments::Fragment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fragmentId",
            "fragmentType",
            "distributionType",
            "actors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FragmentId,
            FragmentType,
            DistributionType,
            Actors,
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
                            "fragmentId" => Ok(GeneratedField::FragmentId),
                            "fragmentType" => Ok(GeneratedField::FragmentType),
                            "distributionType" => Ok(GeneratedField::DistributionType),
                            "actors" => Ok(GeneratedField::Actors),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_fragments::Fragment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct meta.TableFragments.Fragment")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<table_fragments::Fragment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fragment_id = None;
                let mut fragment_type = None;
                let mut distribution_type = None;
                let mut actors = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::FragmentId => {
                            if fragment_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragmentId"));
                            }
                            fragment_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::FragmentType => {
                            if fragment_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("fragmentType"));
                            }
                            fragment_type = Some(map.next_value::<table_fragments::fragment::FragmentType>()? as i32);
                        }
                        GeneratedField::DistributionType => {
                            if distribution_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("distributionType"));
                            }
                            distribution_type = Some(map.next_value::<table_fragments::fragment::FragmentDistributionType>()? as i32);
                        }
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(map.next_value()?);
                        }
                    }
                }
                Ok(table_fragments::Fragment {
                    fragment_id: fragment_id.unwrap_or_default(),
                    fragment_type: fragment_type.unwrap_or_default(),
                    distribution_type: distribution_type.unwrap_or_default(),
                    actors: actors.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("meta.TableFragments.Fragment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for table_fragments::fragment::FragmentDistributionType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Single => "SINGLE",
            Self::Hash => "HASH",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for table_fragments::fragment::FragmentDistributionType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SINGLE",
            "HASH",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_fragments::fragment::FragmentDistributionType;

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
                    .and_then(table_fragments::fragment::FragmentDistributionType::from_i32)
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
                    .and_then(table_fragments::fragment::FragmentDistributionType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SINGLE" => Ok(table_fragments::fragment::FragmentDistributionType::Single),
                    "HASH" => Ok(table_fragments::fragment::FragmentDistributionType::Hash),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for table_fragments::fragment::FragmentType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Source => "SOURCE",
            Self::Sink => "SINK",
            Self::Others => "OTHERS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for table_fragments::fragment::FragmentType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SOURCE",
            "SINK",
            "OTHERS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_fragments::fragment::FragmentType;

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
                    .and_then(table_fragments::fragment::FragmentType::from_i32)
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
                    .and_then(table_fragments::fragment::FragmentType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SOURCE" => Ok(table_fragments::fragment::FragmentType::Source),
                    "SINK" => Ok(table_fragments::fragment::FragmentType::Sink),
                    "OTHERS" => Ok(table_fragments::fragment::FragmentType::Others),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
