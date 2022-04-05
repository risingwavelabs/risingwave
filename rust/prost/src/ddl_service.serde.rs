use crate::ddl_service::*;
impl serde::Serialize for CreateDatabaseRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.db.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateDatabaseRequest", len)?;
        if let Some(v) = self.db.as_ref() {
            struct_ser.serialize_field("db", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateDatabaseRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "db",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Db,
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
                            "db" => Ok(GeneratedField::Db),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateDatabaseRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateDatabaseRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateDatabaseRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut db = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Db => {
                            if db.is_some() {
                                return Err(serde::de::Error::duplicate_field("db"));
                            }
                            db = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateDatabaseRequest {
                    db,
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateDatabaseRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateDatabaseResponse {
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
        if self.database_id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateDatabaseResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.database_id != 0 {
            struct_ser.serialize_field("databaseId", &self.database_id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateDatabaseResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "databaseId",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            DatabaseId,
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
                            "databaseId" => Ok(GeneratedField::DatabaseId),
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
            type Value = CreateDatabaseResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateDatabaseResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateDatabaseResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut database_id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::DatabaseId => {
                            if database_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseId"));
                            }
                            database_id = Some(
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
                Ok(CreateDatabaseResponse {
                    status,
                    database_id: database_id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateDatabaseResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateMaterializedSourceRequest {
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
        if self.materialized_view.is_some() {
            len += 1;
        }
        if self.stream_node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateMaterializedSourceRequest", len)?;
        if let Some(v) = self.source.as_ref() {
            struct_ser.serialize_field("source", v)?;
        }
        if let Some(v) = self.materialized_view.as_ref() {
            struct_ser.serialize_field("materializedView", v)?;
        }
        if let Some(v) = self.stream_node.as_ref() {
            struct_ser.serialize_field("streamNode", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateMaterializedSourceRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "source",
            "materializedView",
            "streamNode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Source,
            MaterializedView,
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
                            "source" => Ok(GeneratedField::Source),
                            "materializedView" => Ok(GeneratedField::MaterializedView),
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
            type Value = CreateMaterializedSourceRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateMaterializedSourceRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedSourceRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source = None;
                let mut materialized_view = None;
                let mut stream_node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Source => {
                            if source.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source = Some(map.next_value()?);
                        }
                        GeneratedField::MaterializedView => {
                            if materialized_view.is_some() {
                                return Err(serde::de::Error::duplicate_field("materializedView"));
                            }
                            materialized_view = Some(map.next_value()?);
                        }
                        GeneratedField::StreamNode => {
                            if stream_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamNode"));
                            }
                            stream_node = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateMaterializedSourceRequest {
                    source,
                    materialized_view,
                    stream_node,
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateMaterializedSourceRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateMaterializedSourceResponse {
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
        if self.source_id != 0 {
            len += 1;
        }
        if self.table_id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateMaterializedSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.source_id != 0 {
            struct_ser.serialize_field("sourceId", &self.source_id)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateMaterializedSourceResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "sourceId",
            "tableId",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            SourceId,
            TableId,
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
                            "sourceId" => Ok(GeneratedField::SourceId),
                            "tableId" => Ok(GeneratedField::TableId),
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
            type Value = CreateMaterializedSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateMaterializedSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedSourceResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut source_id = None;
                let mut table_id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::SourceId => {
                            if source_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceId"));
                            }
                            source_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::TableId => {
                            if table_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id = Some(
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
                Ok(CreateMaterializedSourceResponse {
                    status,
                    source_id: source_id.unwrap_or_default(),
                    table_id: table_id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateMaterializedSourceResponse", FIELDS, GeneratedVisitor)
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
        if self.materialized_view.is_some() {
            len += 1;
        }
        if self.stream_node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateMaterializedViewRequest", len)?;
        if let Some(v) = self.materialized_view.as_ref() {
            struct_ser.serialize_field("materializedView", v)?;
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
            "materializedView",
            "streamNode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            MaterializedView,
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
                            "materializedView" => Ok(GeneratedField::MaterializedView),
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
                formatter.write_str("struct ddl_service.CreateMaterializedViewRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedViewRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut materialized_view = None;
                let mut stream_node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::MaterializedView => {
                            if materialized_view.is_some() {
                                return Err(serde::de::Error::duplicate_field("materializedView"));
                            }
                            materialized_view = Some(map.next_value()?);
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
                    materialized_view,
                    stream_node,
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateMaterializedViewRequest", FIELDS, GeneratedVisitor)
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
        if self.table_id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateMaterializedViewResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
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
            "tableId",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            TableId,
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
                            "tableId" => Ok(GeneratedField::TableId),
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
            type Value = CreateMaterializedViewResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateMaterializedViewResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateMaterializedViewResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut table_id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::TableId => {
                            if table_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id = Some(
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
                Ok(CreateMaterializedViewResponse {
                    status,
                    table_id: table_id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateMaterializedViewResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateSchemaRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateSchemaRequest", len)?;
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateSchemaRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
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
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateSchemaRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateSchemaRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSchemaRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateSchemaRequest {
                    schema,
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateSchemaRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateSchemaResponse {
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
        if self.schema_id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateSchemaResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.schema_id != 0 {
            struct_ser.serialize_field("schemaId", &self.schema_id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateSchemaResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "schemaId",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            SchemaId,
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
                            "schemaId" => Ok(GeneratedField::SchemaId),
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
            type Value = CreateSchemaResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateSchemaResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSchemaResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut schema_id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::SchemaId => {
                            if schema_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaId"));
                            }
                            schema_id = Some(
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
                Ok(CreateSchemaResponse {
                    status,
                    schema_id: schema_id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateSchemaResponse", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateSourceRequest", len)?;
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
                formatter.write_str("struct ddl_service.CreateSourceRequest")
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
        deserializer.deserialize_struct("ddl_service.CreateSourceRequest", FIELDS, GeneratedVisitor)
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
        if self.source_id != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.CreateSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.source_id != 0 {
            struct_ser.serialize_field("sourceId", &self.source_id)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
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
            "sourceId",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            SourceId,
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
                            "sourceId" => Ok(GeneratedField::SourceId),
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
            type Value = CreateSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.CreateSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSourceResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut source_id = None;
                let mut version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::SourceId => {
                            if source_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceId"));
                            }
                            source_id = Some(
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
                Ok(CreateSourceResponse {
                    status,
                    source_id: source_id.unwrap_or_default(),
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.CreateSourceResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropDatabaseRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.database_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropDatabaseRequest", len)?;
        if self.database_id != 0 {
            struct_ser.serialize_field("databaseId", &self.database_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropDatabaseRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "databaseId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DatabaseId,
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
                            "databaseId" => Ok(GeneratedField::DatabaseId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropDatabaseRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropDatabaseRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropDatabaseRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DatabaseId => {
                            if database_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseId"));
                            }
                            database_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropDatabaseRequest {
                    database_id: database_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropDatabaseRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropDatabaseResponse {
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
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropDatabaseResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropDatabaseResponse {
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
            type Value = DropDatabaseResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropDatabaseResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropDatabaseResponse, V::Error>
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
                Ok(DropDatabaseResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropDatabaseResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropMaterializedSourceRequest {
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
        if self.table_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropMaterializedSourceRequest", len)?;
        if self.source_id != 0 {
            struct_ser.serialize_field("sourceId", &self.source_id)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropMaterializedSourceRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sourceId",
            "tableId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SourceId,
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
                            "sourceId" => Ok(GeneratedField::SourceId),
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
            type Value = DropMaterializedSourceRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropMaterializedSourceRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedSourceRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut source_id = None;
                let mut table_id = None;
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
                        GeneratedField::TableId => {
                            if table_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropMaterializedSourceRequest {
                    source_id: source_id.unwrap_or_default(),
                    table_id: table_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropMaterializedSourceRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropMaterializedSourceResponse {
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
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropMaterializedSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropMaterializedSourceResponse {
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
            type Value = DropMaterializedSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropMaterializedSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedSourceResponse, V::Error>
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
                Ok(DropMaterializedSourceResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropMaterializedSourceResponse", FIELDS, GeneratedVisitor)
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
        if self.table_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropMaterializedViewRequest", len)?;
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
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
            "tableId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = DropMaterializedViewRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropMaterializedViewRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedViewRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableId => {
                            if table_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropMaterializedViewRequest {
                    table_id: table_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropMaterializedViewRequest", FIELDS, GeneratedVisitor)
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
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropMaterializedViewResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
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
            type Value = DropMaterializedViewResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropMaterializedViewResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropMaterializedViewResponse, V::Error>
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
                Ok(DropMaterializedViewResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropMaterializedViewResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropSchemaRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropSchemaRequest", len)?;
        if self.schema_id != 0 {
            struct_ser.serialize_field("schemaId", &self.schema_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropSchemaRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schemaId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaId,
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
                            "schemaId" => Ok(GeneratedField::SchemaId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropSchemaRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropSchemaRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSchemaRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SchemaId => {
                            if schema_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaId"));
                            }
                            schema_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(DropSchemaRequest {
                    schema_id: schema_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropSchemaRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropSchemaResponse {
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
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropSchemaResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropSchemaResponse {
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
            type Value = DropSchemaResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropSchemaResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSchemaResponse, V::Error>
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
                Ok(DropSchemaResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropSchemaResponse", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropSourceRequest", len)?;
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
                formatter.write_str("struct ddl_service.DropSourceRequest")
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
        deserializer.deserialize_struct("ddl_service.DropSourceRequest", FIELDS, GeneratedVisitor)
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
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("ddl_service.DropSourceResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.version != 0 {
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
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
            type Value = DropSourceResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct ddl_service.DropSourceResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSourceResponse, V::Error>
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
                Ok(DropSourceResponse {
                    status,
                    version: version.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("ddl_service.DropSourceResponse", FIELDS, GeneratedVisitor)
    }
}
