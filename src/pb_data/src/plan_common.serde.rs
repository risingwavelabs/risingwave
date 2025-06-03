use crate::plan_common::*;
impl serde::Serialize for AdditionalCollectionName {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalCollectionName", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalCollectionName {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalCollectionName;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalCollectionName")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalCollectionName, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalCollectionName {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalCollectionName", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.AdditionalColumn", len)?;
        if let Some(v) = self.column_type.as_ref() {
            match v {
                additional_column::ColumnType::Key(v) => {
                    struct_ser.serialize_field("key", v)?;
                }
                additional_column::ColumnType::Timestamp(v) => {
                    struct_ser.serialize_field("timestamp", v)?;
                }
                additional_column::ColumnType::Partition(v) => {
                    struct_ser.serialize_field("partition", v)?;
                }
                additional_column::ColumnType::Offset(v) => {
                    struct_ser.serialize_field("offset", v)?;
                }
                additional_column::ColumnType::HeaderInner(v) => {
                    struct_ser.serialize_field("headerInner", v)?;
                }
                additional_column::ColumnType::Filename(v) => {
                    struct_ser.serialize_field("filename", v)?;
                }
                additional_column::ColumnType::Headers(v) => {
                    struct_ser.serialize_field("headers", v)?;
                }
                additional_column::ColumnType::DatabaseName(v) => {
                    struct_ser.serialize_field("databaseName", v)?;
                }
                additional_column::ColumnType::SchemaName(v) => {
                    struct_ser.serialize_field("schemaName", v)?;
                }
                additional_column::ColumnType::TableName(v) => {
                    struct_ser.serialize_field("tableName", v)?;
                }
                additional_column::ColumnType::CollectionName(v) => {
                    struct_ser.serialize_field("collectionName", v)?;
                }
                additional_column::ColumnType::Payload(v) => {
                    struct_ser.serialize_field("payload", v)?;
                }
                additional_column::ColumnType::Subject(v) => {
                    struct_ser.serialize_field("subject", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
            "timestamp",
            "partition",
            "offset",
            "header_inner",
            "headerInner",
            "filename",
            "headers",
            "database_name",
            "databaseName",
            "schema_name",
            "schemaName",
            "table_name",
            "tableName",
            "collection_name",
            "collectionName",
            "payload",
            "subject",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
            Timestamp,
            Partition,
            Offset,
            HeaderInner,
            Filename,
            Headers,
            DatabaseName,
            SchemaName,
            TableName,
            CollectionName,
            Payload,
            Subject,
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
                            "key" => Ok(GeneratedField::Key),
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            "partition" => Ok(GeneratedField::Partition),
                            "offset" => Ok(GeneratedField::Offset),
                            "headerInner" | "header_inner" => Ok(GeneratedField::HeaderInner),
                            "filename" => Ok(GeneratedField::Filename),
                            "headers" => Ok(GeneratedField::Headers),
                            "databaseName" | "database_name" => Ok(GeneratedField::DatabaseName),
                            "schemaName" | "schema_name" => Ok(GeneratedField::SchemaName),
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "collectionName" | "collection_name" => Ok(GeneratedField::CollectionName),
                            "payload" => Ok(GeneratedField::Payload),
                            "subject" => Ok(GeneratedField::Subject),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdditionalColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumn")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Key)
;
                        }
                        GeneratedField::Timestamp => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Timestamp)
;
                        }
                        GeneratedField::Partition => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partition"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Partition)
;
                        }
                        GeneratedField::Offset => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Offset)
;
                        }
                        GeneratedField::HeaderInner => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("headerInner"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::HeaderInner)
;
                        }
                        GeneratedField::Filename => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filename"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Filename)
;
                        }
                        GeneratedField::Headers => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("headers"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Headers)
;
                        }
                        GeneratedField::DatabaseName => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseName"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::DatabaseName)
;
                        }
                        GeneratedField::SchemaName => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::SchemaName)
;
                        }
                        GeneratedField::TableName => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::TableName)
;
                        }
                        GeneratedField::CollectionName => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collectionName"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::CollectionName)
;
                        }
                        GeneratedField::Payload => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("payload"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Payload)
;
                        }
                        GeneratedField::Subject => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subject"));
                            }
                            column_type__ = map_.next_value::<::std::option::Option<_>>()?.map(additional_column::ColumnType::Subject)
;
                        }
                    }
                }
                Ok(AdditionalColumn {
                    column_type: column_type__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnFilename {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnFilename", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnFilename {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnFilename;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnFilename")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnFilename, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnFilename {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnFilename", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnHeader {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inner_field.is_empty() {
            len += 1;
        }
        if self.data_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnHeader", len)?;
        if !self.inner_field.is_empty() {
            struct_ser.serialize_field("innerField", &self.inner_field)?;
        }
        if let Some(v) = self.data_type.as_ref() {
            struct_ser.serialize_field("dataType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnHeader {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inner_field",
            "innerField",
            "data_type",
            "dataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            InnerField,
            DataType,
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
                            "innerField" | "inner_field" => Ok(GeneratedField::InnerField),
                            "dataType" | "data_type" => Ok(GeneratedField::DataType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdditionalColumnHeader;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnHeader")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnHeader, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inner_field__ = None;
                let mut data_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::InnerField => {
                            if inner_field__.is_some() {
                                return Err(serde::de::Error::duplicate_field("innerField"));
                            }
                            inner_field__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DataType => {
                            if data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AdditionalColumnHeader {
                    inner_field: inner_field__.unwrap_or_default(),
                    data_type: data_type__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnHeader", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnHeaders {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnHeaders", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnHeaders {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnHeaders;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnHeaders")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnHeaders, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnHeaders {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnHeaders", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnKey {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnKey", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnKey {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnKey")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnKey, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnKey {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnKey", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnOffset {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnOffset", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnOffset {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnOffset;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnOffset")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnOffset, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnOffset {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnOffset", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnPartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnPartition", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnPartition {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnPartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnPartition")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnPartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnPartition {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnPartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnPayload {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnPayload", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnPayload {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnPayload;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnPayload")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnPayload, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnPayload {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnPayload", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnTimestamp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalColumnTimestamp", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnTimestamp {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalColumnTimestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalColumnTimestamp")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalColumnTimestamp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalColumnTimestamp {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalColumnTimestamp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalColumnType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "ADDITIONAL_COLUMN_TYPE_UNSPECIFIED",
            Self::Key => "ADDITIONAL_COLUMN_TYPE_KEY",
            Self::Timestamp => "ADDITIONAL_COLUMN_TYPE_TIMESTAMP",
            Self::Partition => "ADDITIONAL_COLUMN_TYPE_PARTITION",
            Self::Offset => "ADDITIONAL_COLUMN_TYPE_OFFSET",
            Self::Header => "ADDITIONAL_COLUMN_TYPE_HEADER",
            Self::Filename => "ADDITIONAL_COLUMN_TYPE_FILENAME",
            Self::Normal => "ADDITIONAL_COLUMN_TYPE_NORMAL",
            Self::Payload => "ADDITIONAL_COLUMN_TYPE_PAYLOAD",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalColumnType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ADDITIONAL_COLUMN_TYPE_UNSPECIFIED",
            "ADDITIONAL_COLUMN_TYPE_KEY",
            "ADDITIONAL_COLUMN_TYPE_TIMESTAMP",
            "ADDITIONAL_COLUMN_TYPE_PARTITION",
            "ADDITIONAL_COLUMN_TYPE_OFFSET",
            "ADDITIONAL_COLUMN_TYPE_HEADER",
            "ADDITIONAL_COLUMN_TYPE_FILENAME",
            "ADDITIONAL_COLUMN_TYPE_NORMAL",
            "ADDITIONAL_COLUMN_TYPE_PAYLOAD",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AdditionalColumnType;

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
                    "ADDITIONAL_COLUMN_TYPE_UNSPECIFIED" => Ok(AdditionalColumnType::Unspecified),
                    "ADDITIONAL_COLUMN_TYPE_KEY" => Ok(AdditionalColumnType::Key),
                    "ADDITIONAL_COLUMN_TYPE_TIMESTAMP" => Ok(AdditionalColumnType::Timestamp),
                    "ADDITIONAL_COLUMN_TYPE_PARTITION" => Ok(AdditionalColumnType::Partition),
                    "ADDITIONAL_COLUMN_TYPE_OFFSET" => Ok(AdditionalColumnType::Offset),
                    "ADDITIONAL_COLUMN_TYPE_HEADER" => Ok(AdditionalColumnType::Header),
                    "ADDITIONAL_COLUMN_TYPE_FILENAME" => Ok(AdditionalColumnType::Filename),
                    "ADDITIONAL_COLUMN_TYPE_NORMAL" => Ok(AdditionalColumnType::Normal),
                    "ADDITIONAL_COLUMN_TYPE_PAYLOAD" => Ok(AdditionalColumnType::Payload),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalDatabaseName {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalDatabaseName", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalDatabaseName {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalDatabaseName;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalDatabaseName")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalDatabaseName, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalDatabaseName {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalDatabaseName", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalSchemaName {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalSchemaName", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalSchemaName {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalSchemaName;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalSchemaName")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalSchemaName, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalSchemaName {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalSchemaName", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalSubject {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalSubject", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalSubject {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalSubject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalSubject")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalSubject, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalSubject {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalSubject", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AdditionalTableName {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AdditionalTableName", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AdditionalTableName {
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

                    #[allow(unused_variables)]
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
            type Value = AdditionalTableName;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AdditionalTableName")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AdditionalTableName, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(AdditionalTableName {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AdditionalTableName", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AsOf {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.as_of_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.AsOf", len)?;
        if let Some(v) = self.as_of_type.as_ref() {
            match v {
                as_of::AsOfType::ProcessTime(v) => {
                    struct_ser.serialize_field("processTime", v)?;
                }
                as_of::AsOfType::Timestamp(v) => {
                    struct_ser.serialize_field("timestamp", v)?;
                }
                as_of::AsOfType::Version(v) => {
                    struct_ser.serialize_field("version", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AsOf {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "process_time",
            "processTime",
            "timestamp",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProcessTime,
            Timestamp,
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "processTime" | "process_time" => Ok(GeneratedField::ProcessTime),
                            "timestamp" => Ok(GeneratedField::Timestamp),
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
            type Value = AsOf;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AsOf")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AsOf, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut as_of_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ProcessTime => {
                            if as_of_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("processTime"));
                            }
                            as_of_type__ = map_.next_value::<::std::option::Option<_>>()?.map(as_of::AsOfType::ProcessTime)
;
                        }
                        GeneratedField::Timestamp => {
                            if as_of_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            as_of_type__ = map_.next_value::<::std::option::Option<_>>()?.map(as_of::AsOfType::Timestamp)
;
                        }
                        GeneratedField::Version => {
                            if as_of_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            as_of_type__ = map_.next_value::<::std::option::Option<_>>()?.map(as_of::AsOfType::Version)
;
                        }
                    }
                }
                Ok(AsOf {
                    as_of_type: as_of_type__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AsOf", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for as_of::ProcessTime {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan_common.AsOf.ProcessTime", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for as_of::ProcessTime {
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

                    #[allow(unused_variables)]
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
            type Value = as_of::ProcessTime;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AsOf.ProcessTime")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<as_of::ProcessTime, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(as_of::ProcessTime {
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AsOf.ProcessTime", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for as_of::Timestamp {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.timestamp != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.AsOf.Timestamp", len)?;
        if self.timestamp != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("timestamp", ToString::to_string(&self.timestamp).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for as_of::Timestamp {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "timestamp",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Timestamp,
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
                            "timestamp" => Ok(GeneratedField::Timestamp),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = as_of::Timestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AsOf.Timestamp")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<as_of::Timestamp, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut timestamp__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Timestamp => {
                            if timestamp__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestamp"));
                            }
                            timestamp__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(as_of::Timestamp {
                    timestamp: timestamp__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AsOf.Timestamp", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for as_of::Version {
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
        let mut struct_ser = serializer.serialize_struct("plan_common.AsOf.Version", len)?;
        if self.version != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("version", ToString::to_string(&self.version).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for as_of::Version {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
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
            type Value = as_of::Version;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AsOf.Version")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<as_of::Version, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(as_of::Version {
                    version: version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AsOf.Version", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AsOfJoinDesc {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.right_idx != 0 {
            len += 1;
        }
        if self.left_idx != 0 {
            len += 1;
        }
        if self.inequality_type != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.AsOfJoinDesc", len)?;
        if self.right_idx != 0 {
            struct_ser.serialize_field("rightIdx", &self.right_idx)?;
        }
        if self.left_idx != 0 {
            struct_ser.serialize_field("leftIdx", &self.left_idx)?;
        }
        if self.inequality_type != 0 {
            let v = AsOfJoinInequalityType::try_from(self.inequality_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.inequality_type)))?;
            struct_ser.serialize_field("inequalityType", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AsOfJoinDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "right_idx",
            "rightIdx",
            "left_idx",
            "leftIdx",
            "inequality_type",
            "inequalityType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RightIdx,
            LeftIdx,
            InequalityType,
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
                            "rightIdx" | "right_idx" => Ok(GeneratedField::RightIdx),
                            "leftIdx" | "left_idx" => Ok(GeneratedField::LeftIdx),
                            "inequalityType" | "inequality_type" => Ok(GeneratedField::InequalityType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AsOfJoinDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.AsOfJoinDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AsOfJoinDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut right_idx__ = None;
                let mut left_idx__ = None;
                let mut inequality_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RightIdx => {
                            if right_idx__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightIdx"));
                            }
                            right_idx__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LeftIdx => {
                            if left_idx__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftIdx"));
                            }
                            left_idx__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::InequalityType => {
                            if inequality_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inequalityType"));
                            }
                            inequality_type__ = Some(map_.next_value::<AsOfJoinInequalityType>()? as i32);
                        }
                    }
                }
                Ok(AsOfJoinDesc {
                    right_idx: right_idx__.unwrap_or_default(),
                    left_idx: left_idx__.unwrap_or_default(),
                    inequality_type: inequality_type__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.AsOfJoinDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AsOfJoinInequalityType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::AsOfInequalityTypeUnspecified => "AS_OF_INEQUALITY_TYPE_UNSPECIFIED",
            Self::AsOfInequalityTypeGt => "AS_OF_INEQUALITY_TYPE_GT",
            Self::AsOfInequalityTypeGe => "AS_OF_INEQUALITY_TYPE_GE",
            Self::AsOfInequalityTypeLt => "AS_OF_INEQUALITY_TYPE_LT",
            Self::AsOfInequalityTypeLe => "AS_OF_INEQUALITY_TYPE_LE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AsOfJoinInequalityType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "AS_OF_INEQUALITY_TYPE_UNSPECIFIED",
            "AS_OF_INEQUALITY_TYPE_GT",
            "AS_OF_INEQUALITY_TYPE_GE",
            "AS_OF_INEQUALITY_TYPE_LT",
            "AS_OF_INEQUALITY_TYPE_LE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AsOfJoinInequalityType;

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
                    "AS_OF_INEQUALITY_TYPE_UNSPECIFIED" => Ok(AsOfJoinInequalityType::AsOfInequalityTypeUnspecified),
                    "AS_OF_INEQUALITY_TYPE_GT" => Ok(AsOfJoinInequalityType::AsOfInequalityTypeGt),
                    "AS_OF_INEQUALITY_TYPE_GE" => Ok(AsOfJoinInequalityType::AsOfInequalityTypeGe),
                    "AS_OF_INEQUALITY_TYPE_LT" => Ok(AsOfJoinInequalityType::AsOfInequalityTypeLt),
                    "AS_OF_INEQUALITY_TYPE_LE" => Ok(AsOfJoinInequalityType::AsOfInequalityTypeLe),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AsOfJoinType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "AS_OF_JOIN_TYPE_UNSPECIFIED",
            Self::Inner => "AS_OF_JOIN_TYPE_INNER",
            Self::LeftOuter => "AS_OF_JOIN_TYPE_LEFT_OUTER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AsOfJoinType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "AS_OF_JOIN_TYPE_UNSPECIFIED",
            "AS_OF_JOIN_TYPE_INNER",
            "AS_OF_JOIN_TYPE_LEFT_OUTER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AsOfJoinType;

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
                    "AS_OF_JOIN_TYPE_UNSPECIFIED" => Ok(AsOfJoinType::Unspecified),
                    "AS_OF_JOIN_TYPE_INNER" => Ok(AsOfJoinType::Inner),
                    "AS_OF_JOIN_TYPE_LEFT_OUTER" => Ok(AsOfJoinType::LeftOuter),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Cardinality {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.lo != 0 {
            len += 1;
        }
        if self.hi.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.Cardinality", len)?;
        if self.lo != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("lo", ToString::to_string(&self.lo).as_str())?;
        }
        if let Some(v) = self.hi.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("hi", ToString::to_string(&v).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Cardinality {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "lo",
            "hi",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Lo,
            Hi,
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
                            "lo" => Ok(GeneratedField::Lo),
                            "hi" => Ok(GeneratedField::Hi),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Cardinality;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.Cardinality")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Cardinality, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut lo__ = None;
                let mut hi__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Lo => {
                            if lo__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lo"));
                            }
                            lo__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Hi => {
                            if hi__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hi"));
                            }
                            hi__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(Cardinality {
                    lo: lo__.unwrap_or_default(),
                    hi: hi__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.Cardinality", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnCatalog {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column_desc.is_some() {
            len += 1;
        }
        if self.is_hidden {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.ColumnCatalog", len)?;
        if let Some(v) = self.column_desc.as_ref() {
            struct_ser.serialize_field("columnDesc", v)?;
        }
        if self.is_hidden {
            struct_ser.serialize_field("isHidden", &self.is_hidden)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnCatalog {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column_desc",
            "columnDesc",
            "is_hidden",
            "isHidden",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnDesc,
            IsHidden,
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
                            "columnDesc" | "column_desc" => Ok(GeneratedField::ColumnDesc),
                            "isHidden" | "is_hidden" => Ok(GeneratedField::IsHidden),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnCatalog;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.ColumnCatalog")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnCatalog, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_desc__ = None;
                let mut is_hidden__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ColumnDesc => {
                            if column_desc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDesc"));
                            }
                            column_desc__ = map_.next_value()?;
                        }
                        GeneratedField::IsHidden => {
                            if is_hidden__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isHidden"));
                            }
                            is_hidden__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ColumnCatalog {
                    column_desc: column_desc__,
                    is_hidden: is_hidden__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.ColumnCatalog", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnDesc {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column_type.is_some() {
            len += 1;
        }
        if self.column_id != 0 {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if self.description.is_some() {
            len += 1;
        }
        if self.additional_column_type != 0 {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        if self.additional_column.is_some() {
            len += 1;
        }
        if self.nullable {
            len += 1;
        }
        if self.generated_or_default_column.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.ColumnDesc", len)?;
        if let Some(v) = self.column_type.as_ref() {
            struct_ser.serialize_field("columnType", v)?;
        }
        if self.column_id != 0 {
            struct_ser.serialize_field("columnId", &self.column_id)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.description.as_ref() {
            struct_ser.serialize_field("description", v)?;
        }
        if self.additional_column_type != 0 {
            let v = AdditionalColumnType::try_from(self.additional_column_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.additional_column_type)))?;
            struct_ser.serialize_field("additionalColumnType", &v)?;
        }
        if self.version != 0 {
            let v = ColumnDescVersion::try_from(self.version)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.version)))?;
            struct_ser.serialize_field("version", &v)?;
        }
        if let Some(v) = self.additional_column.as_ref() {
            struct_ser.serialize_field("additionalColumn", v)?;
        }
        if self.nullable {
            struct_ser.serialize_field("nullable", &self.nullable)?;
        }
        if let Some(v) = self.generated_or_default_column.as_ref() {
            match v {
                column_desc::GeneratedOrDefaultColumn::GeneratedColumn(v) => {
                    struct_ser.serialize_field("generatedColumn", v)?;
                }
                column_desc::GeneratedOrDefaultColumn::DefaultColumn(v) => {
                    struct_ser.serialize_field("defaultColumn", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column_type",
            "columnType",
            "column_id",
            "columnId",
            "name",
            "description",
            "additional_column_type",
            "additionalColumnType",
            "version",
            "additional_column",
            "additionalColumn",
            "nullable",
            "generated_column",
            "generatedColumn",
            "default_column",
            "defaultColumn",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnType,
            ColumnId,
            Name,
            Description,
            AdditionalColumnType,
            Version,
            AdditionalColumn,
            Nullable,
            GeneratedColumn,
            DefaultColumn,
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
                            "columnType" | "column_type" => Ok(GeneratedField::ColumnType),
                            "columnId" | "column_id" => Ok(GeneratedField::ColumnId),
                            "name" => Ok(GeneratedField::Name),
                            "description" => Ok(GeneratedField::Description),
                            "additionalColumnType" | "additional_column_type" => Ok(GeneratedField::AdditionalColumnType),
                            "version" => Ok(GeneratedField::Version),
                            "additionalColumn" | "additional_column" => Ok(GeneratedField::AdditionalColumn),
                            "nullable" => Ok(GeneratedField::Nullable),
                            "generatedColumn" | "generated_column" => Ok(GeneratedField::GeneratedColumn),
                            "defaultColumn" | "default_column" => Ok(GeneratedField::DefaultColumn),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.ColumnDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_type__ = None;
                let mut column_id__ = None;
                let mut name__ = None;
                let mut description__ = None;
                let mut additional_column_type__ = None;
                let mut version__ = None;
                let mut additional_column__ = None;
                let mut nullable__ = None;
                let mut generated_or_default_column__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ColumnType => {
                            if column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnType"));
                            }
                            column_type__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnId => {
                            if column_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnId"));
                            }
                            column_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Description => {
                            if description__.is_some() {
                                return Err(serde::de::Error::duplicate_field("description"));
                            }
                            description__ = map_.next_value()?;
                        }
                        GeneratedField::AdditionalColumnType => {
                            if additional_column_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("additionalColumnType"));
                            }
                            additional_column_type__ = Some(map_.next_value::<AdditionalColumnType>()? as i32);
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value::<ColumnDescVersion>()? as i32);
                        }
                        GeneratedField::AdditionalColumn => {
                            if additional_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("additionalColumn"));
                            }
                            additional_column__ = map_.next_value()?;
                        }
                        GeneratedField::Nullable => {
                            if nullable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullable"));
                            }
                            nullable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::GeneratedColumn => {
                            if generated_or_default_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("generatedColumn"));
                            }
                            generated_or_default_column__ = map_.next_value::<::std::option::Option<_>>()?.map(column_desc::GeneratedOrDefaultColumn::GeneratedColumn)
;
                        }
                        GeneratedField::DefaultColumn => {
                            if generated_or_default_column__.is_some() {
                                return Err(serde::de::Error::duplicate_field("defaultColumn"));
                            }
                            generated_or_default_column__ = map_.next_value::<::std::option::Option<_>>()?.map(column_desc::GeneratedOrDefaultColumn::DefaultColumn)
;
                        }
                    }
                }
                Ok(ColumnDesc {
                    column_type: column_type__,
                    column_id: column_id__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    description: description__,
                    additional_column_type: additional_column_type__.unwrap_or_default(),
                    version: version__.unwrap_or_default(),
                    additional_column: additional_column__,
                    nullable: nullable__.unwrap_or_default(),
                    generated_or_default_column: generated_or_default_column__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.ColumnDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnDescVersion {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "COLUMN_DESC_VERSION_UNSPECIFIED",
            Self::Pr13707 => "COLUMN_DESC_VERSION_PR_13707",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ColumnDescVersion {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "COLUMN_DESC_VERSION_UNSPECIFIED",
            "COLUMN_DESC_VERSION_PR_13707",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnDescVersion;

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
                    "COLUMN_DESC_VERSION_UNSPECIFIED" => Ok(ColumnDescVersion::Unspecified),
                    "COLUMN_DESC_VERSION_PR_13707" => Ok(ColumnDescVersion::Pr13707),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for DefaultColumnDesc {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.snapshot_value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.DefaultColumnDesc", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.snapshot_value.as_ref() {
            struct_ser.serialize_field("snapshotValue", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DefaultColumnDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "snapshot_value",
            "snapshotValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            SnapshotValue,
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
                            "expr" => Ok(GeneratedField::Expr),
                            "snapshotValue" | "snapshot_value" => Ok(GeneratedField::SnapshotValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DefaultColumnDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.DefaultColumnDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DefaultColumnDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut snapshot_value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::SnapshotValue => {
                            if snapshot_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotValue"));
                            }
                            snapshot_value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DefaultColumnDesc {
                    expr: expr__,
                    snapshot_value: snapshot_value__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.DefaultColumnDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DefaultColumns {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.default_columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.DefaultColumns", len)?;
        if !self.default_columns.is_empty() {
            struct_ser.serialize_field("defaultColumns", &self.default_columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DefaultColumns {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "default_columns",
            "defaultColumns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DefaultColumns,
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
                            "defaultColumns" | "default_columns" => Ok(GeneratedField::DefaultColumns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DefaultColumns;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.DefaultColumns")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DefaultColumns, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut default_columns__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::DefaultColumns => {
                            if default_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("defaultColumns"));
                            }
                            default_columns__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DefaultColumns {
                    default_columns: default_columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.DefaultColumns", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EncodeType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "ENCODE_TYPE_UNSPECIFIED",
            Self::Native => "ENCODE_TYPE_NATIVE",
            Self::Avro => "ENCODE_TYPE_AVRO",
            Self::Csv => "ENCODE_TYPE_CSV",
            Self::Protobuf => "ENCODE_TYPE_PROTOBUF",
            Self::Json => "ENCODE_TYPE_JSON",
            Self::Bytes => "ENCODE_TYPE_BYTES",
            Self::Template => "ENCODE_TYPE_TEMPLATE",
            Self::None => "ENCODE_TYPE_NONE",
            Self::Text => "ENCODE_TYPE_TEXT",
            Self::Parquet => "ENCODE_TYPE_PARQUET",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for EncodeType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ENCODE_TYPE_UNSPECIFIED",
            "ENCODE_TYPE_NATIVE",
            "ENCODE_TYPE_AVRO",
            "ENCODE_TYPE_CSV",
            "ENCODE_TYPE_PROTOBUF",
            "ENCODE_TYPE_JSON",
            "ENCODE_TYPE_BYTES",
            "ENCODE_TYPE_TEMPLATE",
            "ENCODE_TYPE_NONE",
            "ENCODE_TYPE_TEXT",
            "ENCODE_TYPE_PARQUET",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EncodeType;

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
                    "ENCODE_TYPE_UNSPECIFIED" => Ok(EncodeType::Unspecified),
                    "ENCODE_TYPE_NATIVE" => Ok(EncodeType::Native),
                    "ENCODE_TYPE_AVRO" => Ok(EncodeType::Avro),
                    "ENCODE_TYPE_CSV" => Ok(EncodeType::Csv),
                    "ENCODE_TYPE_PROTOBUF" => Ok(EncodeType::Protobuf),
                    "ENCODE_TYPE_JSON" => Ok(EncodeType::Json),
                    "ENCODE_TYPE_BYTES" => Ok(EncodeType::Bytes),
                    "ENCODE_TYPE_TEMPLATE" => Ok(EncodeType::Template),
                    "ENCODE_TYPE_NONE" => Ok(EncodeType::None),
                    "ENCODE_TYPE_TEXT" => Ok(EncodeType::Text),
                    "ENCODE_TYPE_PARQUET" => Ok(EncodeType::Parquet),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ExprContext {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.time_zone.is_empty() {
            len += 1;
        }
        if self.strict_mode {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.ExprContext", len)?;
        if !self.time_zone.is_empty() {
            struct_ser.serialize_field("timeZone", &self.time_zone)?;
        }
        if self.strict_mode {
            struct_ser.serialize_field("strictMode", &self.strict_mode)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExprContext {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "time_zone",
            "timeZone",
            "strict_mode",
            "strictMode",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TimeZone,
            StrictMode,
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
                            "timeZone" | "time_zone" => Ok(GeneratedField::TimeZone),
                            "strictMode" | "strict_mode" => Ok(GeneratedField::StrictMode),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExprContext;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.ExprContext")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ExprContext, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut time_zone__ = None;
                let mut strict_mode__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TimeZone => {
                            if time_zone__.is_some() {
                                return Err(serde::de::Error::duplicate_field("timeZone"));
                            }
                            time_zone__ = Some(map_.next_value()?);
                        }
                        GeneratedField::StrictMode => {
                            if strict_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("strictMode"));
                            }
                            strict_mode__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ExprContext {
                    time_zone: time_zone__.unwrap_or_default(),
                    strict_mode: strict_mode__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.ExprContext", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExternalTableDesc {
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
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.pk.is_empty() {
            len += 1;
        }
        if !self.table_name.is_empty() {
            len += 1;
        }
        if !self.stream_key.is_empty() {
            len += 1;
        }
        if !self.connect_properties.is_empty() {
            len += 1;
        }
        if self.source_id != 0 {
            len += 1;
        }
        if !self.secret_refs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.ExternalTableDesc", len)?;
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.pk.is_empty() {
            struct_ser.serialize_field("pk", &self.pk)?;
        }
        if !self.table_name.is_empty() {
            struct_ser.serialize_field("tableName", &self.table_name)?;
        }
        if !self.stream_key.is_empty() {
            struct_ser.serialize_field("streamKey", &self.stream_key)?;
        }
        if !self.connect_properties.is_empty() {
            struct_ser.serialize_field("connectProperties", &self.connect_properties)?;
        }
        if self.source_id != 0 {
            struct_ser.serialize_field("sourceId", &self.source_id)?;
        }
        if !self.secret_refs.is_empty() {
            struct_ser.serialize_field("secretRefs", &self.secret_refs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExternalTableDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_id",
            "tableId",
            "columns",
            "pk",
            "table_name",
            "tableName",
            "stream_key",
            "streamKey",
            "connect_properties",
            "connectProperties",
            "source_id",
            "sourceId",
            "secret_refs",
            "secretRefs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableId,
            Columns,
            Pk,
            TableName,
            StreamKey,
            ConnectProperties,
            SourceId,
            SecretRefs,
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
                            "tableId" | "table_id" => Ok(GeneratedField::TableId),
                            "columns" => Ok(GeneratedField::Columns),
                            "pk" => Ok(GeneratedField::Pk),
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "streamKey" | "stream_key" => Ok(GeneratedField::StreamKey),
                            "connectProperties" | "connect_properties" => Ok(GeneratedField::ConnectProperties),
                            "sourceId" | "source_id" => Ok(GeneratedField::SourceId),
                            "secretRefs" | "secret_refs" => Ok(GeneratedField::SecretRefs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExternalTableDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.ExternalTableDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ExternalTableDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_id__ = None;
                let mut columns__ = None;
                let mut pk__ = None;
                let mut table_name__ = None;
                let mut stream_key__ = None;
                let mut connect_properties__ = None;
                let mut source_id__ = None;
                let mut secret_refs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TableId => {
                            if table_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Pk => {
                            if pk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pk"));
                            }
                            pk__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::StreamKey => {
                            if stream_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamKey"));
                            }
                            stream_key__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::ConnectProperties => {
                            if connect_properties__.is_some() {
                                return Err(serde::de::Error::duplicate_field("connectProperties"));
                            }
                            connect_properties__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                        GeneratedField::SourceId => {
                            if source_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceId"));
                            }
                            source_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::SecretRefs => {
                            if secret_refs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("secretRefs"));
                            }
                            secret_refs__ = Some(
                                map_.next_value::<std::collections::BTreeMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(ExternalTableDesc {
                    table_id: table_id__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                    pk: pk__.unwrap_or_default(),
                    table_name: table_name__.unwrap_or_default(),
                    stream_key: stream_key__.unwrap_or_default(),
                    connect_properties: connect_properties__.unwrap_or_default(),
                    source_id: source_id__.unwrap_or_default(),
                    secret_refs: secret_refs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.ExternalTableDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Field {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.data_type.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.Field", len)?;
        if let Some(v) = self.data_type.as_ref() {
            struct_ser.serialize_field("dataType", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Field {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "data_type",
            "dataType",
            "name",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DataType,
            Name,
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
                            "dataType" | "data_type" => Ok(GeneratedField::DataType),
                            "name" => Ok(GeneratedField::Name),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Field;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.Field")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Field, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut data_type__ = None;
                let mut name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::DataType => {
                            if data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Field {
                    data_type: data_type__,
                    name: name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan_common.Field", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FormatType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "FORMAT_TYPE_UNSPECIFIED",
            Self::Native => "FORMAT_TYPE_NATIVE",
            Self::Debezium => "FORMAT_TYPE_DEBEZIUM",
            Self::DebeziumMongo => "FORMAT_TYPE_DEBEZIUM_MONGO",
            Self::Maxwell => "FORMAT_TYPE_MAXWELL",
            Self::Canal => "FORMAT_TYPE_CANAL",
            Self::Upsert => "FORMAT_TYPE_UPSERT",
            Self::Plain => "FORMAT_TYPE_PLAIN",
            Self::None => "FORMAT_TYPE_NONE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for FormatType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "FORMAT_TYPE_UNSPECIFIED",
            "FORMAT_TYPE_NATIVE",
            "FORMAT_TYPE_DEBEZIUM",
            "FORMAT_TYPE_DEBEZIUM_MONGO",
            "FORMAT_TYPE_MAXWELL",
            "FORMAT_TYPE_CANAL",
            "FORMAT_TYPE_UPSERT",
            "FORMAT_TYPE_PLAIN",
            "FORMAT_TYPE_NONE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FormatType;

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
                    "FORMAT_TYPE_UNSPECIFIED" => Ok(FormatType::Unspecified),
                    "FORMAT_TYPE_NATIVE" => Ok(FormatType::Native),
                    "FORMAT_TYPE_DEBEZIUM" => Ok(FormatType::Debezium),
                    "FORMAT_TYPE_DEBEZIUM_MONGO" => Ok(FormatType::DebeziumMongo),
                    "FORMAT_TYPE_MAXWELL" => Ok(FormatType::Maxwell),
                    "FORMAT_TYPE_CANAL" => Ok(FormatType::Canal),
                    "FORMAT_TYPE_UPSERT" => Ok(FormatType::Upsert),
                    "FORMAT_TYPE_PLAIN" => Ok(FormatType::Plain),
                    "FORMAT_TYPE_NONE" => Ok(FormatType::None),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for GeneratedColumnDesc {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.GeneratedColumnDesc", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GeneratedColumnDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
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
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GeneratedColumnDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.GeneratedColumnDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GeneratedColumnDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(GeneratedColumnDesc {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.GeneratedColumnDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IndexAndExpr {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.IndexAndExpr", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IndexAndExpr {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Expr,
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
                            "index" => Ok(GeneratedField::Index),
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IndexAndExpr;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.IndexAndExpr")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IndexAndExpr, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IndexAndExpr {
                    index: index__.unwrap_or_default(),
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.IndexAndExpr", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "JOIN_TYPE_UNSPECIFIED",
            Self::Inner => "JOIN_TYPE_INNER",
            Self::LeftOuter => "JOIN_TYPE_LEFT_OUTER",
            Self::RightOuter => "JOIN_TYPE_RIGHT_OUTER",
            Self::FullOuter => "JOIN_TYPE_FULL_OUTER",
            Self::LeftSemi => "JOIN_TYPE_LEFT_SEMI",
            Self::LeftAnti => "JOIN_TYPE_LEFT_ANTI",
            Self::RightSemi => "JOIN_TYPE_RIGHT_SEMI",
            Self::RightAnti => "JOIN_TYPE_RIGHT_ANTI",
            Self::AsofInner => "JOIN_TYPE_ASOF_INNER",
            Self::AsofLeftOuter => "JOIN_TYPE_ASOF_LEFT_OUTER",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for JoinType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "JOIN_TYPE_UNSPECIFIED",
            "JOIN_TYPE_INNER",
            "JOIN_TYPE_LEFT_OUTER",
            "JOIN_TYPE_RIGHT_OUTER",
            "JOIN_TYPE_FULL_OUTER",
            "JOIN_TYPE_LEFT_SEMI",
            "JOIN_TYPE_LEFT_ANTI",
            "JOIN_TYPE_RIGHT_SEMI",
            "JOIN_TYPE_RIGHT_ANTI",
            "JOIN_TYPE_ASOF_INNER",
            "JOIN_TYPE_ASOF_LEFT_OUTER",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinType;

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
                    "JOIN_TYPE_UNSPECIFIED" => Ok(JoinType::Unspecified),
                    "JOIN_TYPE_INNER" => Ok(JoinType::Inner),
                    "JOIN_TYPE_LEFT_OUTER" => Ok(JoinType::LeftOuter),
                    "JOIN_TYPE_RIGHT_OUTER" => Ok(JoinType::RightOuter),
                    "JOIN_TYPE_FULL_OUTER" => Ok(JoinType::FullOuter),
                    "JOIN_TYPE_LEFT_SEMI" => Ok(JoinType::LeftSemi),
                    "JOIN_TYPE_LEFT_ANTI" => Ok(JoinType::LeftAnti),
                    "JOIN_TYPE_RIGHT_SEMI" => Ok(JoinType::RightSemi),
                    "JOIN_TYPE_RIGHT_ANTI" => Ok(JoinType::RightAnti),
                    "JOIN_TYPE_ASOF_INNER" => Ok(JoinType::AsofInner),
                    "JOIN_TYPE_ASOF_LEFT_OUTER" => Ok(JoinType::AsofLeftOuter),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for RowFormatType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::RowUnspecified => "ROW_UNSPECIFIED",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
            Self::DebeziumJson => "DEBEZIUM_JSON",
            Self::Avro => "AVRO",
            Self::Maxwell => "MAXWELL",
            Self::CanalJson => "CANAL_JSON",
            Self::Csv => "CSV",
            Self::Native => "NATIVE",
            Self::DebeziumAvro => "DEBEZIUM_AVRO",
            Self::UpsertJson => "UPSERT_JSON",
            Self::UpsertAvro => "UPSERT_AVRO",
            Self::DebeziumMongoJson => "DEBEZIUM_MONGO_JSON",
            Self::Bytes => "BYTES",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for RowFormatType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ROW_UNSPECIFIED",
            "JSON",
            "PROTOBUF",
            "DEBEZIUM_JSON",
            "AVRO",
            "MAXWELL",
            "CANAL_JSON",
            "CSV",
            "NATIVE",
            "DEBEZIUM_AVRO",
            "UPSERT_JSON",
            "UPSERT_AVRO",
            "DEBEZIUM_MONGO_JSON",
            "BYTES",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RowFormatType;

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
                    "ROW_UNSPECIFIED" => Ok(RowFormatType::RowUnspecified),
                    "JSON" => Ok(RowFormatType::Json),
                    "PROTOBUF" => Ok(RowFormatType::Protobuf),
                    "DEBEZIUM_JSON" => Ok(RowFormatType::DebeziumJson),
                    "AVRO" => Ok(RowFormatType::Avro),
                    "MAXWELL" => Ok(RowFormatType::Maxwell),
                    "CANAL_JSON" => Ok(RowFormatType::CanalJson),
                    "CSV" => Ok(RowFormatType::Csv),
                    "NATIVE" => Ok(RowFormatType::Native),
                    "DEBEZIUM_AVRO" => Ok(RowFormatType::DebeziumAvro),
                    "UPSERT_JSON" => Ok(RowFormatType::UpsertJson),
                    "UPSERT_AVRO" => Ok(RowFormatType::UpsertAvro),
                    "DEBEZIUM_MONGO_JSON" => Ok(RowFormatType::DebeziumMongoJson),
                    "BYTES" => Ok(RowFormatType::Bytes),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for StorageTableDesc {
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
        if !self.columns.is_empty() {
            len += 1;
        }
        if !self.pk.is_empty() {
            len += 1;
        }
        if !self.dist_key_in_pk_indices.is_empty() {
            len += 1;
        }
        if !self.value_indices.is_empty() {
            len += 1;
        }
        if self.read_prefix_len_hint != 0 {
            len += 1;
        }
        if self.versioned {
            len += 1;
        }
        if !self.stream_key.is_empty() {
            len += 1;
        }
        if self.vnode_col_idx_in_pk.is_some() {
            len += 1;
        }
        if self.retention_seconds.is_some() {
            len += 1;
        }
        if self.maybe_vnode_count.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan_common.StorageTableDesc", len)?;
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        if !self.pk.is_empty() {
            struct_ser.serialize_field("pk", &self.pk)?;
        }
        if !self.dist_key_in_pk_indices.is_empty() {
            struct_ser.serialize_field("distKeyInPkIndices", &self.dist_key_in_pk_indices)?;
        }
        if !self.value_indices.is_empty() {
            struct_ser.serialize_field("valueIndices", &self.value_indices)?;
        }
        if self.read_prefix_len_hint != 0 {
            struct_ser.serialize_field("readPrefixLenHint", &self.read_prefix_len_hint)?;
        }
        if self.versioned {
            struct_ser.serialize_field("versioned", &self.versioned)?;
        }
        if !self.stream_key.is_empty() {
            struct_ser.serialize_field("streamKey", &self.stream_key)?;
        }
        if let Some(v) = self.vnode_col_idx_in_pk.as_ref() {
            struct_ser.serialize_field("vnodeColIdxInPk", v)?;
        }
        if let Some(v) = self.retention_seconds.as_ref() {
            struct_ser.serialize_field("retentionSeconds", v)?;
        }
        if let Some(v) = self.maybe_vnode_count.as_ref() {
            struct_ser.serialize_field("maybeVnodeCount", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StorageTableDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_id",
            "tableId",
            "columns",
            "pk",
            "dist_key_in_pk_indices",
            "distKeyInPkIndices",
            "value_indices",
            "valueIndices",
            "read_prefix_len_hint",
            "readPrefixLenHint",
            "versioned",
            "stream_key",
            "streamKey",
            "vnode_col_idx_in_pk",
            "vnodeColIdxInPk",
            "retention_seconds",
            "retentionSeconds",
            "maybe_vnode_count",
            "maybeVnodeCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableId,
            Columns,
            Pk,
            DistKeyInPkIndices,
            ValueIndices,
            ReadPrefixLenHint,
            Versioned,
            StreamKey,
            VnodeColIdxInPk,
            RetentionSeconds,
            MaybeVnodeCount,
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
                            "tableId" | "table_id" => Ok(GeneratedField::TableId),
                            "columns" => Ok(GeneratedField::Columns),
                            "pk" => Ok(GeneratedField::Pk),
                            "distKeyInPkIndices" | "dist_key_in_pk_indices" => Ok(GeneratedField::DistKeyInPkIndices),
                            "valueIndices" | "value_indices" => Ok(GeneratedField::ValueIndices),
                            "readPrefixLenHint" | "read_prefix_len_hint" => Ok(GeneratedField::ReadPrefixLenHint),
                            "versioned" => Ok(GeneratedField::Versioned),
                            "streamKey" | "stream_key" => Ok(GeneratedField::StreamKey),
                            "vnodeColIdxInPk" | "vnode_col_idx_in_pk" => Ok(GeneratedField::VnodeColIdxInPk),
                            "retentionSeconds" | "retention_seconds" => Ok(GeneratedField::RetentionSeconds),
                            "maybeVnodeCount" | "maybe_vnode_count" => Ok(GeneratedField::MaybeVnodeCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StorageTableDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan_common.StorageTableDesc")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StorageTableDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_id__ = None;
                let mut columns__ = None;
                let mut pk__ = None;
                let mut dist_key_in_pk_indices__ = None;
                let mut value_indices__ = None;
                let mut read_prefix_len_hint__ = None;
                let mut versioned__ = None;
                let mut stream_key__ = None;
                let mut vnode_col_idx_in_pk__ = None;
                let mut retention_seconds__ = None;
                let mut maybe_vnode_count__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TableId => {
                            if table_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Pk => {
                            if pk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pk"));
                            }
                            pk__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DistKeyInPkIndices => {
                            if dist_key_in_pk_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distKeyInPkIndices"));
                            }
                            dist_key_in_pk_indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::ValueIndices => {
                            if value_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueIndices"));
                            }
                            value_indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::ReadPrefixLenHint => {
                            if read_prefix_len_hint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("readPrefixLenHint"));
                            }
                            read_prefix_len_hint__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Versioned => {
                            if versioned__.is_some() {
                                return Err(serde::de::Error::duplicate_field("versioned"));
                            }
                            versioned__ = Some(map_.next_value()?);
                        }
                        GeneratedField::StreamKey => {
                            if stream_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamKey"));
                            }
                            stream_key__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::VnodeColIdxInPk => {
                            if vnode_col_idx_in_pk__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vnodeColIdxInPk"));
                            }
                            vnode_col_idx_in_pk__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::RetentionSeconds => {
                            if retention_seconds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("retentionSeconds"));
                            }
                            retention_seconds__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::MaybeVnodeCount => {
                            if maybe_vnode_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("maybeVnodeCount"));
                            }
                            maybe_vnode_count__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(StorageTableDesc {
                    table_id: table_id__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                    pk: pk__.unwrap_or_default(),
                    dist_key_in_pk_indices: dist_key_in_pk_indices__.unwrap_or_default(),
                    value_indices: value_indices__.unwrap_or_default(),
                    read_prefix_len_hint: read_prefix_len_hint__.unwrap_or_default(),
                    versioned: versioned__.unwrap_or_default(),
                    stream_key: stream_key__.unwrap_or_default(),
                    vnode_col_idx_in_pk: vnode_col_idx_in_pk__,
                    retention_seconds: retention_seconds__,
                    maybe_vnode_count: maybe_vnode_count__,
                })
            }
        }
        deserializer.deserialize_struct("plan_common.StorageTableDesc", FIELDS, GeneratedVisitor)
    }
}
