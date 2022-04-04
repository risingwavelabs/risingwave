use crate::hummock::*;
impl serde::Serialize for AbortEpochRequest {
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
        let mut struct_ser = serializer.serialize_struct("hummock.AbortEpochRequest", len)?;
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AbortEpochRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = AbortEpochRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.AbortEpochRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AbortEpochRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(AbortEpochRequest {
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.AbortEpochRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AbortEpochResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.AbortEpochResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AbortEpochResponse {
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
            type Value = AbortEpochResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.AbortEpochResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AbortEpochResponse, V::Error>
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
                Ok(AbortEpochResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.AbortEpochResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AddTablesRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if !self.tables.is_empty() {
            len += 1;
        }
        if self.epoch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.AddTablesRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AddTablesRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "tables",
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            Tables,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "tables" => Ok(GeneratedField::Tables),
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
            type Value = AddTablesRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.AddTablesRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AddTablesRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut tables = None;
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Tables => {
                            if tables.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables = Some(map.next_value()?);
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
                Ok(AddTablesRequest {
                    context_id: context_id.unwrap_or_default(),
                    tables: tables.unwrap_or_default(),
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.AddTablesRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AddTablesResponse {
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
        if self.version.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.AddTablesResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.version.as_ref() {
            struct_ser.serialize_field("version", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AddTablesResponse {
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
            type Value = AddTablesResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.AddTablesResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AddTablesResponse, V::Error>
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
                            version = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AddTablesResponse {
                    status,
                    version,
                })
            }
        }
        deserializer.deserialize_struct("hummock.AddTablesResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CommitEpochRequest {
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
        let mut struct_ser = serializer.serialize_struct("hummock.CommitEpochRequest", len)?;
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CommitEpochRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = CommitEpochRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CommitEpochRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CommitEpochRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(CommitEpochRequest {
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.CommitEpochRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CommitEpochResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.CommitEpochResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CommitEpochResponse {
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
            type Value = CommitEpochResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CommitEpochResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CommitEpochResponse, V::Error>
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
                Ok(CommitEpochResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.CommitEpochResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactMetrics {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.read_level_n.is_some() {
            len += 1;
        }
        if self.read_level_nplus1.is_some() {
            len += 1;
        }
        if self.write.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.CompactMetrics", len)?;
        if let Some(v) = self.read_level_n.as_ref() {
            struct_ser.serialize_field("readLevelN", v)?;
        }
        if let Some(v) = self.read_level_nplus1.as_ref() {
            struct_ser.serialize_field("readLevelNplus1", v)?;
        }
        if let Some(v) = self.write.as_ref() {
            struct_ser.serialize_field("write", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompactMetrics {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "readLevelN",
            "readLevelNplus1",
            "write",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ReadLevelN,
            ReadLevelNplus1,
            Write,
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
                            "readLevelN" => Ok(GeneratedField::ReadLevelN),
                            "readLevelNplus1" => Ok(GeneratedField::ReadLevelNplus1),
                            "write" => Ok(GeneratedField::Write),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompactMetrics;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CompactMetrics")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CompactMetrics, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut read_level_n = None;
                let mut read_level_nplus1 = None;
                let mut write = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ReadLevelN => {
                            if read_level_n.is_some() {
                                return Err(serde::de::Error::duplicate_field("readLevelN"));
                            }
                            read_level_n = Some(map.next_value()?);
                        }
                        GeneratedField::ReadLevelNplus1 => {
                            if read_level_nplus1.is_some() {
                                return Err(serde::de::Error::duplicate_field("readLevelNplus1"));
                            }
                            read_level_nplus1 = Some(map.next_value()?);
                        }
                        GeneratedField::Write => {
                            if write.is_some() {
                                return Err(serde::de::Error::duplicate_field("write"));
                            }
                            write = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CompactMetrics {
                    read_level_n,
                    read_level_nplus1,
                    write,
                })
            }
        }
        deserializer.deserialize_struct("hummock.CompactMetrics", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.level_handlers.is_empty() {
            len += 1;
        }
        if self.next_compact_task_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.CompactStatus", len)?;
        if !self.level_handlers.is_empty() {
            struct_ser.serialize_field("levelHandlers", &self.level_handlers)?;
        }
        if self.next_compact_task_id != 0 {
            struct_ser.serialize_field("nextCompactTaskId", ToString::to_string(&self.next_compact_task_id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompactStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "levelHandlers",
            "nextCompactTaskId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LevelHandlers,
            NextCompactTaskId,
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
                            "levelHandlers" => Ok(GeneratedField::LevelHandlers),
                            "nextCompactTaskId" => Ok(GeneratedField::NextCompactTaskId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompactStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CompactStatus")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CompactStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level_handlers = None;
                let mut next_compact_task_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LevelHandlers => {
                            if level_handlers.is_some() {
                                return Err(serde::de::Error::duplicate_field("levelHandlers"));
                            }
                            level_handlers = Some(map.next_value()?);
                        }
                        GeneratedField::NextCompactTaskId => {
                            if next_compact_task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("nextCompactTaskId"));
                            }
                            next_compact_task_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(CompactStatus {
                    level_handlers: level_handlers.unwrap_or_default(),
                    next_compact_task_id: next_compact_task_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.CompactStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactTask {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.input_ssts.is_empty() {
            len += 1;
        }
        if !self.splits.is_empty() {
            len += 1;
        }
        if self.watermark != 0 {
            len += 1;
        }
        if !self.sorted_output_ssts.is_empty() {
            len += 1;
        }
        if self.task_id != 0 {
            len += 1;
        }
        if self.target_level != 0 {
            len += 1;
        }
        if self.is_target_ultimate_and_leveling {
            len += 1;
        }
        if self.metrics.is_some() {
            len += 1;
        }
        if self.task_status {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.CompactTask", len)?;
        if !self.input_ssts.is_empty() {
            struct_ser.serialize_field("inputSsts", &self.input_ssts)?;
        }
        if !self.splits.is_empty() {
            struct_ser.serialize_field("splits", &self.splits)?;
        }
        if self.watermark != 0 {
            struct_ser.serialize_field("watermark", ToString::to_string(&self.watermark).as_str())?;
        }
        if !self.sorted_output_ssts.is_empty() {
            struct_ser.serialize_field("sortedOutputSsts", &self.sorted_output_ssts)?;
        }
        if self.task_id != 0 {
            struct_ser.serialize_field("taskId", ToString::to_string(&self.task_id).as_str())?;
        }
        if self.target_level != 0 {
            struct_ser.serialize_field("targetLevel", &self.target_level)?;
        }
        if self.is_target_ultimate_and_leveling {
            struct_ser.serialize_field("isTargetUltimateAndLeveling", &self.is_target_ultimate_and_leveling)?;
        }
        if let Some(v) = self.metrics.as_ref() {
            struct_ser.serialize_field("metrics", v)?;
        }
        if self.task_status {
            struct_ser.serialize_field("taskStatus", &self.task_status)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompactTask {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputSsts",
            "splits",
            "watermark",
            "sortedOutputSsts",
            "taskId",
            "targetLevel",
            "isTargetUltimateAndLeveling",
            "metrics",
            "taskStatus",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            InputSsts,
            Splits,
            Watermark,
            SortedOutputSsts,
            TaskId,
            TargetLevel,
            IsTargetUltimateAndLeveling,
            Metrics,
            TaskStatus,
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
                            "inputSsts" => Ok(GeneratedField::InputSsts),
                            "splits" => Ok(GeneratedField::Splits),
                            "watermark" => Ok(GeneratedField::Watermark),
                            "sortedOutputSsts" => Ok(GeneratedField::SortedOutputSsts),
                            "taskId" => Ok(GeneratedField::TaskId),
                            "targetLevel" => Ok(GeneratedField::TargetLevel),
                            "isTargetUltimateAndLeveling" => Ok(GeneratedField::IsTargetUltimateAndLeveling),
                            "metrics" => Ok(GeneratedField::Metrics),
                            "taskStatus" => Ok(GeneratedField::TaskStatus),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompactTask;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CompactTask")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CompactTask, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input_ssts = None;
                let mut splits = None;
                let mut watermark = None;
                let mut sorted_output_ssts = None;
                let mut task_id = None;
                let mut target_level = None;
                let mut is_target_ultimate_and_leveling = None;
                let mut metrics = None;
                let mut task_status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::InputSsts => {
                            if input_ssts.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputSsts"));
                            }
                            input_ssts = Some(map.next_value()?);
                        }
                        GeneratedField::Splits => {
                            if splits.is_some() {
                                return Err(serde::de::Error::duplicate_field("splits"));
                            }
                            splits = Some(map.next_value()?);
                        }
                        GeneratedField::Watermark => {
                            if watermark.is_some() {
                                return Err(serde::de::Error::duplicate_field("watermark"));
                            }
                            watermark = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::SortedOutputSsts => {
                            if sorted_output_ssts.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortedOutputSsts"));
                            }
                            sorted_output_ssts = Some(map.next_value()?);
                        }
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::TargetLevel => {
                            if target_level.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetLevel"));
                            }
                            target_level = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::IsTargetUltimateAndLeveling => {
                            if is_target_ultimate_and_leveling.is_some() {
                                return Err(serde::de::Error::duplicate_field("isTargetUltimateAndLeveling"));
                            }
                            is_target_ultimate_and_leveling = Some(map.next_value()?);
                        }
                        GeneratedField::Metrics => {
                            if metrics.is_some() {
                                return Err(serde::de::Error::duplicate_field("metrics"));
                            }
                            metrics = Some(map.next_value()?);
                        }
                        GeneratedField::TaskStatus => {
                            if task_status.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskStatus"));
                            }
                            task_status = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CompactTask {
                    input_ssts: input_ssts.unwrap_or_default(),
                    splits: splits.unwrap_or_default(),
                    watermark: watermark.unwrap_or_default(),
                    sorted_output_ssts: sorted_output_ssts.unwrap_or_default(),
                    task_id: task_id.unwrap_or_default(),
                    target_level: target_level.unwrap_or_default(),
                    is_target_ultimate_and_leveling: is_target_ultimate_and_leveling.unwrap_or_default(),
                    metrics,
                    task_status: task_status.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.CompactTask", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactTaskAssignment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compact_task.is_some() {
            len += 1;
        }
        if self.context_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.CompactTaskAssignment", len)?;
        if let Some(v) = self.compact_task.as_ref() {
            struct_ser.serialize_field("compactTask", v)?;
        }
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompactTaskAssignment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compactTask",
            "contextId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CompactTask,
            ContextId,
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
                            "compactTask" => Ok(GeneratedField::CompactTask),
                            "contextId" => Ok(GeneratedField::ContextId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompactTaskAssignment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CompactTaskAssignment")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CompactTaskAssignment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compact_task = None;
                let mut context_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::CompactTask => {
                            if compact_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("compactTask"));
                            }
                            compact_task = Some(map.next_value()?);
                        }
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(CompactTaskAssignment {
                    compact_task,
                    context_id: context_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.CompactTaskAssignment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CompactTaskRefId {
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
        let mut struct_ser = serializer.serialize_struct("hummock.CompactTaskRefId", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CompactTaskRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CompactTaskRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.CompactTaskRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CompactTaskRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(CompactTaskRefId {
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.CompactTaskRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetCompactionTasksRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("hummock.GetCompactionTasksRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetCompactionTasksRequest {
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
            type Value = GetCompactionTasksRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.GetCompactionTasksRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetCompactionTasksRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(GetCompactionTasksRequest {
                })
            }
        }
        deserializer.deserialize_struct("hummock.GetCompactionTasksRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetCompactionTasksResponse {
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
        if self.compact_task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.GetCompactionTasksResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.compact_task.as_ref() {
            struct_ser.serialize_field("compactTask", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetCompactionTasksResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "compactTask",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            CompactTask,
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
                            "compactTask" => Ok(GeneratedField::CompactTask),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetCompactionTasksResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.GetCompactionTasksResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetCompactionTasksResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut compact_task = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::CompactTask => {
                            if compact_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("compactTask"));
                            }
                            compact_task = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetCompactionTasksResponse {
                    status,
                    compact_task,
                })
            }
        }
        deserializer.deserialize_struct("hummock.GetCompactionTasksResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetNewTableIdRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("hummock.GetNewTableIdRequest", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetNewTableIdRequest {
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
            type Value = GetNewTableIdRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.GetNewTableIdRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetNewTableIdRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(GetNewTableIdRequest {
                })
            }
        }
        deserializer.deserialize_struct("hummock.GetNewTableIdRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetNewTableIdResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.GetNewTableIdResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", ToString::to_string(&self.table_id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetNewTableIdResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "tableId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
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
                            "status" => Ok(GeneratedField::Status),
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
            type Value = GetNewTableIdResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.GetNewTableIdResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetNewTableIdResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut table_id = None;
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
                    }
                }
                Ok(GetNewTableIdResponse {
                    status,
                    table_id: table_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.GetNewTableIdResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockContextRefId {
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
        let mut struct_ser = serializer.serialize_struct("hummock.HummockContextRefId", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", &self.id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockContextRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockContextRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockContextRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockContextRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(HummockContextRefId {
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockContextRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockPinnedSnapshot {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if !self.snapshot_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.HummockPinnedSnapshot", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if !self.snapshot_id.is_empty() {
            struct_ser.serialize_field("snapshotId", &self.snapshot_id.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockPinnedSnapshot {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "snapshotId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            SnapshotId,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "snapshotId" => Ok(GeneratedField::SnapshotId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockPinnedSnapshot;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockPinnedSnapshot")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockPinnedSnapshot, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut snapshot_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::SnapshotId => {
                            if snapshot_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshotId"));
                            }
                            snapshot_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(HummockPinnedSnapshot {
                    context_id: context_id.unwrap_or_default(),
                    snapshot_id: snapshot_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockPinnedSnapshot", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockPinnedVersion {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if !self.version_id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.HummockPinnedVersion", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if !self.version_id.is_empty() {
            struct_ser.serialize_field("versionId", &self.version_id.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockPinnedVersion {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "versionId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            VersionId,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "versionId" => Ok(GeneratedField::VersionId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockPinnedVersion;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockPinnedVersion")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockPinnedVersion, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut version_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::VersionId => {
                            if version_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("versionId"));
                            }
                            version_id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(HummockPinnedVersion {
                    context_id: context_id.unwrap_or_default(),
                    version_id: version_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockPinnedVersion", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockSnapshot {
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
        let mut struct_ser = serializer.serialize_struct("hummock.HummockSnapshot", len)?;
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockSnapshot {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = HummockSnapshot;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockSnapshot")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockSnapshot, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(HummockSnapshot {
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockSnapshot", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockStaleSstables {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.version_id != 0 {
            len += 1;
        }
        if !self.id.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.HummockStaleSstables", len)?;
        if self.version_id != 0 {
            struct_ser.serialize_field("versionId", ToString::to_string(&self.version_id).as_str())?;
        }
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockStaleSstables {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "versionId",
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            VersionId,
            Id,
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
                            "versionId" => Ok(GeneratedField::VersionId),
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockStaleSstables;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockStaleSstables")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockStaleSstables, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut version_id = None;
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::VersionId => {
                            if version_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("versionId"));
                            }
                            version_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(HummockStaleSstables {
                    version_id: version_id.unwrap_or_default(),
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockStaleSstables", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockVersion {
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
        if !self.levels.is_empty() {
            len += 1;
        }
        if !self.uncommitted_epochs.is_empty() {
            len += 1;
        }
        if self.max_committed_epoch != 0 {
            len += 1;
        }
        if self.safe_epoch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.HummockVersion", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        if !self.levels.is_empty() {
            struct_ser.serialize_field("levels", &self.levels)?;
        }
        if !self.uncommitted_epochs.is_empty() {
            struct_ser.serialize_field("uncommittedEpochs", &self.uncommitted_epochs)?;
        }
        if self.max_committed_epoch != 0 {
            struct_ser.serialize_field("maxCommittedEpoch", ToString::to_string(&self.max_committed_epoch).as_str())?;
        }
        if self.safe_epoch != 0 {
            struct_ser.serialize_field("safeEpoch", ToString::to_string(&self.safe_epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockVersion {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "levels",
            "uncommittedEpochs",
            "maxCommittedEpoch",
            "safeEpoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Levels,
            UncommittedEpochs,
            MaxCommittedEpoch,
            SafeEpoch,
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
                            "id" => Ok(GeneratedField::Id),
                            "levels" => Ok(GeneratedField::Levels),
                            "uncommittedEpochs" => Ok(GeneratedField::UncommittedEpochs),
                            "maxCommittedEpoch" => Ok(GeneratedField::MaxCommittedEpoch),
                            "safeEpoch" => Ok(GeneratedField::SafeEpoch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockVersion;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockVersion")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockVersion, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                let mut levels = None;
                let mut uncommitted_epochs = None;
                let mut max_committed_epoch = None;
                let mut safe_epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Levels => {
                            if levels.is_some() {
                                return Err(serde::de::Error::duplicate_field("levels"));
                            }
                            levels = Some(map.next_value()?);
                        }
                        GeneratedField::UncommittedEpochs => {
                            if uncommitted_epochs.is_some() {
                                return Err(serde::de::Error::duplicate_field("uncommittedEpochs"));
                            }
                            uncommitted_epochs = Some(map.next_value()?);
                        }
                        GeneratedField::MaxCommittedEpoch => {
                            if max_committed_epoch.is_some() {
                                return Err(serde::de::Error::duplicate_field("maxCommittedEpoch"));
                            }
                            max_committed_epoch = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::SafeEpoch => {
                            if safe_epoch.is_some() {
                                return Err(serde::de::Error::duplicate_field("safeEpoch"));
                            }
                            safe_epoch = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(HummockVersion {
                    id: id.unwrap_or_default(),
                    levels: levels.unwrap_or_default(),
                    uncommitted_epochs: uncommitted_epochs.unwrap_or_default(),
                    max_committed_epoch: max_committed_epoch.unwrap_or_default(),
                    safe_epoch: safe_epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockVersion", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HummockVersionRefId {
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
        let mut struct_ser = serializer.serialize_struct("hummock.HummockVersionRefId", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HummockVersionRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HummockVersionRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.HummockVersionRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HummockVersionRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(HummockVersionRefId {
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.HummockVersionRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for KeyRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.left.is_empty() {
            len += 1;
        }
        if !self.right.is_empty() {
            len += 1;
        }
        if self.inf {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.KeyRange", len)?;
        if !self.left.is_empty() {
            struct_ser.serialize_field("left", pbjson::private::base64::encode(&self.left).as_str())?;
        }
        if !self.right.is_empty() {
            struct_ser.serialize_field("right", pbjson::private::base64::encode(&self.right).as_str())?;
        }
        if self.inf {
            struct_ser.serialize_field("inf", &self.inf)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for KeyRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "inf",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            Inf,
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
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "inf" => Ok(GeneratedField::Inf),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = KeyRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.KeyRange")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<KeyRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left = None;
                let mut right = None;
                let mut inf = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Right => {
                            if right.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Inf => {
                            if inf.is_some() {
                                return Err(serde::de::Error::duplicate_field("inf"));
                            }
                            inf = Some(map.next_value()?);
                        }
                    }
                }
                Ok(KeyRange {
                    left: left.unwrap_or_default(),
                    right: right.unwrap_or_default(),
                    inf: inf.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.KeyRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Level {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.level_type != 0 {
            len += 1;
        }
        if !self.table_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.Level", len)?;
        if self.level_type != 0 {
            let v = LevelType::from_i32(self.level_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.level_type)))?;
            struct_ser.serialize_field("levelType", &v)?;
        }
        if !self.table_ids.is_empty() {
            struct_ser.serialize_field("tableIds", &self.table_ids.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Level {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "levelType",
            "tableIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LevelType,
            TableIds,
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
                            "levelType" => Ok(GeneratedField::LevelType),
                            "tableIds" => Ok(GeneratedField::TableIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Level;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.Level")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Level, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level_type = None;
                let mut table_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LevelType => {
                            if level_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("levelType"));
                            }
                            level_type = Some(map.next_value::<LevelType>()? as i32);
                        }
                        GeneratedField::TableIds => {
                            if table_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableIds"));
                            }
                            table_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(Level {
                    level_type: level_type.unwrap_or_default(),
                    table_ids: table_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.Level", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LevelEntry {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.level_idx != 0 {
            len += 1;
        }
        if self.level.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.LevelEntry", len)?;
        if self.level_idx != 0 {
            struct_ser.serialize_field("levelIdx", &self.level_idx)?;
        }
        if let Some(v) = self.level.as_ref() {
            struct_ser.serialize_field("level", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LevelEntry {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "levelIdx",
            "level",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LevelIdx,
            Level,
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
                            "levelIdx" => Ok(GeneratedField::LevelIdx),
                            "level" => Ok(GeneratedField::Level),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LevelEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.LevelEntry")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LevelEntry, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level_idx = None;
                let mut level = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LevelIdx => {
                            if level_idx.is_some() {
                                return Err(serde::de::Error::duplicate_field("levelIdx"));
                            }
                            level_idx = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Level => {
                            if level.is_some() {
                                return Err(serde::de::Error::duplicate_field("level"));
                            }
                            level = Some(map.next_value()?);
                        }
                    }
                }
                Ok(LevelEntry {
                    level_idx: level_idx.unwrap_or_default(),
                    level,
                })
            }
        }
        deserializer.deserialize_struct("hummock.LevelEntry", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LevelHandler {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.level_type != 0 {
            len += 1;
        }
        if !self.ssts.is_empty() {
            len += 1;
        }
        if !self.key_ranges.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.LevelHandler", len)?;
        if self.level_type != 0 {
            let v = LevelType::from_i32(self.level_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.level_type)))?;
            struct_ser.serialize_field("levelType", &v)?;
        }
        if !self.ssts.is_empty() {
            struct_ser.serialize_field("ssts", &self.ssts)?;
        }
        if !self.key_ranges.is_empty() {
            struct_ser.serialize_field("keyRanges", &self.key_ranges)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LevelHandler {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "levelType",
            "ssts",
            "keyRanges",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LevelType,
            Ssts,
            KeyRanges,
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
                            "levelType" => Ok(GeneratedField::LevelType),
                            "ssts" => Ok(GeneratedField::Ssts),
                            "keyRanges" => Ok(GeneratedField::KeyRanges),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LevelHandler;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.LevelHandler")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LevelHandler, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level_type = None;
                let mut ssts = None;
                let mut key_ranges = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LevelType => {
                            if level_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("levelType"));
                            }
                            level_type = Some(map.next_value::<LevelType>()? as i32);
                        }
                        GeneratedField::Ssts => {
                            if ssts.is_some() {
                                return Err(serde::de::Error::duplicate_field("ssts"));
                            }
                            ssts = Some(map.next_value()?);
                        }
                        GeneratedField::KeyRanges => {
                            if key_ranges.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyRanges"));
                            }
                            key_ranges = Some(map.next_value()?);
                        }
                    }
                }
                Ok(LevelHandler {
                    level_type: level_type.unwrap_or_default(),
                    ssts: ssts.unwrap_or_default(),
                    key_ranges: key_ranges.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.LevelHandler", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for level_handler::KeyRangeTaskId {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key_range.is_some() {
            len += 1;
        }
        if self.task_id != 0 {
            len += 1;
        }
        if self.ssts != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.LevelHandler.KeyRangeTaskId", len)?;
        if let Some(v) = self.key_range.as_ref() {
            struct_ser.serialize_field("keyRange", v)?;
        }
        if self.task_id != 0 {
            struct_ser.serialize_field("taskId", ToString::to_string(&self.task_id).as_str())?;
        }
        if self.ssts != 0 {
            struct_ser.serialize_field("ssts", ToString::to_string(&self.ssts).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for level_handler::KeyRangeTaskId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "keyRange",
            "taskId",
            "ssts",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            KeyRange,
            TaskId,
            Ssts,
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
                            "keyRange" => Ok(GeneratedField::KeyRange),
                            "taskId" => Ok(GeneratedField::TaskId),
                            "ssts" => Ok(GeneratedField::Ssts),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = level_handler::KeyRangeTaskId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.LevelHandler.KeyRangeTaskId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<level_handler::KeyRangeTaskId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key_range = None;
                let mut task_id = None;
                let mut ssts = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::KeyRange => {
                            if key_range.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyRange"));
                            }
                            key_range = Some(map.next_value()?);
                        }
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Ssts => {
                            if ssts.is_some() {
                                return Err(serde::de::Error::duplicate_field("ssts"));
                            }
                            ssts = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(level_handler::KeyRangeTaskId {
                    key_range,
                    task_id: task_id.unwrap_or_default(),
                    ssts: ssts.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.LevelHandler.KeyRangeTaskId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LevelType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Nonoverlapping => "NONOVERLAPPING",
            Self::Overlapping => "OVERLAPPING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for LevelType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NONOVERLAPPING",
            "OVERLAPPING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LevelType;

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
                    .and_then(LevelType::from_i32)
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
                    .and_then(LevelType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NONOVERLAPPING" => Ok(LevelType::Nonoverlapping),
                    "OVERLAPPING" => Ok(LevelType::Overlapping),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for PinSnapshotRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if self.last_pinned != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.PinSnapshotRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if self.last_pinned != 0 {
            struct_ser.serialize_field("lastPinned", ToString::to_string(&self.last_pinned).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PinSnapshotRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "lastPinned",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            LastPinned,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "lastPinned" => Ok(GeneratedField::LastPinned),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PinSnapshotRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.PinSnapshotRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PinSnapshotRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut last_pinned = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::LastPinned => {
                            if last_pinned.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastPinned"));
                            }
                            last_pinned = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(PinSnapshotRequest {
                    context_id: context_id.unwrap_or_default(),
                    last_pinned: last_pinned.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.PinSnapshotRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PinSnapshotResponse {
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
        if self.snapshot.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.PinSnapshotResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.snapshot.as_ref() {
            struct_ser.serialize_field("snapshot", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PinSnapshotResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "snapshot",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            Snapshot,
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
                            "snapshot" => Ok(GeneratedField::Snapshot),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PinSnapshotResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.PinSnapshotResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PinSnapshotResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut snapshot = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::Snapshot => {
                            if snapshot.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshot"));
                            }
                            snapshot = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PinSnapshotResponse {
                    status,
                    snapshot,
                })
            }
        }
        deserializer.deserialize_struct("hummock.PinSnapshotResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PinVersionRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if self.last_pinned != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.PinVersionRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if self.last_pinned != 0 {
            struct_ser.serialize_field("lastPinned", ToString::to_string(&self.last_pinned).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PinVersionRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "lastPinned",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            LastPinned,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "lastPinned" => Ok(GeneratedField::LastPinned),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PinVersionRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.PinVersionRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PinVersionRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut last_pinned = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::LastPinned => {
                            if last_pinned.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastPinned"));
                            }
                            last_pinned = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(PinVersionRequest {
                    context_id: context_id.unwrap_or_default(),
                    last_pinned: last_pinned.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.PinVersionRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PinVersionResponse {
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
        if self.pinned_version.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.PinVersionResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.pinned_version.as_ref() {
            struct_ser.serialize_field("pinnedVersion", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PinVersionResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "pinnedVersion",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            PinnedVersion,
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
                            "pinnedVersion" => Ok(GeneratedField::PinnedVersion),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PinVersionResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.PinVersionResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PinVersionResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut pinned_version = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::PinnedVersion => {
                            if pinned_version.is_some() {
                                return Err(serde::de::Error::duplicate_field("pinnedVersion"));
                            }
                            pinned_version = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PinVersionResponse {
                    status,
                    pinned_version,
                })
            }
        }
        deserializer.deserialize_struct("hummock.PinVersionResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReportCompactionTasksRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compact_task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.ReportCompactionTasksRequest", len)?;
        if let Some(v) = self.compact_task.as_ref() {
            struct_ser.serialize_field("compactTask", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReportCompactionTasksRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compactTask",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CompactTask,
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
                            "compactTask" => Ok(GeneratedField::CompactTask),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReportCompactionTasksRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.ReportCompactionTasksRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ReportCompactionTasksRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compact_task = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::CompactTask => {
                            if compact_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("compactTask"));
                            }
                            compact_task = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ReportCompactionTasksRequest {
                    compact_task,
                })
            }
        }
        deserializer.deserialize_struct("hummock.ReportCompactionTasksRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReportCompactionTasksResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.ReportCompactionTasksResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReportCompactionTasksResponse {
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
            type Value = ReportCompactionTasksResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.ReportCompactionTasksResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ReportCompactionTasksResponse, V::Error>
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
                Ok(ReportCompactionTasksResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.ReportCompactionTasksResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReportVacuumTaskRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.vacuum_task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.ReportVacuumTaskRequest", len)?;
        if let Some(v) = self.vacuum_task.as_ref() {
            struct_ser.serialize_field("vacuumTask", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReportVacuumTaskRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "vacuumTask",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            VacuumTask,
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
                            "vacuumTask" => Ok(GeneratedField::VacuumTask),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ReportVacuumTaskRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.ReportVacuumTaskRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ReportVacuumTaskRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut vacuum_task = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::VacuumTask => {
                            if vacuum_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("vacuumTask"));
                            }
                            vacuum_task = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ReportVacuumTaskRequest {
                    vacuum_task,
                })
            }
        }
        deserializer.deserialize_struct("hummock.ReportVacuumTaskRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ReportVacuumTaskResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.ReportVacuumTaskResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ReportVacuumTaskResponse {
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
            type Value = ReportVacuumTaskResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.ReportVacuumTaskResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ReportVacuumTaskResponse, V::Error>
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
                Ok(ReportVacuumTaskResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.ReportVacuumTaskResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SstableIdInfo {
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
        if self.id_create_timestamp != 0 {
            len += 1;
        }
        if self.meta_create_timestamp != 0 {
            len += 1;
        }
        if self.meta_delete_timestamp != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.SstableIdInfo", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        if self.id_create_timestamp != 0 {
            struct_ser.serialize_field("idCreateTimestamp", ToString::to_string(&self.id_create_timestamp).as_str())?;
        }
        if self.meta_create_timestamp != 0 {
            struct_ser.serialize_field("metaCreateTimestamp", ToString::to_string(&self.meta_create_timestamp).as_str())?;
        }
        if self.meta_delete_timestamp != 0 {
            struct_ser.serialize_field("metaDeleteTimestamp", ToString::to_string(&self.meta_delete_timestamp).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SstableIdInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "idCreateTimestamp",
            "metaCreateTimestamp",
            "metaDeleteTimestamp",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            IdCreateTimestamp,
            MetaCreateTimestamp,
            MetaDeleteTimestamp,
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
                            "id" => Ok(GeneratedField::Id),
                            "idCreateTimestamp" => Ok(GeneratedField::IdCreateTimestamp),
                            "metaCreateTimestamp" => Ok(GeneratedField::MetaCreateTimestamp),
                            "metaDeleteTimestamp" => Ok(GeneratedField::MetaDeleteTimestamp),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SstableIdInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SstableIdInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SstableIdInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                let mut id_create_timestamp = None;
                let mut meta_create_timestamp = None;
                let mut meta_delete_timestamp = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::IdCreateTimestamp => {
                            if id_create_timestamp.is_some() {
                                return Err(serde::de::Error::duplicate_field("idCreateTimestamp"));
                            }
                            id_create_timestamp = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::MetaCreateTimestamp => {
                            if meta_create_timestamp.is_some() {
                                return Err(serde::de::Error::duplicate_field("metaCreateTimestamp"));
                            }
                            meta_create_timestamp = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::MetaDeleteTimestamp => {
                            if meta_delete_timestamp.is_some() {
                                return Err(serde::de::Error::duplicate_field("metaDeleteTimestamp"));
                            }
                            meta_delete_timestamp = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(SstableIdInfo {
                    id: id.unwrap_or_default(),
                    id_create_timestamp: id_create_timestamp.unwrap_or_default(),
                    meta_create_timestamp: meta_create_timestamp.unwrap_or_default(),
                    meta_delete_timestamp: meta_delete_timestamp.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.SstableIdInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SstableInfo {
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
        if self.key_range.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.SstableInfo", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        if let Some(v) = self.key_range.as_ref() {
            struct_ser.serialize_field("keyRange", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SstableInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "keyRange",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            KeyRange,
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
                            "id" => Ok(GeneratedField::Id),
                            "keyRange" => Ok(GeneratedField::KeyRange),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SstableInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SstableInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SstableInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                let mut key_range = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::KeyRange => {
                            if key_range.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyRange"));
                            }
                            key_range = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SstableInfo {
                    id: id.unwrap_or_default(),
                    key_range,
                })
            }
        }
        deserializer.deserialize_struct("hummock.SstableInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SstableRefId {
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
        let mut struct_ser = serializer.serialize_struct("hummock.SstableRefId", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SstableRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SstableRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SstableRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SstableRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(SstableRefId {
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.SstableRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SstableStat {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key_range.is_some() {
            len += 1;
        }
        if self.table_id != 0 {
            len += 1;
        }
        if self.compact_task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.SstableStat", len)?;
        if let Some(v) = self.key_range.as_ref() {
            struct_ser.serialize_field("keyRange", v)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", ToString::to_string(&self.table_id).as_str())?;
        }
        if let Some(v) = self.compact_task.as_ref() {
            struct_ser.serialize_field("compactTask", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SstableStat {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "keyRange",
            "tableId",
            "compactTask",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            KeyRange,
            TableId,
            CompactTask,
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
                            "keyRange" => Ok(GeneratedField::KeyRange),
                            "tableId" => Ok(GeneratedField::TableId),
                            "compactTask" => Ok(GeneratedField::CompactTask),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SstableStat;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SstableStat")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SstableStat, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key_range = None;
                let mut table_id = None;
                let mut compact_task = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::KeyRange => {
                            if key_range.is_some() {
                                return Err(serde::de::Error::duplicate_field("keyRange"));
                            }
                            key_range = Some(map.next_value()?);
                        }
                        GeneratedField::TableId => {
                            if table_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableId"));
                            }
                            table_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::CompactTask => {
                            if compact_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("compactTask"));
                            }
                            compact_task = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SstableStat {
                    key_range,
                    table_id: table_id.unwrap_or_default(),
                    compact_task,
                })
            }
        }
        deserializer.deserialize_struct("hummock.SstableStat", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for sstable_stat::CompactTaskId {
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
        let mut struct_ser = serializer.serialize_struct("hummock.SstableStat.CompactTaskId", len)?;
        if self.id != 0 {
            struct_ser.serialize_field("id", ToString::to_string(&self.id).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for sstable_stat::CompactTaskId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
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
                            "id" => Ok(GeneratedField::Id),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = sstable_stat::CompactTaskId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SstableStat.CompactTaskId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<sstable_stat::CompactTaskId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(sstable_stat::CompactTaskId {
                    id: id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.SstableStat.CompactTaskId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubscribeCompactTasksRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.SubscribeCompactTasksRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubscribeCompactTasksRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubscribeCompactTasksRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SubscribeCompactTasksRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SubscribeCompactTasksRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(SubscribeCompactTasksRequest {
                    context_id: context_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.SubscribeCompactTasksRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubscribeCompactTasksResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.compact_task.is_some() {
            len += 1;
        }
        if self.vacuum_task.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.SubscribeCompactTasksResponse", len)?;
        if let Some(v) = self.compact_task.as_ref() {
            struct_ser.serialize_field("compactTask", v)?;
        }
        if let Some(v) = self.vacuum_task.as_ref() {
            struct_ser.serialize_field("vacuumTask", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubscribeCompactTasksResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "compactTask",
            "vacuumTask",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CompactTask,
            VacuumTask,
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
                            "compactTask" => Ok(GeneratedField::CompactTask),
                            "vacuumTask" => Ok(GeneratedField::VacuumTask),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubscribeCompactTasksResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.SubscribeCompactTasksResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SubscribeCompactTasksResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compact_task = None;
                let mut vacuum_task = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::CompactTask => {
                            if compact_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("compactTask"));
                            }
                            compact_task = Some(map.next_value()?);
                        }
                        GeneratedField::VacuumTask => {
                            if vacuum_task.is_some() {
                                return Err(serde::de::Error::duplicate_field("vacuumTask"));
                            }
                            vacuum_task = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SubscribeCompactTasksResponse {
                    compact_task,
                    vacuum_task,
                })
            }
        }
        deserializer.deserialize_struct("hummock.SubscribeCompactTasksResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableSetStatistics {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.level_idx != 0 {
            len += 1;
        }
        if self.size_gb != 0. {
            len += 1;
        }
        if self.cnt != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.TableSetStatistics", len)?;
        if self.level_idx != 0 {
            struct_ser.serialize_field("levelIdx", &self.level_idx)?;
        }
        if self.size_gb != 0. {
            struct_ser.serialize_field("sizeGb", &self.size_gb)?;
        }
        if self.cnt != 0 {
            struct_ser.serialize_field("cnt", ToString::to_string(&self.cnt).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableSetStatistics {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "levelIdx",
            "sizeGb",
            "cnt",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LevelIdx,
            SizeGb,
            Cnt,
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
                            "levelIdx" => Ok(GeneratedField::LevelIdx),
                            "sizeGb" => Ok(GeneratedField::SizeGb),
                            "cnt" => Ok(GeneratedField::Cnt),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableSetStatistics;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.TableSetStatistics")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableSetStatistics, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut level_idx = None;
                let mut size_gb = None;
                let mut cnt = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::LevelIdx => {
                            if level_idx.is_some() {
                                return Err(serde::de::Error::duplicate_field("levelIdx"));
                            }
                            level_idx = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::SizeGb => {
                            if size_gb.is_some() {
                                return Err(serde::de::Error::duplicate_field("sizeGb"));
                            }
                            size_gb = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Cnt => {
                            if cnt.is_some() {
                                return Err(serde::de::Error::duplicate_field("cnt"));
                            }
                            cnt = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(TableSetStatistics {
                    level_idx: level_idx.unwrap_or_default(),
                    size_gb: size_gb.unwrap_or_default(),
                    cnt: cnt.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.TableSetStatistics", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UncommittedEpoch {
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
        if !self.tables.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.UncommittedEpoch", len)?;
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        if !self.tables.is_empty() {
            struct_ser.serialize_field("tables", &self.tables)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UncommittedEpoch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
            "tables",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Epoch,
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
                            "epoch" => Ok(GeneratedField::Epoch),
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
            type Value = UncommittedEpoch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.UncommittedEpoch")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UncommittedEpoch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch = None;
                let mut tables = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Epoch => {
                            if epoch.is_some() {
                                return Err(serde::de::Error::duplicate_field("epoch"));
                            }
                            epoch = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Tables => {
                            if tables.is_some() {
                                return Err(serde::de::Error::duplicate_field("tables"));
                            }
                            tables = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UncommittedEpoch {
                    epoch: epoch.unwrap_or_default(),
                    tables: tables.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.UncommittedEpoch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnpinSnapshotRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if !self.snapshots.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.UnpinSnapshotRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if !self.snapshots.is_empty() {
            struct_ser.serialize_field("snapshots", &self.snapshots)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnpinSnapshotRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "snapshots",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            Snapshots,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "snapshots" => Ok(GeneratedField::Snapshots),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnpinSnapshotRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.UnpinSnapshotRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnpinSnapshotRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut snapshots = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Snapshots => {
                            if snapshots.is_some() {
                                return Err(serde::de::Error::duplicate_field("snapshots"));
                            }
                            snapshots = Some(map.next_value()?);
                        }
                    }
                }
                Ok(UnpinSnapshotRequest {
                    context_id: context_id.unwrap_or_default(),
                    snapshots: snapshots.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.UnpinSnapshotRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnpinSnapshotResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.UnpinSnapshotResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnpinSnapshotResponse {
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
            type Value = UnpinSnapshotResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.UnpinSnapshotResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnpinSnapshotResponse, V::Error>
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
                Ok(UnpinSnapshotResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.UnpinSnapshotResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnpinVersionRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.context_id != 0 {
            len += 1;
        }
        if !self.pinned_version_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.UnpinVersionRequest", len)?;
        if self.context_id != 0 {
            struct_ser.serialize_field("contextId", &self.context_id)?;
        }
        if !self.pinned_version_ids.is_empty() {
            struct_ser.serialize_field("pinnedVersionIds", &self.pinned_version_ids.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnpinVersionRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "contextId",
            "pinnedVersionIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ContextId,
            PinnedVersionIds,
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
                            "contextId" => Ok(GeneratedField::ContextId),
                            "pinnedVersionIds" => Ok(GeneratedField::PinnedVersionIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnpinVersionRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.UnpinVersionRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnpinVersionRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut context_id = None;
                let mut pinned_version_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ContextId => {
                            if context_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("contextId"));
                            }
                            context_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::PinnedVersionIds => {
                            if pinned_version_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("pinnedVersionIds"));
                            }
                            pinned_version_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(UnpinVersionRequest {
                    context_id: context_id.unwrap_or_default(),
                    pinned_version_ids: pinned_version_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.UnpinVersionRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnpinVersionResponse {
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
        let mut struct_ser = serializer.serialize_struct("hummock.UnpinVersionResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnpinVersionResponse {
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
            type Value = UnpinVersionResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.UnpinVersionResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UnpinVersionResponse, V::Error>
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
                Ok(UnpinVersionResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("hummock.UnpinVersionResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for VacuumTask {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sstable_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("hummock.VacuumTask", len)?;
        if !self.sstable_ids.is_empty() {
            struct_ser.serialize_field("sstableIds", &self.sstable_ids.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for VacuumTask {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sstableIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SstableIds,
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
                            "sstableIds" => Ok(GeneratedField::SstableIds),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = VacuumTask;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct hummock.VacuumTask")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<VacuumTask, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sstable_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SstableIds => {
                            if sstable_ids.is_some() {
                                return Err(serde::de::Error::duplicate_field("sstableIds"));
                            }
                            sstable_ids = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(VacuumTask {
                    sstable_ids: sstable_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("hummock.VacuumTask", FIELDS, GeneratedVisitor)
    }
}
