use crate::data::*;
impl serde::Serialize for Actors {
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
        let mut struct_ser = serializer.serialize_struct("data.Actors", len)?;
        if !self.info.is_empty() {
            struct_ser.serialize_field("info", &self.info)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Actors {
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
            type Value = Actors;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Actors")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Actors, V::Error>
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
                Ok(Actors {
                    info: info.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Actors", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AddMutation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.actors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.AddMutation", len)?;
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AddMutation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "actors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = AddMutation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.AddMutation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AddMutation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut actors = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                    }
                }
                Ok(AddMutation {
                    actors: actors.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.AddMutation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Array {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.array_type != 0 {
            len += 1;
        }
        if self.null_bitmap.is_some() {
            len += 1;
        }
        if !self.values.is_empty() {
            len += 1;
        }
        if self.struct_array_data.is_some() {
            len += 1;
        }
        if self.list_array_data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Array", len)?;
        if self.array_type != 0 {
            let v = ArrayType::from_i32(self.array_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.array_type)))?;
            struct_ser.serialize_field("arrayType", &v)?;
        }
        if let Some(v) = self.null_bitmap.as_ref() {
            struct_ser.serialize_field("nullBitmap", v)?;
        }
        if !self.values.is_empty() {
            struct_ser.serialize_field("values", &self.values)?;
        }
        if let Some(v) = self.struct_array_data.as_ref() {
            struct_ser.serialize_field("structArrayData", v)?;
        }
        if let Some(v) = self.list_array_data.as_ref() {
            struct_ser.serialize_field("listArrayData", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Array {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "arrayType",
            "nullBitmap",
            "values",
            "structArrayData",
            "listArrayData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ArrayType,
            NullBitmap,
            Values,
            StructArrayData,
            ListArrayData,
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
                            "arrayType" => Ok(GeneratedField::ArrayType),
                            "nullBitmap" => Ok(GeneratedField::NullBitmap),
                            "values" => Ok(GeneratedField::Values),
                            "structArrayData" => Ok(GeneratedField::StructArrayData),
                            "listArrayData" => Ok(GeneratedField::ListArrayData),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Array;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Array")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Array, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut array_type = None;
                let mut null_bitmap = None;
                let mut values = None;
                let mut struct_array_data = None;
                let mut list_array_data = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ArrayType => {
                            if array_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrayType"));
                            }
                            array_type = Some(map.next_value::<ArrayType>()? as i32);
                        }
                        GeneratedField::NullBitmap => {
                            if null_bitmap.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullBitmap"));
                            }
                            null_bitmap = Some(map.next_value()?);
                        }
                        GeneratedField::Values => {
                            if values.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values = Some(map.next_value()?);
                        }
                        GeneratedField::StructArrayData => {
                            if struct_array_data.is_some() {
                                return Err(serde::de::Error::duplicate_field("structArrayData"));
                            }
                            struct_array_data = Some(map.next_value()?);
                        }
                        GeneratedField::ListArrayData => {
                            if list_array_data.is_some() {
                                return Err(serde::de::Error::duplicate_field("listArrayData"));
                            }
                            list_array_data = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Array {
                    array_type: array_type.unwrap_or_default(),
                    null_bitmap,
                    values: values.unwrap_or_default(),
                    struct_array_data,
                    list_array_data,
                })
            }
        }
        deserializer.deserialize_struct("data.Array", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ArrayType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
            Self::Utf8 => "UTF8",
            Self::Bool => "BOOL",
            Self::Decimal => "DECIMAL",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::Interval => "INTERVAL",
            Self::Struct => "STRUCT",
            Self::List => "LIST",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for ArrayType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INT16",
            "INT32",
            "INT64",
            "FLOAT32",
            "FLOAT64",
            "UTF8",
            "BOOL",
            "DECIMAL",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "INTERVAL",
            "STRUCT",
            "LIST",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ArrayType;

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
                    .and_then(ArrayType::from_i32)
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
                    .and_then(ArrayType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INT16" => Ok(ArrayType::Int16),
                    "INT32" => Ok(ArrayType::Int32),
                    "INT64" => Ok(ArrayType::Int64),
                    "FLOAT32" => Ok(ArrayType::Float32),
                    "FLOAT64" => Ok(ArrayType::Float64),
                    "UTF8" => Ok(ArrayType::Utf8),
                    "BOOL" => Ok(ArrayType::Bool),
                    "DECIMAL" => Ok(ArrayType::Decimal),
                    "DATE" => Ok(ArrayType::Date),
                    "TIME" => Ok(ArrayType::Time),
                    "TIMESTAMP" => Ok(ArrayType::Timestamp),
                    "INTERVAL" => Ok(ArrayType::Interval),
                    "STRUCT" => Ok(ArrayType::Struct),
                    "LIST" => Ok(ArrayType::List),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Barrier {
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
        if !self.span.is_empty() {
            len += 1;
        }
        if self.mutation.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Barrier", len)?;
        if let Some(v) = self.epoch.as_ref() {
            struct_ser.serialize_field("epoch", v)?;
        }
        if !self.span.is_empty() {
            struct_ser.serialize_field("span", pbjson::private::base64::encode(&self.span).as_str())?;
        }
        if let Some(v) = self.mutation.as_ref() {
            match v {
                barrier::Mutation::Nothing(v) => {
                    struct_ser.serialize_field("nothing", v)?;
                }
                barrier::Mutation::Stop(v) => {
                    struct_ser.serialize_field("stop", v)?;
                }
                barrier::Mutation::Update(v) => {
                    struct_ser.serialize_field("update", v)?;
                }
                barrier::Mutation::Add(v) => {
                    struct_ser.serialize_field("add", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Barrier {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "epoch",
            "span",
            "nothing",
            "stop",
            "update",
            "add",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Epoch,
            Span,
            Nothing,
            Stop,
            Update,
            Add,
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
                            "span" => Ok(GeneratedField::Span),
                            "nothing" => Ok(GeneratedField::Nothing),
                            "stop" => Ok(GeneratedField::Stop),
                            "update" => Ok(GeneratedField::Update),
                            "add" => Ok(GeneratedField::Add),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Barrier;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Barrier")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Barrier, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut epoch = None;
                let mut span = None;
                let mut mutation = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Epoch => {
                            if epoch.is_some() {
                                return Err(serde::de::Error::duplicate_field("epoch"));
                            }
                            epoch = Some(map.next_value()?);
                        }
                        GeneratedField::Span => {
                            if span.is_some() {
                                return Err(serde::de::Error::duplicate_field("span"));
                            }
                            span = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Nothing => {
                            if mutation.is_some() {
                                return Err(serde::de::Error::duplicate_field("nothing"));
                            }
                            mutation = Some(barrier::Mutation::Nothing(map.next_value()?));
                        }
                        GeneratedField::Stop => {
                            if mutation.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            mutation = Some(barrier::Mutation::Stop(map.next_value()?));
                        }
                        GeneratedField::Update => {
                            if mutation.is_some() {
                                return Err(serde::de::Error::duplicate_field("update"));
                            }
                            mutation = Some(barrier::Mutation::Update(map.next_value()?));
                        }
                        GeneratedField::Add => {
                            if mutation.is_some() {
                                return Err(serde::de::Error::duplicate_field("add"));
                            }
                            mutation = Some(barrier::Mutation::Add(map.next_value()?));
                        }
                    }
                }
                Ok(Barrier {
                    epoch,
                    span: span.unwrap_or_default(),
                    mutation,
                })
            }
        }
        deserializer.deserialize_struct("data.Barrier", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("data.Buffer", len)?;
        if self.compression != 0 {
            let v = buffer::CompressionType::from_i32(self.compression)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.compression)))?;
            struct_ser.serialize_field("compression", &v)?;
        }
        if !self.body.is_empty() {
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
                formatter.write_str("struct data.Buffer")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Buffer, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut compression = None;
                let mut body = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Compression => {
                            if compression.is_some() {
                                return Err(serde::de::Error::duplicate_field("compression"));
                            }
                            compression = Some(map.next_value::<buffer::CompressionType>()? as i32);
                        }
                        GeneratedField::Body => {
                            if body.is_some() {
                                return Err(serde::de::Error::duplicate_field("body"));
                            }
                            body = Some(
                                map.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Buffer {
                    compression: compression.unwrap_or_default(),
                    body: body.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Buffer", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for buffer::CompressionType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
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
            "INVALID",
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
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(buffer::CompressionType::from_i32)
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
                    .and_then(buffer::CompressionType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(buffer::CompressionType::Invalid),
                    "NONE" => Ok(buffer::CompressionType::None),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Column {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.array.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Column", len)?;
        if let Some(v) = self.array.as_ref() {
            struct_ser.serialize_field("array", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Column {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "array",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Array,
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
                            "array" => Ok(GeneratedField::Array),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Column")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Column, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut array = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Array => {
                            if array.is_some() {
                                return Err(serde::de::Error::duplicate_field("array"));
                            }
                            array = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Column {
                    array,
                })
            }
        }
        deserializer.deserialize_struct("data.Column", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DataChunk {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cardinality != 0 {
            len += 1;
        }
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.DataChunk", len)?;
        if self.cardinality != 0 {
            struct_ser.serialize_field("cardinality", &self.cardinality)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DataChunk {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cardinality",
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cardinality,
            Columns,
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
                            "cardinality" => Ok(GeneratedField::Cardinality),
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DataChunk;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.DataChunk")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DataChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cardinality = None;
                let mut columns = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Cardinality => {
                            if cardinality.is_some() {
                                return Err(serde::de::Error::duplicate_field("cardinality"));
                            }
                            cardinality = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Columns => {
                            if columns.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DataChunk {
                    cardinality: cardinality.unwrap_or_default(),
                    columns: columns.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.DataChunk", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DataType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.type_name != 0 {
            len += 1;
        }
        if self.precision != 0 {
            len += 1;
        }
        if self.scale != 0 {
            len += 1;
        }
        if self.is_nullable {
            len += 1;
        }
        if self.interval_type != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.DataType", len)?;
        if self.type_name != 0 {
            let v = data_type::TypeName::from_i32(self.type_name)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.type_name)))?;
            struct_ser.serialize_field("typeName", &v)?;
        }
        if self.precision != 0 {
            struct_ser.serialize_field("precision", &self.precision)?;
        }
        if self.scale != 0 {
            struct_ser.serialize_field("scale", &self.scale)?;
        }
        if self.is_nullable {
            struct_ser.serialize_field("isNullable", &self.is_nullable)?;
        }
        if self.interval_type != 0 {
            let v = data_type::IntervalType::from_i32(self.interval_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.interval_type)))?;
            struct_ser.serialize_field("intervalType", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DataType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "typeName",
            "precision",
            "scale",
            "isNullable",
            "intervalType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TypeName,
            Precision,
            Scale,
            IsNullable,
            IntervalType,
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
                            "typeName" => Ok(GeneratedField::TypeName),
                            "precision" => Ok(GeneratedField::Precision),
                            "scale" => Ok(GeneratedField::Scale),
                            "isNullable" => Ok(GeneratedField::IsNullable),
                            "intervalType" => Ok(GeneratedField::IntervalType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DataType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.DataType")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DataType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut type_name = None;
                let mut precision = None;
                let mut scale = None;
                let mut is_nullable = None;
                let mut interval_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TypeName => {
                            if type_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeName"));
                            }
                            type_name = Some(map.next_value::<data_type::TypeName>()? as i32);
                        }
                        GeneratedField::Precision => {
                            if precision.is_some() {
                                return Err(serde::de::Error::duplicate_field("precision"));
                            }
                            precision = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Scale => {
                            if scale.is_some() {
                                return Err(serde::de::Error::duplicate_field("scale"));
                            }
                            scale = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::IsNullable => {
                            if is_nullable.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullable"));
                            }
                            is_nullable = Some(map.next_value()?);
                        }
                        GeneratedField::IntervalType => {
                            if interval_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalType"));
                            }
                            interval_type = Some(map.next_value::<data_type::IntervalType>()? as i32);
                        }
                    }
                }
                Ok(DataType {
                    type_name: type_name.unwrap_or_default(),
                    precision: precision.unwrap_or_default(),
                    scale: scale.unwrap_or_default(),
                    is_nullable: is_nullable.unwrap_or_default(),
                    interval_type: interval_type.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.DataType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for data_type::IntervalType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Year => "YEAR",
            Self::Month => "MONTH",
            Self::Day => "DAY",
            Self::Hour => "HOUR",
            Self::Minute => "MINUTE",
            Self::Second => "SECOND",
            Self::YearToMonth => "YEAR_TO_MONTH",
            Self::DayToHour => "DAY_TO_HOUR",
            Self::DayToMinute => "DAY_TO_MINUTE",
            Self::DayToSecond => "DAY_TO_SECOND",
            Self::HourToMinute => "HOUR_TO_MINUTE",
            Self::HourToSecond => "HOUR_TO_SECOND",
            Self::MinuteToSecond => "MINUTE_TO_SECOND",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for data_type::IntervalType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "YEAR_TO_MONTH",
            "DAY_TO_HOUR",
            "DAY_TO_MINUTE",
            "DAY_TO_SECOND",
            "HOUR_TO_MINUTE",
            "HOUR_TO_SECOND",
            "MINUTE_TO_SECOND",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = data_type::IntervalType;

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
                    .and_then(data_type::IntervalType::from_i32)
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
                    .and_then(data_type::IntervalType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(data_type::IntervalType::Invalid),
                    "YEAR" => Ok(data_type::IntervalType::Year),
                    "MONTH" => Ok(data_type::IntervalType::Month),
                    "DAY" => Ok(data_type::IntervalType::Day),
                    "HOUR" => Ok(data_type::IntervalType::Hour),
                    "MINUTE" => Ok(data_type::IntervalType::Minute),
                    "SECOND" => Ok(data_type::IntervalType::Second),
                    "YEAR_TO_MONTH" => Ok(data_type::IntervalType::YearToMonth),
                    "DAY_TO_HOUR" => Ok(data_type::IntervalType::DayToHour),
                    "DAY_TO_MINUTE" => Ok(data_type::IntervalType::DayToMinute),
                    "DAY_TO_SECOND" => Ok(data_type::IntervalType::DayToSecond),
                    "HOUR_TO_MINUTE" => Ok(data_type::IntervalType::HourToMinute),
                    "HOUR_TO_SECOND" => Ok(data_type::IntervalType::HourToSecond),
                    "MINUTE_TO_SECOND" => Ok(data_type::IntervalType::MinuteToSecond),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for data_type::TypeName {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Boolean => "BOOLEAN",
            Self::Char => "CHAR",
            Self::Varchar => "VARCHAR",
            Self::Decimal => "DECIMAL",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::Interval => "INTERVAL",
            Self::Date => "DATE",
            Self::Timestampz => "TIMESTAMPZ",
            Self::Symbol => "SYMBOL",
            Self::Struct => "STRUCT",
            Self::List => "LIST",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for data_type::TypeName {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INT16",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "CHAR",
            "VARCHAR",
            "DECIMAL",
            "TIME",
            "TIMESTAMP",
            "INTERVAL",
            "DATE",
            "TIMESTAMPZ",
            "SYMBOL",
            "STRUCT",
            "LIST",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = data_type::TypeName;

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
                    .and_then(data_type::TypeName::from_i32)
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
                    .and_then(data_type::TypeName::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INT16" => Ok(data_type::TypeName::Int16),
                    "INT32" => Ok(data_type::TypeName::Int32),
                    "INT64" => Ok(data_type::TypeName::Int64),
                    "FLOAT" => Ok(data_type::TypeName::Float),
                    "DOUBLE" => Ok(data_type::TypeName::Double),
                    "BOOLEAN" => Ok(data_type::TypeName::Boolean),
                    "CHAR" => Ok(data_type::TypeName::Char),
                    "VARCHAR" => Ok(data_type::TypeName::Varchar),
                    "DECIMAL" => Ok(data_type::TypeName::Decimal),
                    "TIME" => Ok(data_type::TypeName::Time),
                    "TIMESTAMP" => Ok(data_type::TypeName::Timestamp),
                    "INTERVAL" => Ok(data_type::TypeName::Interval),
                    "DATE" => Ok(data_type::TypeName::Date),
                    "TIMESTAMPZ" => Ok(data_type::TypeName::Timestampz),
                    "SYMBOL" => Ok(data_type::TypeName::Symbol),
                    "STRUCT" => Ok(data_type::TypeName::Struct),
                    "LIST" => Ok(data_type::TypeName::List),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Epoch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.curr != 0 {
            len += 1;
        }
        if self.prev != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Epoch", len)?;
        if self.curr != 0 {
            struct_ser.serialize_field("curr", ToString::to_string(&self.curr).as_str())?;
        }
        if self.prev != 0 {
            struct_ser.serialize_field("prev", ToString::to_string(&self.prev).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Epoch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "curr",
            "prev",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Curr,
            Prev,
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
                            "curr" => Ok(GeneratedField::Curr),
                            "prev" => Ok(GeneratedField::Prev),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Epoch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Epoch")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Epoch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut curr = None;
                let mut prev = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Curr => {
                            if curr.is_some() {
                                return Err(serde::de::Error::duplicate_field("curr"));
                            }
                            curr = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Prev => {
                            if prev.is_some() {
                                return Err(serde::de::Error::duplicate_field("prev"));
                            }
                            prev = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(Epoch {
                    curr: curr.unwrap_or_default(),
                    prev: prev.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Epoch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListArrayData {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.offsets.is_empty() {
            len += 1;
        }
        if self.value.is_some() {
            len += 1;
        }
        if self.value_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.ListArrayData", len)?;
        if !self.offsets.is_empty() {
            struct_ser.serialize_field("offsets", &self.offsets)?;
        }
        if let Some(v) = self.value.as_ref() {
            struct_ser.serialize_field("value", v)?;
        }
        if let Some(v) = self.value_type.as_ref() {
            struct_ser.serialize_field("valueType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListArrayData {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "offsets",
            "value",
            "valueType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Offsets,
            Value,
            ValueType,
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
                            "offsets" => Ok(GeneratedField::Offsets),
                            "value" => Ok(GeneratedField::Value),
                            "valueType" => Ok(GeneratedField::ValueType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListArrayData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.ListArrayData")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ListArrayData, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut offsets = None;
                let mut value = None;
                let mut value_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Offsets => {
                            if offsets.is_some() {
                                return Err(serde::de::Error::duplicate_field("offsets"));
                            }
                            offsets = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Value => {
                            if value.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value = Some(map.next_value()?);
                        }
                        GeneratedField::ValueType => {
                            if value_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueType"));
                            }
                            value_type = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ListArrayData {
                    offsets: offsets.unwrap_or_default(),
                    value,
                    value_type,
                })
            }
        }
        deserializer.deserialize_struct("data.ListArrayData", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NothingMutation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("data.NothingMutation", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NothingMutation {
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
            type Value = NothingMutation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.NothingMutation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<NothingMutation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(NothingMutation {
                })
            }
        }
        deserializer.deserialize_struct("data.NothingMutation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Op {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Insert => "INSERT",
            Self::Delete => "DELETE",
            Self::UpdateInsert => "UPDATE_INSERT",
            Self::UpdateDelete => "UPDATE_DELETE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Op {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INSERT",
            "DELETE",
            "UPDATE_INSERT",
            "UPDATE_DELETE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Op;

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
                    .and_then(Op::from_i32)
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
                    .and_then(Op::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INSERT" => Ok(Op::Insert),
                    "DELETE" => Ok(Op::Delete),
                    "UPDATE_INSERT" => Ok(Op::UpdateInsert),
                    "UPDATE_DELETE" => Ok(Op::UpdateDelete),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for StopMutation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.actors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.StopMutation", len)?;
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StopMutation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "actors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = StopMutation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.StopMutation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StopMutation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut actors = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(StopMutation {
                    actors: actors.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.StopMutation", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StreamChunk {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.cardinality != 0 {
            len += 1;
        }
        if !self.ops.is_empty() {
            len += 1;
        }
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.StreamChunk", len)?;
        if self.cardinality != 0 {
            struct_ser.serialize_field("cardinality", &self.cardinality)?;
        }
        if !self.ops.is_empty() {
            let v = self.ops.iter().cloned().map(|v| {
                Op::from_i32(v)
                    .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", v)))
                }).collect::<Result<Vec<_>, _>>()?;
            struct_ser.serialize_field("ops", &v)?;
        }
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StreamChunk {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cardinality",
            "ops",
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cardinality,
            Ops,
            Columns,
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
                            "cardinality" => Ok(GeneratedField::Cardinality),
                            "ops" => Ok(GeneratedField::Ops),
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamChunk;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.StreamChunk")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StreamChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cardinality = None;
                let mut ops = None;
                let mut columns = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Cardinality => {
                            if cardinality.is_some() {
                                return Err(serde::de::Error::duplicate_field("cardinality"));
                            }
                            cardinality = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Ops => {
                            if ops.is_some() {
                                return Err(serde::de::Error::duplicate_field("ops"));
                            }
                            ops = Some(map.next_value::<Vec<Op>>()?.into_iter().map(|x| x as i32).collect());
                        }
                        GeneratedField::Columns => {
                            if columns.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns = Some(map.next_value()?);
                        }
                    }
                }
                Ok(StreamChunk {
                    cardinality: cardinality.unwrap_or_default(),
                    ops: ops.unwrap_or_default(),
                    columns: columns.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.StreamChunk", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StreamMessage {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.stream_message.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.StreamMessage", len)?;
        if let Some(v) = self.stream_message.as_ref() {
            match v {
                stream_message::StreamMessage::StreamChunk(v) => {
                    struct_ser.serialize_field("streamChunk", v)?;
                }
                stream_message::StreamMessage::Barrier(v) => {
                    struct_ser.serialize_field("barrier", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StreamMessage {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "streamChunk",
            "barrier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            StreamChunk,
            Barrier,
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
                            "streamChunk" => Ok(GeneratedField::StreamChunk),
                            "barrier" => Ok(GeneratedField::Barrier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.StreamMessage")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StreamMessage, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut stream_message = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::StreamChunk => {
                            if stream_message.is_some() {
                                return Err(serde::de::Error::duplicate_field("streamChunk"));
                            }
                            stream_message = Some(stream_message::StreamMessage::StreamChunk(map.next_value()?));
                        }
                        GeneratedField::Barrier => {
                            if stream_message.is_some() {
                                return Err(serde::de::Error::duplicate_field("barrier"));
                            }
                            stream_message = Some(stream_message::StreamMessage::Barrier(map.next_value()?));
                        }
                    }
                }
                Ok(StreamMessage {
                    stream_message,
                })
            }
        }
        deserializer.deserialize_struct("data.StreamMessage", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StructArrayData {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.children_array.is_empty() {
            len += 1;
        }
        if !self.children_type.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.StructArrayData", len)?;
        if !self.children_array.is_empty() {
            struct_ser.serialize_field("childrenArray", &self.children_array)?;
        }
        if !self.children_type.is_empty() {
            struct_ser.serialize_field("childrenType", &self.children_type)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StructArrayData {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "childrenArray",
            "childrenType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ChildrenArray,
            ChildrenType,
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
                            "childrenArray" => Ok(GeneratedField::ChildrenArray),
                            "childrenType" => Ok(GeneratedField::ChildrenType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StructArrayData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.StructArrayData")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StructArrayData, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children_array = None;
                let mut children_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ChildrenArray => {
                            if children_array.is_some() {
                                return Err(serde::de::Error::duplicate_field("childrenArray"));
                            }
                            children_array = Some(map.next_value()?);
                        }
                        GeneratedField::ChildrenType => {
                            if children_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("childrenType"));
                            }
                            children_type = Some(map.next_value()?);
                        }
                    }
                }
                Ok(StructArrayData {
                    children_array: children_array.unwrap_or_default(),
                    children_type: children_type.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.StructArrayData", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Terminate {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("data.Terminate", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Terminate {
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
            type Value = Terminate;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Terminate")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Terminate, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(Terminate {
                })
            }
        }
        deserializer.deserialize_struct("data.Terminate", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UpdateMutation {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.actors.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.UpdateMutation", len)?;
        if !self.actors.is_empty() {
            struct_ser.serialize_field("actors", &self.actors)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UpdateMutation {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "actors",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = UpdateMutation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.UpdateMutation")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<UpdateMutation, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut actors = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Actors => {
                            if actors.is_some() {
                                return Err(serde::de::Error::duplicate_field("actors"));
                            }
                            actors = Some(
                                map.next_value::<std::collections::HashMap<::pbjson::private::NumberDeserialize<u32>, _>>()?
                                    .into_iter().map(|(k,v)| (k.0, v)).collect()
                            );
                        }
                    }
                }
                Ok(UpdateMutation {
                    actors: actors.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.UpdateMutation", FIELDS, GeneratedVisitor)
    }
}
