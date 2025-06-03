use crate::data::*;
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
            let v = ArrayType::try_from(self.array_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.array_type)))?;
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
            "array_type",
            "arrayType",
            "null_bitmap",
            "nullBitmap",
            "values",
            "struct_array_data",
            "structArrayData",
            "list_array_data",
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "arrayType" | "array_type" => Ok(GeneratedField::ArrayType),
                            "nullBitmap" | "null_bitmap" => Ok(GeneratedField::NullBitmap),
                            "values" => Ok(GeneratedField::Values),
                            "structArrayData" | "struct_array_data" => Ok(GeneratedField::StructArrayData),
                            "listArrayData" | "list_array_data" => Ok(GeneratedField::ListArrayData),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Array, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut array_type__ = None;
                let mut null_bitmap__ = None;
                let mut values__ = None;
                let mut struct_array_data__ = None;
                let mut list_array_data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ArrayType => {
                            if array_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrayType"));
                            }
                            array_type__ = Some(map_.next_value::<ArrayType>()? as i32);
                        }
                        GeneratedField::NullBitmap => {
                            if null_bitmap__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullBitmap"));
                            }
                            null_bitmap__ = map_.next_value()?;
                        }
                        GeneratedField::Values => {
                            if values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values__ = Some(map_.next_value()?);
                        }
                        GeneratedField::StructArrayData => {
                            if struct_array_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("structArrayData"));
                            }
                            struct_array_data__ = map_.next_value()?;
                        }
                        GeneratedField::ListArrayData => {
                            if list_array_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listArrayData"));
                            }
                            list_array_data__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Array {
                    array_type: array_type__.unwrap_or_default(),
                    null_bitmap: null_bitmap__,
                    values: values__.unwrap_or_default(),
                    struct_array_data: struct_array_data__,
                    list_array_data: list_array_data__,
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
            Self::Unspecified => "UNSPECIFIED",
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
            Self::Timestamptz => "TIMESTAMPTZ",
            Self::Interval => "INTERVAL",
            Self::Struct => "STRUCT",
            Self::List => "LIST",
            Self::Bytea => "BYTEA",
            Self::Jsonb => "JSONB",
            Self::Serial => "SERIAL",
            Self::Int256 => "INT256",
            Self::Map => "MAP",
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
            "UNSPECIFIED",
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
            "TIMESTAMPTZ",
            "INTERVAL",
            "STRUCT",
            "LIST",
            "BYTEA",
            "JSONB",
            "SERIAL",
            "INT256",
            "MAP",
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
                    "UNSPECIFIED" => Ok(ArrayType::Unspecified),
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
                    "TIMESTAMPTZ" => Ok(ArrayType::Timestamptz),
                    "INTERVAL" => Ok(ArrayType::Interval),
                    "STRUCT" => Ok(ArrayType::Struct),
                    "LIST" => Ok(ArrayType::List),
                    "BYTEA" => Ok(ArrayType::Bytea),
                    "JSONB" => Ok(ArrayType::Jsonb),
                    "SERIAL" => Ok(ArrayType::Serial),
                    "INT256" => Ok(ArrayType::Int256),
                    "MAP" => Ok(ArrayType::Map),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
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

                    #[allow(unused_variables)]
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DataChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cardinality__ = None;
                let mut columns__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Cardinality => {
                            if cardinality__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cardinality"));
                            }
                            cardinality__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(DataChunk {
                    cardinality: cardinality__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
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
        if !self.field_type.is_empty() {
            len += 1;
        }
        if !self.field_names.is_empty() {
            len += 1;
        }
        if !self.field_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.DataType", len)?;
        if self.type_name != 0 {
            let v = data_type::TypeName::try_from(self.type_name)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.type_name)))?;
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
            let v = data_type::IntervalType::try_from(self.interval_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.interval_type)))?;
            struct_ser.serialize_field("intervalType", &v)?;
        }
        if !self.field_type.is_empty() {
            struct_ser.serialize_field("fieldType", &self.field_type)?;
        }
        if !self.field_names.is_empty() {
            struct_ser.serialize_field("fieldNames", &self.field_names)?;
        }
        if !self.field_ids.is_empty() {
            struct_ser.serialize_field("fieldIds", &self.field_ids)?;
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
            "type_name",
            "typeName",
            "precision",
            "scale",
            "is_nullable",
            "isNullable",
            "interval_type",
            "intervalType",
            "field_type",
            "fieldType",
            "field_names",
            "fieldNames",
            "field_ids",
            "fieldIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TypeName,
            Precision,
            Scale,
            IsNullable,
            IntervalType,
            FieldType,
            FieldNames,
            FieldIds,
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
                            "typeName" | "type_name" => Ok(GeneratedField::TypeName),
                            "precision" => Ok(GeneratedField::Precision),
                            "scale" => Ok(GeneratedField::Scale),
                            "isNullable" | "is_nullable" => Ok(GeneratedField::IsNullable),
                            "intervalType" | "interval_type" => Ok(GeneratedField::IntervalType),
                            "fieldType" | "field_type" => Ok(GeneratedField::FieldType),
                            "fieldNames" | "field_names" => Ok(GeneratedField::FieldNames),
                            "fieldIds" | "field_ids" => Ok(GeneratedField::FieldIds),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DataType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut type_name__ = None;
                let mut precision__ = None;
                let mut scale__ = None;
                let mut is_nullable__ = None;
                let mut interval_type__ = None;
                let mut field_type__ = None;
                let mut field_names__ = None;
                let mut field_ids__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TypeName => {
                            if type_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeName"));
                            }
                            type_name__ = Some(map_.next_value::<data_type::TypeName>()? as i32);
                        }
                        GeneratedField::Precision => {
                            if precision__.is_some() {
                                return Err(serde::de::Error::duplicate_field("precision"));
                            }
                            precision__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Scale => {
                            if scale__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scale"));
                            }
                            scale__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::IsNullable => {
                            if is_nullable__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullable"));
                            }
                            is_nullable__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IntervalType => {
                            if interval_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("intervalType"));
                            }
                            interval_type__ = Some(map_.next_value::<data_type::IntervalType>()? as i32);
                        }
                        GeneratedField::FieldType => {
                            if field_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldType"));
                            }
                            field_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FieldNames => {
                            if field_names__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldNames"));
                            }
                            field_names__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FieldIds => {
                            if field_ids__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldIds"));
                            }
                            field_ids__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(DataType {
                    type_name: type_name__.unwrap_or_default(),
                    precision: precision__.unwrap_or_default(),
                    scale: scale__.unwrap_or_default(),
                    is_nullable: is_nullable__.unwrap_or_default(),
                    interval_type: interval_type__.unwrap_or_default(),
                    field_type: field_type__.unwrap_or_default(),
                    field_names: field_names__.unwrap_or_default(),
                    field_ids: field_ids__.unwrap_or_default(),
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
            Self::Unspecified => "UNSPECIFIED",
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
            "UNSPECIFIED",
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
                    "UNSPECIFIED" => Ok(data_type::IntervalType::Unspecified),
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
            Self::TypeUnspecified => "TYPE_UNSPECIFIED",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Boolean => "BOOLEAN",
            Self::Varchar => "VARCHAR",
            Self::Decimal => "DECIMAL",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::Interval => "INTERVAL",
            Self::Date => "DATE",
            Self::Timestamptz => "TIMESTAMPTZ",
            Self::Struct => "STRUCT",
            Self::List => "LIST",
            Self::Bytea => "BYTEA",
            Self::Jsonb => "JSONB",
            Self::Serial => "SERIAL",
            Self::Int256 => "INT256",
            Self::Map => "MAP",
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
            "TYPE_UNSPECIFIED",
            "INT16",
            "INT32",
            "INT64",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "VARCHAR",
            "DECIMAL",
            "TIME",
            "TIMESTAMP",
            "INTERVAL",
            "DATE",
            "TIMESTAMPTZ",
            "STRUCT",
            "LIST",
            "BYTEA",
            "JSONB",
            "SERIAL",
            "INT256",
            "MAP",
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
                    "TYPE_UNSPECIFIED" => Ok(data_type::TypeName::TypeUnspecified),
                    "INT16" => Ok(data_type::TypeName::Int16),
                    "INT32" => Ok(data_type::TypeName::Int32),
                    "INT64" => Ok(data_type::TypeName::Int64),
                    "FLOAT" => Ok(data_type::TypeName::Float),
                    "DOUBLE" => Ok(data_type::TypeName::Double),
                    "BOOLEAN" => Ok(data_type::TypeName::Boolean),
                    "VARCHAR" => Ok(data_type::TypeName::Varchar),
                    "DECIMAL" => Ok(data_type::TypeName::Decimal),
                    "TIME" => Ok(data_type::TypeName::Time),
                    "TIMESTAMP" => Ok(data_type::TypeName::Timestamp),
                    "INTERVAL" => Ok(data_type::TypeName::Interval),
                    "DATE" => Ok(data_type::TypeName::Date),
                    "TIMESTAMPTZ" => Ok(data_type::TypeName::Timestamptz),
                    "STRUCT" => Ok(data_type::TypeName::Struct),
                    "LIST" => Ok(data_type::TypeName::List),
                    "BYTEA" => Ok(data_type::TypeName::Bytea),
                    "JSONB" => Ok(data_type::TypeName::Jsonb),
                    "SERIAL" => Ok(data_type::TypeName::Serial),
                    "INT256" => Ok(data_type::TypeName::Int256),
                    "MAP" => Ok(data_type::TypeName::Map),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for Datum {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.body.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Datum", len)?;
        if !self.body.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("body", pbjson::private::base64::encode(&self.body).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Datum {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "body",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = Datum;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Datum")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Datum, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut body__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
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
                Ok(Datum {
                    body: body__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Datum", FIELDS, GeneratedVisitor)
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
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("curr", ToString::to_string(&self.curr).as_str())?;
        }
        if self.prev != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
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

                    #[allow(unused_variables)]
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Epoch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut curr__ = None;
                let mut prev__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Curr => {
                            if curr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("curr"));
                            }
                            curr__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Prev => {
                            if prev__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prev"));
                            }
                            prev__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Epoch {
                    curr: curr__.unwrap_or_default(),
                    prev: prev__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Epoch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Interval {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.months != 0 {
            len += 1;
        }
        if self.days != 0 {
            len += 1;
        }
        if self.usecs != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("data.Interval", len)?;
        if self.months != 0 {
            struct_ser.serialize_field("months", &self.months)?;
        }
        if self.days != 0 {
            struct_ser.serialize_field("days", &self.days)?;
        }
        if self.usecs != 0 {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("usecs", ToString::to_string(&self.usecs).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Interval {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "months",
            "days",
            "usecs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Months,
            Days,
            Usecs,
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
                            "months" => Ok(GeneratedField::Months),
                            "days" => Ok(GeneratedField::Days),
                            "usecs" => Ok(GeneratedField::Usecs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Interval;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Interval")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Interval, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut months__ = None;
                let mut days__ = None;
                let mut usecs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Months => {
                            if months__.is_some() {
                                return Err(serde::de::Error::duplicate_field("months"));
                            }
                            months__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Days => {
                            if days__.is_some() {
                                return Err(serde::de::Error::duplicate_field("days"));
                            }
                            days__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Usecs => {
                            if usecs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("usecs"));
                            }
                            usecs__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(Interval {
                    months: months__.unwrap_or_default(),
                    days: days__.unwrap_or_default(),
                    usecs: usecs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.Interval", FIELDS, GeneratedVisitor)
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
            "value_type",
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "offsets" => Ok(GeneratedField::Offsets),
                            "value" => Ok(GeneratedField::Value),
                            "valueType" | "value_type" => Ok(GeneratedField::ValueType),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListArrayData, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut offsets__ = None;
                let mut value__ = None;
                let mut value_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Offsets => {
                            if offsets__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offsets"));
                            }
                            offsets__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = map_.next_value()?;
                        }
                        GeneratedField::ValueType => {
                            if value_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valueType"));
                            }
                            value_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ListArrayData {
                    offsets: offsets__.unwrap_or_default(),
                    value: value__,
                    value_type: value_type__,
                })
            }
        }
        deserializer.deserialize_struct("data.ListArrayData", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Op {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "OP_UNSPECIFIED",
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
            "OP_UNSPECIFIED",
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
                    "OP_UNSPECIFIED" => Ok(Op::Unspecified),
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
                Op::try_from(v)
                    .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", v)))
                }).collect::<std::result::Result<Vec<_>, _>>()?;
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

                    #[allow(unused_variables)]
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StreamChunk, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cardinality__ = None;
                let mut ops__ = None;
                let mut columns__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Cardinality => {
                            if cardinality__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cardinality"));
                            }
                            cardinality__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Ops => {
                            if ops__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ops"));
                            }
                            ops__ = Some(map_.next_value::<Vec<Op>>()?.into_iter().map(|x| x as i32).collect());
                        }
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(StreamChunk {
                    cardinality: cardinality__.unwrap_or_default(),
                    ops: ops__.unwrap_or_default(),
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("data.StreamChunk", FIELDS, GeneratedVisitor)
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
            "children_array",
            "childrenArray",
            "children_type",
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "childrenArray" | "children_array" => Ok(GeneratedField::ChildrenArray),
                            "childrenType" | "children_type" => Ok(GeneratedField::ChildrenType),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StructArrayData, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children_array__ = None;
                let mut children_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ChildrenArray => {
                            if children_array__.is_some() {
                                return Err(serde::de::Error::duplicate_field("childrenArray"));
                            }
                            children_array__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ChildrenType => {
                            if children_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("childrenType"));
                            }
                            children_type__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(StructArrayData {
                    children_array: children_array__.unwrap_or_default(),
                    children_type: children_type__.unwrap_or_default(),
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
            type Value = Terminate;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct data.Terminate")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Terminate, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(Terminate {
                })
            }
        }
        deserializer.deserialize_struct("data.Terminate", FIELDS, GeneratedVisitor)
    }
}
