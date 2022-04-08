use crate::plan::*;
impl serde::Serialize for CellBasedTableDesc {
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
        if !self.pk.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.CellBasedTableDesc", len)?;
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        if !self.pk.is_empty() {
            struct_ser.serialize_field("pk", &self.pk)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CellBasedTableDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableId",
            "pk",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableId,
            Pk,
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
                            "pk" => Ok(GeneratedField::Pk),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CellBasedTableDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.CellBasedTableDesc")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CellBasedTableDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_id = None;
                let mut pk = None;
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
                        GeneratedField::Pk => {
                            if pk.is_some() {
                                return Err(serde::de::Error::duplicate_field("pk"));
                            }
                            pk = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CellBasedTableDesc {
                    table_id: table_id.unwrap_or_default(),
                    pk: pk.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.CellBasedTableDesc", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("plan.ColumnCatalog", len)?;
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
            "columnDesc",
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columnDesc" => Ok(GeneratedField::ColumnDesc),
                            "isHidden" => Ok(GeneratedField::IsHidden),
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
                formatter.write_str("struct plan.ColumnCatalog")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnCatalog, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_desc = None;
                let mut is_hidden = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnDesc => {
                            if column_desc.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDesc"));
                            }
                            column_desc = Some(map.next_value()?);
                        }
                        GeneratedField::IsHidden => {
                            if is_hidden.is_some() {
                                return Err(serde::de::Error::duplicate_field("isHidden"));
                            }
                            is_hidden = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ColumnCatalog {
                    column_desc,
                    is_hidden: is_hidden.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ColumnCatalog", FIELDS, GeneratedVisitor)
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
        if !self.field_descs.is_empty() {
            len += 1;
        }
        if !self.type_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ColumnDesc", len)?;
        if let Some(v) = self.column_type.as_ref() {
            struct_ser.serialize_field("columnType", v)?;
        }
        if self.column_id != 0 {
            struct_ser.serialize_field("columnId", &self.column_id)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.field_descs.is_empty() {
            struct_ser.serialize_field("fieldDescs", &self.field_descs)?;
        }
        if !self.type_name.is_empty() {
            struct_ser.serialize_field("typeName", &self.type_name)?;
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
            "columnType",
            "columnId",
            "name",
            "fieldDescs",
            "typeName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnType,
            ColumnId,
            Name,
            FieldDescs,
            TypeName,
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
                            "columnType" => Ok(GeneratedField::ColumnType),
                            "columnId" => Ok(GeneratedField::ColumnId),
                            "name" => Ok(GeneratedField::Name),
                            "fieldDescs" => Ok(GeneratedField::FieldDescs),
                            "typeName" => Ok(GeneratedField::TypeName),
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
                formatter.write_str("struct plan.ColumnDesc")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_type = None;
                let mut column_id = None;
                let mut name = None;
                let mut field_descs = None;
                let mut type_name = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnType => {
                            if column_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnType"));
                            }
                            column_type = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnId => {
                            if column_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnId"));
                            }
                            column_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Name => {
                            if name.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        GeneratedField::FieldDescs => {
                            if field_descs.is_some() {
                                return Err(serde::de::Error::duplicate_field("fieldDescs"));
                            }
                            field_descs = Some(map.next_value()?);
                        }
                        GeneratedField::TypeName => {
                            if type_name.is_some() {
                                return Err(serde::de::Error::duplicate_field("typeName"));
                            }
                            type_name = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ColumnDesc {
                    column_type,
                    column_id: column_id.unwrap_or_default(),
                    name: name.unwrap_or_default(),
                    field_descs: field_descs.unwrap_or_default(),
                    type_name: type_name.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ColumnDesc", FIELDS, GeneratedVisitor)
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
        if self.order_type != 0 {
            len += 1;
        }
        if self.input_ref.is_some() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ColumnOrder", len)?;
        if self.order_type != 0 {
            let v = OrderType::from_i32(self.order_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.order_type)))?;
            struct_ser.serialize_field("orderType", &v)?;
        }
        if let Some(v) = self.input_ref.as_ref() {
            struct_ser.serialize_field("inputRef", v)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
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
            "orderType",
            "inputRef",
            "returnType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OrderType,
            InputRef,
            ReturnType,
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
                            "orderType" => Ok(GeneratedField::OrderType),
                            "inputRef" => Ok(GeneratedField::InputRef),
                            "returnType" => Ok(GeneratedField::ReturnType),
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
                formatter.write_str("struct plan.ColumnOrder")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ColumnOrder, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut order_type = None;
                let mut input_ref = None;
                let mut return_type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OrderType => {
                            if order_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderType"));
                            }
                            order_type = Some(map.next_value::<OrderType>()? as i32);
                        }
                        GeneratedField::InputRef => {
                            if input_ref.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputRef"));
                            }
                            input_ref = Some(map.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ColumnOrder {
                    order_type: order_type.unwrap_or_default(),
                    input_ref,
                    return_type,
                })
            }
        }
        deserializer.deserialize_struct("plan.ColumnOrder", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateSourceNode {
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
        if self.info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.CreateSourceNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.column_descs.is_empty() {
            struct_ser.serialize_field("columnDescs", &self.column_descs)?;
        }
        if let Some(v) = self.info.as_ref() {
            struct_ser.serialize_field("info", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateSourceNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "columnDescs",
            "info",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            ColumnDescs,
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
                            "tableRefId" => Ok(GeneratedField::TableRefId),
                            "columnDescs" => Ok(GeneratedField::ColumnDescs),
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
            type Value = CreateSourceNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.CreateSourceNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateSourceNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut column_descs = None;
                let mut info = None;
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
                        GeneratedField::Info => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("info"));
                            }
                            info = Some(map.next_value()?);
                        }
                    }
                }
                Ok(CreateSourceNode {
                    table_ref_id,
                    column_descs: column_descs.unwrap_or_default(),
                    info,
                })
            }
        }
        deserializer.deserialize_struct("plan.CreateSourceNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTableNode {
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
        if self.info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.CreateTableNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.column_descs.is_empty() {
            struct_ser.serialize_field("columnDescs", &self.column_descs)?;
        }
        if let Some(v) = self.info.as_ref() {
            match v {
                create_table_node::Info::TableSource(v) => {
                    struct_ser.serialize_field("tableSource", v)?;
                }
                create_table_node::Info::MaterializedView(v) => {
                    struct_ser.serialize_field("materializedView", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTableNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "columnDescs",
            "tableSource",
            "materializedView",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            ColumnDescs,
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
                            "columnDescs" => Ok(GeneratedField::ColumnDescs),
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
            type Value = CreateTableNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.CreateTableNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateTableNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut column_descs = None;
                let mut info = None;
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
                        GeneratedField::TableSource => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSource"));
                            }
                            info = Some(create_table_node::Info::TableSource(map.next_value()?));
                        }
                        GeneratedField::MaterializedView => {
                            if info.is_some() {
                                return Err(serde::de::Error::duplicate_field("materializedView"));
                            }
                            info = Some(create_table_node::Info::MaterializedView(map.next_value()?));
                        }
                    }
                }
                Ok(CreateTableNode {
                    table_ref_id,
                    column_descs: column_descs.unwrap_or_default(),
                    info,
                })
            }
        }
        deserializer.deserialize_struct("plan.CreateTableNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DatabaseRefId {
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
        let mut struct_ser = serializer.serialize_struct("plan.DatabaseRefId", len)?;
        if self.database_id != 0 {
            struct_ser.serialize_field("databaseId", &self.database_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DatabaseRefId {
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
            type Value = DatabaseRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.DatabaseRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DatabaseRefId, V::Error>
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
                Ok(DatabaseRefId {
                    database_id: database_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.DatabaseRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DeleteNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_source_ref_id.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.DeleteNode", len)?;
        if let Some(v) = self.table_source_ref_id.as_ref() {
            struct_ser.serialize_field("tableSourceRefId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DeleteNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableSourceRefId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableSourceRefId,
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
                            "tableSourceRefId" => Ok(GeneratedField::TableSourceRefId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DeleteNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.DeleteNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DeleteNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_source_ref_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableSourceRefId => {
                            if table_source_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSourceRefId"));
                            }
                            table_source_ref_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DeleteNode {
                    table_source_ref_id,
                })
            }
        }
        deserializer.deserialize_struct("plan.DeleteNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropSourceNode {
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
        let mut struct_ser = serializer.serialize_struct("plan.DropSourceNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropSourceNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = DropSourceNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.DropSourceNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropSourceNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DropSourceNode {
                    table_ref_id,
                })
            }
        }
        deserializer.deserialize_struct("plan.DropSourceNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropTableNode {
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
        let mut struct_ser = serializer.serialize_struct("plan.DropTableNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropTableNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = DropTableNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.DropTableNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<DropTableNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(DropTableNode {
                    table_ref_id,
                })
            }
        }
        deserializer.deserialize_struct("plan.DropTableNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExchangeInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.mode != 0 {
            len += 1;
        }
        if self.distribution.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ExchangeInfo", len)?;
        if self.mode != 0 {
            let v = exchange_info::DistributionMode::from_i32(self.mode)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.mode)))?;
            struct_ser.serialize_field("mode", &v)?;
        }
        if let Some(v) = self.distribution.as_ref() {
            match v {
                exchange_info::Distribution::BroadcastInfo(v) => {
                    struct_ser.serialize_field("broadcastInfo", v)?;
                }
                exchange_info::Distribution::HashInfo(v) => {
                    struct_ser.serialize_field("hashInfo", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExchangeInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "mode",
            "broadcastInfo",
            "hashInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Mode,
            BroadcastInfo,
            HashInfo,
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
                            "mode" => Ok(GeneratedField::Mode),
                            "broadcastInfo" => Ok(GeneratedField::BroadcastInfo),
                            "hashInfo" => Ok(GeneratedField::HashInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExchangeInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ExchangeInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExchangeInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut mode = None;
                let mut distribution = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Mode => {
                            if mode.is_some() {
                                return Err(serde::de::Error::duplicate_field("mode"));
                            }
                            mode = Some(map.next_value::<exchange_info::DistributionMode>()? as i32);
                        }
                        GeneratedField::BroadcastInfo => {
                            if distribution.is_some() {
                                return Err(serde::de::Error::duplicate_field("broadcastInfo"));
                            }
                            distribution = Some(exchange_info::Distribution::BroadcastInfo(map.next_value()?));
                        }
                        GeneratedField::HashInfo => {
                            if distribution.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashInfo"));
                            }
                            distribution = Some(exchange_info::Distribution::HashInfo(map.next_value()?));
                        }
                    }
                }
                Ok(ExchangeInfo {
                    mode: mode.unwrap_or_default(),
                    distribution,
                })
            }
        }
        deserializer.deserialize_struct("plan.ExchangeInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for exchange_info::BroadcastInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ExchangeInfo.BroadcastInfo", len)?;
        if self.count != 0 {
            struct_ser.serialize_field("count", &self.count)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for exchange_info::BroadcastInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "count",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Count,
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
                            "count" => Ok(GeneratedField::Count),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = exchange_info::BroadcastInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ExchangeInfo.BroadcastInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<exchange_info::BroadcastInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut count = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Count => {
                            if count.is_some() {
                                return Err(serde::de::Error::duplicate_field("count"));
                            }
                            count = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(exchange_info::BroadcastInfo {
                    count: count.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ExchangeInfo.BroadcastInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for exchange_info::DistributionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Single => "SINGLE",
            Self::Broadcast => "BROADCAST",
            Self::Hash => "HASH",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for exchange_info::DistributionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SINGLE",
            "BROADCAST",
            "HASH",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = exchange_info::DistributionMode;

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
                    .and_then(exchange_info::DistributionMode::from_i32)
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
                    .and_then(exchange_info::DistributionMode::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SINGLE" => Ok(exchange_info::DistributionMode::Single),
                    "BROADCAST" => Ok(exchange_info::DistributionMode::Broadcast),
                    "HASH" => Ok(exchange_info::DistributionMode::Hash),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for exchange_info::HashInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.output_count != 0 {
            len += 1;
        }
        if !self.keys.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ExchangeInfo.HashInfo", len)?;
        if self.output_count != 0 {
            struct_ser.serialize_field("outputCount", &self.output_count)?;
        }
        if !self.keys.is_empty() {
            struct_ser.serialize_field("keys", &self.keys)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for exchange_info::HashInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "outputCount",
            "keys",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OutputCount,
            Keys,
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
                            "outputCount" => Ok(GeneratedField::OutputCount),
                            "keys" => Ok(GeneratedField::Keys),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = exchange_info::HashInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ExchangeInfo.HashInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<exchange_info::HashInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut output_count = None;
                let mut keys = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::OutputCount => {
                            if output_count.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputCount"));
                            }
                            output_count = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Keys => {
                            if keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("keys"));
                            }
                            keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(exchange_info::HashInfo {
                    output_count: output_count.unwrap_or_default(),
                    keys: keys.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ExchangeInfo.HashInfo", FIELDS, GeneratedVisitor)
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
        if !self.sources.is_empty() {
            len += 1;
        }
        if !self.input_schema.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ExchangeNode", len)?;
        if !self.sources.is_empty() {
            struct_ser.serialize_field("sources", &self.sources)?;
        }
        if !self.input_schema.is_empty() {
            struct_ser.serialize_field("inputSchema", &self.input_schema)?;
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
            "sources",
            "inputSchema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Sources,
            InputSchema,
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
                            "sources" => Ok(GeneratedField::Sources),
                            "inputSchema" => Ok(GeneratedField::InputSchema),
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
                formatter.write_str("struct plan.ExchangeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExchangeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sources = None;
                let mut input_schema = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Sources => {
                            if sources.is_some() {
                                return Err(serde::de::Error::duplicate_field("sources"));
                            }
                            sources = Some(map.next_value()?);
                        }
                        GeneratedField::InputSchema => {
                            if input_schema.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputSchema"));
                            }
                            input_schema = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExchangeNode {
                    sources: sources.unwrap_or_default(),
                    input_schema: input_schema.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ExchangeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExchangeSource {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.task_output_id.is_some() {
            len += 1;
        }
        if self.host.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ExchangeSource", len)?;
        if let Some(v) = self.task_output_id.as_ref() {
            struct_ser.serialize_field("taskOutputId", v)?;
        }
        if let Some(v) = self.host.as_ref() {
            struct_ser.serialize_field("host", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExchangeSource {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskOutputId",
            "host",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskOutputId,
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
                            "taskOutputId" => Ok(GeneratedField::TaskOutputId),
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
            type Value = ExchangeSource;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ExchangeSource")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExchangeSource, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_output_id = None;
                let mut host = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskOutputId => {
                            if task_output_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskOutputId"));
                            }
                            task_output_id = Some(map.next_value()?);
                        }
                        GeneratedField::Host => {
                            if host.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ExchangeSource {
                    task_output_id,
                    host,
                })
            }
        }
        deserializer.deserialize_struct("plan.ExchangeSource", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("plan.Field", len)?;
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "dataType" => Ok(GeneratedField::DataType),
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
                formatter.write_str("struct plan.Field")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<Field, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut data_type = None;
                let mut name = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DataType => {
                            if data_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type = Some(map.next_value()?);
                        }
                        GeneratedField::Name => {
                            if name.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                    }
                }
                Ok(Field {
                    data_type,
                    name: name.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.Field", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("plan.FilterNode", len)?;
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
                formatter.write_str("struct plan.FilterNode")
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
        deserializer.deserialize_struct("plan.FilterNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FilterScanNode {
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
        let mut struct_ser = serializer.serialize_struct("plan.FilterScanNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FilterScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "columnIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
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
            type Value = FilterScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.FilterScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FilterScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut column_ids = None;
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
                    }
                }
                Ok(FilterScanNode {
                    table_ref_id,
                    column_ids: column_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.FilterScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GenerateInt32SeriesNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start != 0 {
            len += 1;
        }
        if self.stop != 0 {
            len += 1;
        }
        if self.step != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.GenerateInt32SeriesNode", len)?;
        if self.start != 0 {
            struct_ser.serialize_field("start", &self.start)?;
        }
        if self.stop != 0 {
            struct_ser.serialize_field("stop", &self.stop)?;
        }
        if self.step != 0 {
            struct_ser.serialize_field("step", &self.step)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GenerateInt32SeriesNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "stop",
            "step",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            Stop,
            Step,
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
                            "start" => Ok(GeneratedField::Start),
                            "stop" => Ok(GeneratedField::Stop),
                            "step" => Ok(GeneratedField::Step),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GenerateInt32SeriesNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.GenerateInt32SeriesNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GenerateInt32SeriesNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start = None;
                let mut stop = None;
                let mut step = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Stop => {
                            if stop.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            stop = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::Step => {
                            if step.is_some() {
                                return Err(serde::de::Error::duplicate_field("step"));
                            }
                            step = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(GenerateInt32SeriesNode {
                    start: start.unwrap_or_default(),
                    stop: stop.unwrap_or_default(),
                    step: step.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.GenerateInt32SeriesNode", FIELDS, GeneratedVisitor)
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
        if !self.group_keys.is_empty() {
            len += 1;
        }
        if !self.agg_calls.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.HashAggNode", len)?;
        if !self.group_keys.is_empty() {
            struct_ser.serialize_field("groupKeys", &self.group_keys)?;
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
            "groupKeys",
            "aggCalls",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            GroupKeys,
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
                            "groupKeys" => Ok(GeneratedField::GroupKeys),
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
                formatter.write_str("struct plan.HashAggNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashAggNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut group_keys = None;
                let mut agg_calls = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::GroupKeys => {
                            if group_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupKeys"));
                            }
                            group_keys = Some(
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
                    group_keys: group_keys.unwrap_or_default(),
                    agg_calls: agg_calls.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.HashAggNode", FIELDS, GeneratedVisitor)
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
        if !self.left_output.is_empty() {
            len += 1;
        }
        if !self.right_key.is_empty() {
            len += 1;
        }
        if !self.right_output.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.HashJoinNode", len)?;
        if self.join_type != 0 {
            let v = JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if !self.left_key.is_empty() {
            struct_ser.serialize_field("leftKey", &self.left_key)?;
        }
        if !self.left_output.is_empty() {
            struct_ser.serialize_field("leftOutput", &self.left_output)?;
        }
        if !self.right_key.is_empty() {
            struct_ser.serialize_field("rightKey", &self.right_key)?;
        }
        if !self.right_output.is_empty() {
            struct_ser.serialize_field("rightOutput", &self.right_output)?;
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
            "leftOutput",
            "rightKey",
            "rightOutput",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JoinType,
            LeftKey,
            LeftOutput,
            RightKey,
            RightOutput,
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
                            "leftOutput" => Ok(GeneratedField::LeftOutput),
                            "rightKey" => Ok(GeneratedField::RightKey),
                            "rightOutput" => Ok(GeneratedField::RightOutput),
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
                formatter.write_str("struct plan.HashJoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<HashJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut join_type = None;
                let mut left_key = None;
                let mut left_output = None;
                let mut right_key = None;
                let mut right_output = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JoinType => {
                            if join_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type = Some(map.next_value::<JoinType>()? as i32);
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
                        GeneratedField::LeftOutput => {
                            if left_output.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftOutput"));
                            }
                            left_output = Some(
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
                        GeneratedField::RightOutput => {
                            if right_output.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightOutput"));
                            }
                            right_output = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                    }
                }
                Ok(HashJoinNode {
                    join_type: join_type.unwrap_or_default(),
                    left_key: left_key.unwrap_or_default(),
                    left_output: left_output.unwrap_or_default(),
                    right_key: right_key.unwrap_or_default(),
                    right_output: right_output.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.HashJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InsertNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_source_ref_id.is_some() {
            len += 1;
        }
        if !self.column_ids.is_empty() {
            len += 1;
        }
        if self.frontend_v2 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.InsertNode", len)?;
        if let Some(v) = self.table_source_ref_id.as_ref() {
            struct_ser.serialize_field("tableSourceRefId", v)?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        if self.frontend_v2 {
            struct_ser.serialize_field("frontendV2", &self.frontend_v2)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InsertNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableSourceRefId",
            "columnIds",
            "frontendV2",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableSourceRefId,
            ColumnIds,
            FrontendV2,
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
                            "tableSourceRefId" => Ok(GeneratedField::TableSourceRefId),
                            "columnIds" => Ok(GeneratedField::ColumnIds),
                            "frontendV2" => Ok(GeneratedField::FrontendV2),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InsertNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.InsertNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<InsertNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_source_ref_id = None;
                let mut column_ids = None;
                let mut frontend_v2 = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableSourceRefId => {
                            if table_source_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableSourceRefId"));
                            }
                            table_source_ref_id = Some(map.next_value()?);
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
                        GeneratedField::FrontendV2 => {
                            if frontend_v2.is_some() {
                                return Err(serde::de::Error::duplicate_field("frontendV2"));
                            }
                            frontend_v2 = Some(map.next_value()?);
                        }
                    }
                }
                Ok(InsertNode {
                    table_source_ref_id,
                    column_ids: column_ids.unwrap_or_default(),
                    frontend_v2: frontend_v2.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.InsertNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Inner => "INNER",
            Self::LeftOuter => "LEFT_OUTER",
            Self::RightOuter => "RIGHT_OUTER",
            Self::FullOuter => "FULL_OUTER",
            Self::LeftSemi => "LEFT_SEMI",
            Self::LeftAnti => "LEFT_ANTI",
            Self::RightSemi => "RIGHT_SEMI",
            Self::RightAnti => "RIGHT_ANTI",
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
            "INNER",
            "LEFT_OUTER",
            "RIGHT_OUTER",
            "FULL_OUTER",
            "LEFT_SEMI",
            "LEFT_ANTI",
            "RIGHT_SEMI",
            "RIGHT_ANTI",
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
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(JoinType::from_i32)
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
                    .and_then(JoinType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INNER" => Ok(JoinType::Inner),
                    "LEFT_OUTER" => Ok(JoinType::LeftOuter),
                    "RIGHT_OUTER" => Ok(JoinType::RightOuter),
                    "FULL_OUTER" => Ok(JoinType::FullOuter),
                    "LEFT_SEMI" => Ok(JoinType::LeftSemi),
                    "LEFT_ANTI" => Ok(JoinType::LeftAnti),
                    "RIGHT_SEMI" => Ok(JoinType::RightSemi),
                    "RIGHT_ANTI" => Ok(JoinType::RightAnti),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for LimitNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.limit != 0 {
            len += 1;
        }
        if self.offset != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.LimitNode", len)?;
        if self.limit != 0 {
            struct_ser.serialize_field("limit", &self.limit)?;
        }
        if self.offset != 0 {
            struct_ser.serialize_field("offset", &self.offset)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LimitNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "limit",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Limit,
            Offset,
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
                            "limit" => Ok(GeneratedField::Limit),
                            "offset" => Ok(GeneratedField::Offset),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LimitNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.LimitNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<LimitNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut limit = None;
                let mut offset = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                    }
                }
                Ok(LimitNode {
                    limit: limit.unwrap_or_default(),
                    offset: offset.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.LimitNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaterializedViewInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.associated_table_ref_id.is_some() {
            len += 1;
        }
        if !self.column_orders.is_empty() {
            len += 1;
        }
        if !self.pk_indices.is_empty() {
            len += 1;
        }
        if !self.dependent_tables.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.MaterializedViewInfo", len)?;
        if let Some(v) = self.associated_table_ref_id.as_ref() {
            struct_ser.serialize_field("associatedTableRefId", v)?;
        }
        if !self.column_orders.is_empty() {
            struct_ser.serialize_field("columnOrders", &self.column_orders)?;
        }
        if !self.pk_indices.is_empty() {
            struct_ser.serialize_field("pkIndices", &self.pk_indices)?;
        }
        if !self.dependent_tables.is_empty() {
            struct_ser.serialize_field("dependentTables", &self.dependent_tables)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaterializedViewInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "associatedTableRefId",
            "columnOrders",
            "pkIndices",
            "dependentTables",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AssociatedTableRefId,
            ColumnOrders,
            PkIndices,
            DependentTables,
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
                            "associatedTableRefId" => Ok(GeneratedField::AssociatedTableRefId),
                            "columnOrders" => Ok(GeneratedField::ColumnOrders),
                            "pkIndices" => Ok(GeneratedField::PkIndices),
                            "dependentTables" => Ok(GeneratedField::DependentTables),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaterializedViewInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.MaterializedViewInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MaterializedViewInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut associated_table_ref_id = None;
                let mut column_orders = None;
                let mut pk_indices = None;
                let mut dependent_tables = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                        GeneratedField::PkIndices => {
                            if pk_indices.is_some() {
                                return Err(serde::de::Error::duplicate_field("pkIndices"));
                            }
                            pk_indices = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::DependentTables => {
                            if dependent_tables.is_some() {
                                return Err(serde::de::Error::duplicate_field("dependentTables"));
                            }
                            dependent_tables = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MaterializedViewInfo {
                    associated_table_ref_id,
                    column_orders: column_orders.unwrap_or_default(),
                    pk_indices: pk_indices.unwrap_or_default(),
                    dependent_tables: dependent_tables.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.MaterializedViewInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MergeSortExchangeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.exchange_node.is_some() {
            len += 1;
        }
        if !self.column_orders.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.MergeSortExchangeNode", len)?;
        if let Some(v) = self.exchange_node.as_ref() {
            struct_ser.serialize_field("exchangeNode", v)?;
        }
        if !self.column_orders.is_empty() {
            struct_ser.serialize_field("columnOrders", &self.column_orders)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MergeSortExchangeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exchangeNode",
            "columnOrders",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExchangeNode,
            ColumnOrders,
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
                            "exchangeNode" => Ok(GeneratedField::ExchangeNode),
                            "columnOrders" => Ok(GeneratedField::ColumnOrders),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MergeSortExchangeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.MergeSortExchangeNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<MergeSortExchangeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut exchange_node = None;
                let mut column_orders = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ExchangeNode => {
                            if exchange_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("exchangeNode"));
                            }
                            exchange_node = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnOrders => {
                            if column_orders.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnOrders"));
                            }
                            column_orders = Some(map.next_value()?);
                        }
                    }
                }
                Ok(MergeSortExchangeNode {
                    exchange_node,
                    column_orders: column_orders.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.MergeSortExchangeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NestedLoopJoinNode {
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
        if self.join_cond.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.NestedLoopJoinNode", len)?;
        if self.join_type != 0 {
            let v = JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if let Some(v) = self.join_cond.as_ref() {
            struct_ser.serialize_field("joinCond", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NestedLoopJoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "joinType",
            "joinCond",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JoinType,
            JoinCond,
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
                            "joinCond" => Ok(GeneratedField::JoinCond),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NestedLoopJoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.NestedLoopJoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<NestedLoopJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut join_type = None;
                let mut join_cond = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JoinType => {
                            if join_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type = Some(map.next_value::<JoinType>()? as i32);
                        }
                        GeneratedField::JoinCond => {
                            if join_cond.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinCond"));
                            }
                            join_cond = Some(map.next_value()?);
                        }
                    }
                }
                Ok(NestedLoopJoinNode {
                    join_type: join_type.unwrap_or_default(),
                    join_cond,
                })
            }
        }
        deserializer.deserialize_struct("plan.NestedLoopJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OrderByNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.column_orders.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.OrderByNode", len)?;
        if !self.column_orders.is_empty() {
            struct_ser.serialize_field("columnOrders", &self.column_orders)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OrderByNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columnOrders",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnOrders,
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
                            "columnOrders" => Ok(GeneratedField::ColumnOrders),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OrderByNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.OrderByNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OrderByNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_orders = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnOrders => {
                            if column_orders.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnOrders"));
                            }
                            column_orders = Some(map.next_value()?);
                        }
                    }
                }
                Ok(OrderByNode {
                    column_orders: column_orders.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.OrderByNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OrderType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Ascending => "ASCENDING",
            Self::Descending => "DESCENDING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for OrderType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "ASCENDING",
            "DESCENDING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OrderType;

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
                    .and_then(OrderType::from_i32)
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
                    .and_then(OrderType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(OrderType::Invalid),
                    "ASCENDING" => Ok(OrderType::Ascending),
                    "DESCENDING" => Ok(OrderType::Descending),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for OrderedColumnDesc {
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
        if self.order != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.OrderedColumnDesc", len)?;
        if let Some(v) = self.column_desc.as_ref() {
            struct_ser.serialize_field("columnDesc", v)?;
        }
        if self.order != 0 {
            let v = OrderType::from_i32(self.order)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.order)))?;
            struct_ser.serialize_field("order", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OrderedColumnDesc {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columnDesc",
            "order",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnDesc,
            Order,
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
                            "columnDesc" => Ok(GeneratedField::ColumnDesc),
                            "order" => Ok(GeneratedField::Order),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OrderedColumnDesc;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.OrderedColumnDesc")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<OrderedColumnDesc, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_desc = None;
                let mut order = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnDesc => {
                            if column_desc.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDesc"));
                            }
                            column_desc = Some(map.next_value()?);
                        }
                        GeneratedField::Order => {
                            if order.is_some() {
                                return Err(serde::de::Error::duplicate_field("order"));
                            }
                            order = Some(map.next_value::<OrderType>()? as i32);
                        }
                    }
                }
                Ok(OrderedColumnDesc {
                    column_desc,
                    order: order.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.OrderedColumnDesc", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlanFragment {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.root.is_some() {
            len += 1;
        }
        if self.exchange_info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.PlanFragment", len)?;
        if let Some(v) = self.root.as_ref() {
            struct_ser.serialize_field("root", v)?;
        }
        if let Some(v) = self.exchange_info.as_ref() {
            struct_ser.serialize_field("exchangeInfo", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlanFragment {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "root",
            "exchangeInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Root,
            ExchangeInfo,
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
                            "root" => Ok(GeneratedField::Root),
                            "exchangeInfo" => Ok(GeneratedField::ExchangeInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlanFragment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.PlanFragment")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PlanFragment, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut root = None;
                let mut exchange_info = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Root => {
                            if root.is_some() {
                                return Err(serde::de::Error::duplicate_field("root"));
                            }
                            root = Some(map.next_value()?);
                        }
                        GeneratedField::ExchangeInfo => {
                            if exchange_info.is_some() {
                                return Err(serde::de::Error::duplicate_field("exchangeInfo"));
                            }
                            exchange_info = Some(map.next_value()?);
                        }
                    }
                }
                Ok(PlanFragment {
                    root,
                    exchange_info,
                })
            }
        }
        deserializer.deserialize_struct("plan.PlanFragment", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.children.is_empty() {
            len += 1;
        }
        if !self.identity.is_empty() {
            len += 1;
        }
        if self.node_body.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.PlanNode", len)?;
        if !self.children.is_empty() {
            struct_ser.serialize_field("children", &self.children)?;
        }
        if !self.identity.is_empty() {
            struct_ser.serialize_field("identity", &self.identity)?;
        }
        if let Some(v) = self.node_body.as_ref() {
            match v {
                plan_node::NodeBody::Insert(v) => {
                    struct_ser.serialize_field("insert", v)?;
                }
                plan_node::NodeBody::Delete(v) => {
                    struct_ser.serialize_field("delete", v)?;
                }
                plan_node::NodeBody::Project(v) => {
                    struct_ser.serialize_field("project", v)?;
                }
                plan_node::NodeBody::CreateTable(v) => {
                    struct_ser.serialize_field("createTable", v)?;
                }
                plan_node::NodeBody::DropTable(v) => {
                    struct_ser.serialize_field("dropTable", v)?;
                }
                plan_node::NodeBody::HashAgg(v) => {
                    struct_ser.serialize_field("hashAgg", v)?;
                }
                plan_node::NodeBody::Filter(v) => {
                    struct_ser.serialize_field("filter", v)?;
                }
                plan_node::NodeBody::Exchange(v) => {
                    struct_ser.serialize_field("exchange", v)?;
                }
                plan_node::NodeBody::OrderBy(v) => {
                    struct_ser.serialize_field("orderBy", v)?;
                }
                plan_node::NodeBody::NestedLoopJoin(v) => {
                    struct_ser.serialize_field("nestedLoopJoin", v)?;
                }
                plan_node::NodeBody::CreateSource(v) => {
                    struct_ser.serialize_field("createSource", v)?;
                }
                plan_node::NodeBody::SourceScan(v) => {
                    struct_ser.serialize_field("sourceScan", v)?;
                }
                plan_node::NodeBody::TopN(v) => {
                    struct_ser.serialize_field("topN", v)?;
                }
                plan_node::NodeBody::SortAgg(v) => {
                    struct_ser.serialize_field("sortAgg", v)?;
                }
                plan_node::NodeBody::RowSeqScan(v) => {
                    struct_ser.serialize_field("rowSeqScan", v)?;
                }
                plan_node::NodeBody::Limit(v) => {
                    struct_ser.serialize_field("limit", v)?;
                }
                plan_node::NodeBody::Values(v) => {
                    struct_ser.serialize_field("values", v)?;
                }
                plan_node::NodeBody::HashJoin(v) => {
                    struct_ser.serialize_field("hashJoin", v)?;
                }
                plan_node::NodeBody::DropSource(v) => {
                    struct_ser.serialize_field("dropSource", v)?;
                }
                plan_node::NodeBody::MergeSortExchange(v) => {
                    struct_ser.serialize_field("mergeSortExchange", v)?;
                }
                plan_node::NodeBody::SortMergeJoin(v) => {
                    struct_ser.serialize_field("sortMergeJoin", v)?;
                }
                plan_node::NodeBody::GenerateInt32Series(v) => {
                    struct_ser.serialize_field("generateInt32Series", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "children",
            "identity",
            "insert",
            "delete",
            "project",
            "createTable",
            "dropTable",
            "hashAgg",
            "filter",
            "exchange",
            "orderBy",
            "nestedLoopJoin",
            "createSource",
            "sourceScan",
            "topN",
            "sortAgg",
            "rowSeqScan",
            "limit",
            "values",
            "hashJoin",
            "dropSource",
            "mergeSortExchange",
            "sortMergeJoin",
            "generateInt32Series",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Children,
            Identity,
            Insert,
            Delete,
            Project,
            CreateTable,
            DropTable,
            HashAgg,
            Filter,
            Exchange,
            OrderBy,
            NestedLoopJoin,
            CreateSource,
            SourceScan,
            TopN,
            SortAgg,
            RowSeqScan,
            Limit,
            Values,
            HashJoin,
            DropSource,
            MergeSortExchange,
            SortMergeJoin,
            GenerateInt32Series,
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
                            "children" => Ok(GeneratedField::Children),
                            "identity" => Ok(GeneratedField::Identity),
                            "insert" => Ok(GeneratedField::Insert),
                            "delete" => Ok(GeneratedField::Delete),
                            "project" => Ok(GeneratedField::Project),
                            "createTable" => Ok(GeneratedField::CreateTable),
                            "dropTable" => Ok(GeneratedField::DropTable),
                            "hashAgg" => Ok(GeneratedField::HashAgg),
                            "filter" => Ok(GeneratedField::Filter),
                            "exchange" => Ok(GeneratedField::Exchange),
                            "orderBy" => Ok(GeneratedField::OrderBy),
                            "nestedLoopJoin" => Ok(GeneratedField::NestedLoopJoin),
                            "createSource" => Ok(GeneratedField::CreateSource),
                            "sourceScan" => Ok(GeneratedField::SourceScan),
                            "topN" => Ok(GeneratedField::TopN),
                            "sortAgg" => Ok(GeneratedField::SortAgg),
                            "rowSeqScan" => Ok(GeneratedField::RowSeqScan),
                            "limit" => Ok(GeneratedField::Limit),
                            "values" => Ok(GeneratedField::Values),
                            "hashJoin" => Ok(GeneratedField::HashJoin),
                            "dropSource" => Ok(GeneratedField::DropSource),
                            "mergeSortExchange" => Ok(GeneratedField::MergeSortExchange),
                            "sortMergeJoin" => Ok(GeneratedField::SortMergeJoin),
                            "generateInt32Series" => Ok(GeneratedField::GenerateInt32Series),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.PlanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<PlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children = None;
                let mut identity = None;
                let mut node_body = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Children => {
                            if children.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children = Some(map.next_value()?);
                        }
                        GeneratedField::Identity => {
                            if identity.is_some() {
                                return Err(serde::de::Error::duplicate_field("identity"));
                            }
                            identity = Some(map.next_value()?);
                        }
                        GeneratedField::Insert => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("insert"));
                            }
                            node_body = Some(plan_node::NodeBody::Insert(map.next_value()?));
                        }
                        GeneratedField::Delete => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("delete"));
                            }
                            node_body = Some(plan_node::NodeBody::Delete(map.next_value()?));
                        }
                        GeneratedField::Project => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("project"));
                            }
                            node_body = Some(plan_node::NodeBody::Project(map.next_value()?));
                        }
                        GeneratedField::CreateTable => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("createTable"));
                            }
                            node_body = Some(plan_node::NodeBody::CreateTable(map.next_value()?));
                        }
                        GeneratedField::DropTable => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("dropTable"));
                            }
                            node_body = Some(plan_node::NodeBody::DropTable(map.next_value()?));
                        }
                        GeneratedField::HashAgg => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashAgg"));
                            }
                            node_body = Some(plan_node::NodeBody::HashAgg(map.next_value()?));
                        }
                        GeneratedField::Filter => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            node_body = Some(plan_node::NodeBody::Filter(map.next_value()?));
                        }
                        GeneratedField::Exchange => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("exchange"));
                            }
                            node_body = Some(plan_node::NodeBody::Exchange(map.next_value()?));
                        }
                        GeneratedField::OrderBy => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            node_body = Some(plan_node::NodeBody::OrderBy(map.next_value()?));
                        }
                        GeneratedField::NestedLoopJoin => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("nestedLoopJoin"));
                            }
                            node_body = Some(plan_node::NodeBody::NestedLoopJoin(map.next_value()?));
                        }
                        GeneratedField::CreateSource => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("createSource"));
                            }
                            node_body = Some(plan_node::NodeBody::CreateSource(map.next_value()?));
                        }
                        GeneratedField::SourceScan => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("sourceScan"));
                            }
                            node_body = Some(plan_node::NodeBody::SourceScan(map.next_value()?));
                        }
                        GeneratedField::TopN => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("topN"));
                            }
                            node_body = Some(plan_node::NodeBody::TopN(map.next_value()?));
                        }
                        GeneratedField::SortAgg => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortAgg"));
                            }
                            node_body = Some(plan_node::NodeBody::SortAgg(map.next_value()?));
                        }
                        GeneratedField::RowSeqScan => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("rowSeqScan"));
                            }
                            node_body = Some(plan_node::NodeBody::RowSeqScan(map.next_value()?));
                        }
                        GeneratedField::Limit => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            node_body = Some(plan_node::NodeBody::Limit(map.next_value()?));
                        }
                        GeneratedField::Values => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            node_body = Some(plan_node::NodeBody::Values(map.next_value()?));
                        }
                        GeneratedField::HashJoin => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashJoin"));
                            }
                            node_body = Some(plan_node::NodeBody::HashJoin(map.next_value()?));
                        }
                        GeneratedField::DropSource => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("dropSource"));
                            }
                            node_body = Some(plan_node::NodeBody::DropSource(map.next_value()?));
                        }
                        GeneratedField::MergeSortExchange => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("mergeSortExchange"));
                            }
                            node_body = Some(plan_node::NodeBody::MergeSortExchange(map.next_value()?));
                        }
                        GeneratedField::SortMergeJoin => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortMergeJoin"));
                            }
                            node_body = Some(plan_node::NodeBody::SortMergeJoin(map.next_value()?));
                        }
                        GeneratedField::GenerateInt32Series => {
                            if node_body.is_some() {
                                return Err(serde::de::Error::duplicate_field("generateInt32Series"));
                            }
                            node_body = Some(plan_node::NodeBody::GenerateInt32Series(map.next_value()?));
                        }
                    }
                }
                Ok(PlanNode {
                    children: children.unwrap_or_default(),
                    identity: identity.unwrap_or_default(),
                    node_body,
                })
            }
        }
        deserializer.deserialize_struct("plan.PlanNode", FIELDS, GeneratedVisitor)
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
        let mut struct_ser = serializer.serialize_struct("plan.ProjectNode", len)?;
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
                formatter.write_str("struct plan.ProjectNode")
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
        deserializer.deserialize_struct("plan.ProjectNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RowFormatType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
            Self::DebeziumJson => "DEBEZIUM_JSON",
            Self::Avro => "AVRO",
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
            "JSON",
            "PROTOBUF",
            "DEBEZIUM_JSON",
            "AVRO",
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
                use std::convert::TryFrom;
                i32::try_from(v)
                    .ok()
                    .and_then(RowFormatType::from_i32)
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
                    .and_then(RowFormatType::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "JSON" => Ok(RowFormatType::Json),
                    "PROTOBUF" => Ok(RowFormatType::Protobuf),
                    "DEBEZIUM_JSON" => Ok(RowFormatType::DebeziumJson),
                    "AVRO" => Ok(RowFormatType::Avro),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for RowSeqScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_desc.is_some() {
            len += 1;
        }
        if !self.column_descs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.RowSeqScanNode", len)?;
        if let Some(v) = self.table_desc.as_ref() {
            struct_ser.serialize_field("tableDesc", v)?;
        }
        if !self.column_descs.is_empty() {
            struct_ser.serialize_field("columnDescs", &self.column_descs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RowSeqScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableDesc",
            "columnDescs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableDesc,
            ColumnDescs,
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
                            "tableDesc" => Ok(GeneratedField::TableDesc),
                            "columnDescs" => Ok(GeneratedField::ColumnDescs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RowSeqScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.RowSeqScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<RowSeqScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_desc = None;
                let mut column_descs = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableDesc => {
                            if table_desc.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableDesc"));
                            }
                            table_desc = Some(map.next_value()?);
                        }
                        GeneratedField::ColumnDescs => {
                            if column_descs.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDescs"));
                            }
                            column_descs = Some(map.next_value()?);
                        }
                    }
                }
                Ok(RowSeqScanNode {
                    table_desc,
                    column_descs: column_descs.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.RowSeqScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SchemaRefId {
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
        if self.schema_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.SchemaRefId", len)?;
        if let Some(v) = self.database_ref_id.as_ref() {
            struct_ser.serialize_field("databaseRefId", v)?;
        }
        if self.schema_id != 0 {
            struct_ser.serialize_field("schemaId", &self.schema_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SchemaRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "databaseRefId",
            "schemaId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            DatabaseRefId,
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
                            "databaseRefId" => Ok(GeneratedField::DatabaseRefId),
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
            type Value = SchemaRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.SchemaRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SchemaRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut database_ref_id = None;
                let mut schema_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::DatabaseRefId => {
                            if database_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("databaseRefId"));
                            }
                            database_ref_id = Some(map.next_value()?);
                        }
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
                Ok(SchemaRefId {
                    database_ref_id,
                    schema_id: schema_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.SchemaRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortAggNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.group_keys.is_empty() {
            len += 1;
        }
        if !self.agg_calls.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.SortAggNode", len)?;
        if !self.group_keys.is_empty() {
            struct_ser.serialize_field("groupKeys", &self.group_keys)?;
        }
        if !self.agg_calls.is_empty() {
            struct_ser.serialize_field("aggCalls", &self.agg_calls)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortAggNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "groupKeys",
            "aggCalls",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            GroupKeys,
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
                            "groupKeys" => Ok(GeneratedField::GroupKeys),
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
            type Value = SortAggNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.SortAggNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortAggNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut group_keys = None;
                let mut agg_calls = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::GroupKeys => {
                            if group_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupKeys"));
                            }
                            group_keys = Some(map.next_value()?);
                        }
                        GeneratedField::AggCalls => {
                            if agg_calls.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggCalls"));
                            }
                            agg_calls = Some(map.next_value()?);
                        }
                    }
                }
                Ok(SortAggNode {
                    group_keys: group_keys.unwrap_or_default(),
                    agg_calls: agg_calls.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.SortAggNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortMergeJoinNode {
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
        if !self.left_keys.is_empty() {
            len += 1;
        }
        if !self.right_keys.is_empty() {
            len += 1;
        }
        if self.direction != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.SortMergeJoinNode", len)?;
        if self.join_type != 0 {
            let v = JoinType::from_i32(self.join_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if !self.left_keys.is_empty() {
            struct_ser.serialize_field("leftKeys", &self.left_keys)?;
        }
        if !self.right_keys.is_empty() {
            struct_ser.serialize_field("rightKeys", &self.right_keys)?;
        }
        if self.direction != 0 {
            let v = OrderType::from_i32(self.direction)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.direction)))?;
            struct_ser.serialize_field("direction", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortMergeJoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "joinType",
            "leftKeys",
            "rightKeys",
            "direction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            JoinType,
            LeftKeys,
            RightKeys,
            Direction,
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
                            "leftKeys" => Ok(GeneratedField::LeftKeys),
                            "rightKeys" => Ok(GeneratedField::RightKeys),
                            "direction" => Ok(GeneratedField::Direction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortMergeJoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.SortMergeJoinNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SortMergeJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut join_type = None;
                let mut left_keys = None;
                let mut right_keys = None;
                let mut direction = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::JoinType => {
                            if join_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type = Some(map.next_value::<JoinType>()? as i32);
                        }
                        GeneratedField::LeftKeys => {
                            if left_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftKeys"));
                            }
                            left_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::RightKeys => {
                            if right_keys.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightKeys"));
                            }
                            right_keys = Some(
                                map.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect()
                            );
                        }
                        GeneratedField::Direction => {
                            if direction.is_some() {
                                return Err(serde::de::Error::duplicate_field("direction"));
                            }
                            direction = Some(map.next_value::<OrderType>()? as i32);
                        }
                    }
                }
                Ok(SortMergeJoinNode {
                    join_type: join_type.unwrap_or_default(),
                    left_keys: left_keys.unwrap_or_default(),
                    right_keys: right_keys.unwrap_or_default(),
                    direction: direction.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.SortMergeJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SourceScanNode {
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
        if self.timestamp_ms != 0 {
            len += 1;
        }
        if !self.column_ids.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.SourceScanNode", len)?;
        if let Some(v) = self.table_ref_id.as_ref() {
            struct_ser.serialize_field("tableRefId", v)?;
        }
        if self.timestamp_ms != 0 {
            struct_ser.serialize_field("timestampMs", ToString::to_string(&self.timestamp_ms).as_str())?;
        }
        if !self.column_ids.is_empty() {
            struct_ser.serialize_field("columnIds", &self.column_ids)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SourceScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tableRefId",
            "timestampMs",
            "columnIds",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableRefId,
            TimestampMs,
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
                            "timestampMs" => Ok(GeneratedField::TimestampMs),
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
            type Value = SourceScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.SourceScanNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<SourceScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_ref_id = None;
                let mut timestamp_ms = None;
                let mut column_ids = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TableRefId => {
                            if table_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableRefId"));
                            }
                            table_ref_id = Some(map.next_value()?);
                        }
                        GeneratedField::TimestampMs => {
                            if timestamp_ms.is_some() {
                                return Err(serde::de::Error::duplicate_field("timestampMs"));
                            }
                            timestamp_ms = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
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
                Ok(SourceScanNode {
                    table_ref_id,
                    timestamp_ms: timestamp_ms.unwrap_or_default(),
                    column_ids: column_ids.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.SourceScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StreamSourceInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.append_only {
            len += 1;
        }
        if !self.properties.is_empty() {
            len += 1;
        }
        if self.row_format != 0 {
            len += 1;
        }
        if !self.row_schema_location.is_empty() {
            len += 1;
        }
        if self.row_id_index != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.StreamSourceInfo", len)?;
        if self.append_only {
            struct_ser.serialize_field("appendOnly", &self.append_only)?;
        }
        if !self.properties.is_empty() {
            struct_ser.serialize_field("properties", &self.properties)?;
        }
        if self.row_format != 0 {
            let v = RowFormatType::from_i32(self.row_format)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.row_format)))?;
            struct_ser.serialize_field("rowFormat", &v)?;
        }
        if !self.row_schema_location.is_empty() {
            struct_ser.serialize_field("rowSchemaLocation", &self.row_schema_location)?;
        }
        if self.row_id_index != 0 {
            struct_ser.serialize_field("rowIdIndex", &self.row_id_index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StreamSourceInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "appendOnly",
            "properties",
            "rowFormat",
            "rowSchemaLocation",
            "rowIdIndex",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AppendOnly,
            Properties,
            RowFormat,
            RowSchemaLocation,
            RowIdIndex,
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
                            "appendOnly" => Ok(GeneratedField::AppendOnly),
                            "properties" => Ok(GeneratedField::Properties),
                            "rowFormat" => Ok(GeneratedField::RowFormat),
                            "rowSchemaLocation" => Ok(GeneratedField::RowSchemaLocation),
                            "rowIdIndex" => Ok(GeneratedField::RowIdIndex),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamSourceInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.StreamSourceInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<StreamSourceInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut append_only = None;
                let mut properties = None;
                let mut row_format = None;
                let mut row_schema_location = None;
                let mut row_id_index = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::AppendOnly => {
                            if append_only.is_some() {
                                return Err(serde::de::Error::duplicate_field("appendOnly"));
                            }
                            append_only = Some(map.next_value()?);
                        }
                        GeneratedField::Properties => {
                            if properties.is_some() {
                                return Err(serde::de::Error::duplicate_field("properties"));
                            }
                            properties = Some(
                                map.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::RowFormat => {
                            if row_format.is_some() {
                                return Err(serde::de::Error::duplicate_field("rowFormat"));
                            }
                            row_format = Some(map.next_value::<RowFormatType>()? as i32);
                        }
                        GeneratedField::RowSchemaLocation => {
                            if row_schema_location.is_some() {
                                return Err(serde::de::Error::duplicate_field("rowSchemaLocation"));
                            }
                            row_schema_location = Some(map.next_value()?);
                        }
                        GeneratedField::RowIdIndex => {
                            if row_id_index.is_some() {
                                return Err(serde::de::Error::duplicate_field("rowIdIndex"));
                            }
                            row_id_index = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(StreamSourceInfo {
                    append_only: append_only.unwrap_or_default(),
                    properties: properties.unwrap_or_default(),
                    row_format: row_format.unwrap_or_default(),
                    row_schema_location: row_schema_location.unwrap_or_default(),
                    row_id_index: row_id_index.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.StreamSourceInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableRefId {
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
        if self.table_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.TableRefId", len)?;
        if let Some(v) = self.schema_ref_id.as_ref() {
            struct_ser.serialize_field("schemaRefId", v)?;
        }
        if self.table_id != 0 {
            struct_ser.serialize_field("tableId", &self.table_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableRefId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schemaRefId",
            "tableId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaRefId,
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
                            "schemaRefId" => Ok(GeneratedField::SchemaRefId),
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
            type Value = TableRefId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.TableRefId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableRefId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_ref_id = None;
                let mut table_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::SchemaRefId => {
                            if schema_ref_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaRefId"));
                            }
                            schema_ref_id = Some(map.next_value()?);
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
                Ok(TableRefId {
                    schema_ref_id,
                    table_id: table_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.TableRefId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableSourceInfo {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("plan.TableSourceInfo", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableSourceInfo {
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
            type Value = TableSourceInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.TableSourceInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TableSourceInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map.next_key::<GeneratedField>()?.is_some() {}
                Ok(TableSourceInfo {
                })
            }
        }
        deserializer.deserialize_struct("plan.TableSourceInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskId {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.query_id.is_empty() {
            len += 1;
        }
        if self.stage_id != 0 {
            len += 1;
        }
        if self.task_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.TaskId", len)?;
        if !self.query_id.is_empty() {
            struct_ser.serialize_field("queryId", &self.query_id)?;
        }
        if self.stage_id != 0 {
            struct_ser.serialize_field("stageId", &self.stage_id)?;
        }
        if self.task_id != 0 {
            struct_ser.serialize_field("taskId", &self.task_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "queryId",
            "stageId",
            "taskId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            QueryId,
            StageId,
            TaskId,
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
                            "queryId" => Ok(GeneratedField::QueryId),
                            "stageId" => Ok(GeneratedField::StageId),
                            "taskId" => Ok(GeneratedField::TaskId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TaskId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.TaskId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TaskId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut query_id = None;
                let mut stage_id = None;
                let mut task_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::QueryId => {
                            if query_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("queryId"));
                            }
                            query_id = Some(map.next_value()?);
                        }
                        GeneratedField::StageId => {
                            if stage_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("stageId"));
                            }
                            stage_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(TaskId {
                    query_id: query_id.unwrap_or_default(),
                    stage_id: stage_id.unwrap_or_default(),
                    task_id: task_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.TaskId", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskOutputId {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.task_id.is_some() {
            len += 1;
        }
        if self.output_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.TaskOutputId", len)?;
        if let Some(v) = self.task_id.as_ref() {
            struct_ser.serialize_field("taskId", v)?;
        }
        if self.output_id != 0 {
            struct_ser.serialize_field("outputId", &self.output_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskOutputId {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskId",
            "outputId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskId,
            OutputId,
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
                            "taskId" => Ok(GeneratedField::TaskId),
                            "outputId" => Ok(GeneratedField::OutputId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TaskOutputId;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.TaskOutputId")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TaskOutputId, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_id = None;
                let mut output_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(map.next_value()?);
                        }
                        GeneratedField::OutputId => {
                            if output_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputId"));
                            }
                            output_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(TaskOutputId {
                    task_id,
                    output_id: output_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.TaskOutputId", FIELDS, GeneratedVisitor)
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
        if !self.column_orders.is_empty() {
            len += 1;
        }
        if self.limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.TopNNode", len)?;
        if !self.column_orders.is_empty() {
            struct_ser.serialize_field("columnOrders", &self.column_orders)?;
        }
        if self.limit != 0 {
            struct_ser.serialize_field("limit", &self.limit)?;
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
            "columnOrders",
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnOrders,
            Limit,
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
                            "columnOrders" => Ok(GeneratedField::ColumnOrders),
                            "limit" => Ok(GeneratedField::Limit),
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
                formatter.write_str("struct plan.TopNNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TopNNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_orders = None;
                let mut limit = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnOrders => {
                            if column_orders.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnOrders"));
                            }
                            column_orders = Some(map.next_value()?);
                        }
                        GeneratedField::Limit => {
                            if limit.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(TopNNode {
                    column_orders: column_orders.unwrap_or_default(),
                    limit: limit.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.TopNNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValuesNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.tuples.is_empty() {
            len += 1;
        }
        if !self.fields.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ValuesNode", len)?;
        if !self.tuples.is_empty() {
            struct_ser.serialize_field("tuples", &self.tuples)?;
        }
        if !self.fields.is_empty() {
            struct_ser.serialize_field("fields", &self.fields)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValuesNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "tuples",
            "fields",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Tuples,
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
                            "tuples" => Ok(GeneratedField::Tuples),
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
            type Value = ValuesNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ValuesNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ValuesNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut tuples = None;
                let mut fields = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Tuples => {
                            if tuples.is_some() {
                                return Err(serde::de::Error::duplicate_field("tuples"));
                            }
                            tuples = Some(map.next_value()?);
                        }
                        GeneratedField::Fields => {
                            if fields.is_some() {
                                return Err(serde::de::Error::duplicate_field("fields"));
                            }
                            fields = Some(map.next_value()?);
                        }
                    }
                }
                Ok(ValuesNode {
                    tuples: tuples.unwrap_or_default(),
                    fields: fields.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ValuesNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for values_node::ExprTuple {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.cells.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("plan.ValuesNode.ExprTuple", len)?;
        if !self.cells.is_empty() {
            struct_ser.serialize_field("cells", &self.cells)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for values_node::ExprTuple {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "cells",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Cells,
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
                            "cells" => Ok(GeneratedField::Cells),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = values_node::ExprTuple;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct plan.ValuesNode.ExprTuple")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<values_node::ExprTuple, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut cells = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Cells => {
                            if cells.is_some() {
                                return Err(serde::de::Error::duplicate_field("cells"));
                            }
                            cells = Some(map.next_value()?);
                        }
                    }
                }
                Ok(values_node::ExprTuple {
                    cells: cells.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("plan.ValuesNode.ExprTuple", FIELDS, GeneratedVisitor)
    }
}
