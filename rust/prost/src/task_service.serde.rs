use crate::task_service::*;
impl serde::Serialize for AbortTaskRequest {
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
        if self.force {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.AbortTaskRequest", len)?;
        if let Some(v) = self.task_id.as_ref() {
            struct_ser.serialize_field("taskId", v)?;
        }
        if self.force {
            struct_ser.serialize_field("force", &self.force)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AbortTaskRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskId",
            "force",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskId,
            Force,
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
                            "force" => Ok(GeneratedField::Force),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AbortTaskRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.AbortTaskRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AbortTaskRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_id = None;
                let mut force = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(map.next_value()?);
                        }
                        GeneratedField::Force => {
                            if force.is_some() {
                                return Err(serde::de::Error::duplicate_field("force"));
                            }
                            force = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AbortTaskRequest {
                    task_id,
                    force: force.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("task_service.AbortTaskRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AbortTaskResponse {
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
        let mut struct_ser = serializer.serialize_struct("task_service.AbortTaskResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AbortTaskResponse {
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
            type Value = AbortTaskResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.AbortTaskResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AbortTaskResponse, V::Error>
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
                Ok(AbortTaskResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("task_service.AbortTaskResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTaskRequest {
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
        if self.plan.is_some() {
            len += 1;
        }
        if self.epoch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.CreateTaskRequest", len)?;
        if let Some(v) = self.task_id.as_ref() {
            struct_ser.serialize_field("taskId", v)?;
        }
        if let Some(v) = self.plan.as_ref() {
            struct_ser.serialize_field("plan", v)?;
        }
        if self.epoch != 0 {
            struct_ser.serialize_field("epoch", ToString::to_string(&self.epoch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTaskRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskId",
            "plan",
            "epoch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskId,
            Plan,
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
                            "taskId" => Ok(GeneratedField::TaskId),
                            "plan" => Ok(GeneratedField::Plan),
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
            type Value = CreateTaskRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.CreateTaskRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateTaskRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_id = None;
                let mut plan = None;
                let mut epoch = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(map.next_value()?);
                        }
                        GeneratedField::Plan => {
                            if plan.is_some() {
                                return Err(serde::de::Error::duplicate_field("plan"));
                            }
                            plan = Some(map.next_value()?);
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
                Ok(CreateTaskRequest {
                    task_id,
                    plan,
                    epoch: epoch.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("task_service.CreateTaskRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateTaskResponse {
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
        let mut struct_ser = serializer.serialize_struct("task_service.CreateTaskResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateTaskResponse {
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
            type Value = CreateTaskResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.CreateTaskResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<CreateTaskResponse, V::Error>
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
                Ok(CreateTaskResponse {
                    status,
                })
            }
        }
        deserializer.deserialize_struct("task_service.CreateTaskResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetDataRequest {
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
        let mut struct_ser = serializer.serialize_struct("task_service.GetDataRequest", len)?;
        if let Some(v) = self.task_output_id.as_ref() {
            struct_ser.serialize_field("taskOutputId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetDataRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskOutputId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskOutputId,
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
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetDataRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetDataRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetDataRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_output_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskOutputId => {
                            if task_output_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskOutputId"));
                            }
                            task_output_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetDataRequest {
                    task_output_id,
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetDataRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetDataResponse {
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
        if self.record_batch.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.GetDataResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.record_batch.as_ref() {
            struct_ser.serialize_field("recordBatch", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetDataResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "recordBatch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            RecordBatch,
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
                            "recordBatch" => Ok(GeneratedField::RecordBatch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetDataResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetDataResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetDataResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut record_batch = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::RecordBatch => {
                            if record_batch.is_some() {
                                return Err(serde::de::Error::duplicate_field("recordBatch"));
                            }
                            record_batch = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetDataResponse {
                    status,
                    record_batch,
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetDataResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetStreamRequest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.up_fragment_id != 0 {
            len += 1;
        }
        if self.down_fragment_id != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.GetStreamRequest", len)?;
        if self.up_fragment_id != 0 {
            struct_ser.serialize_field("upFragmentId", &self.up_fragment_id)?;
        }
        if self.down_fragment_id != 0 {
            struct_ser.serialize_field("downFragmentId", &self.down_fragment_id)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetStreamRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "upFragmentId",
            "downFragmentId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            UpFragmentId,
            DownFragmentId,
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
                            "upFragmentId" => Ok(GeneratedField::UpFragmentId),
                            "downFragmentId" => Ok(GeneratedField::DownFragmentId),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetStreamRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetStreamRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetStreamRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut up_fragment_id = None;
                let mut down_fragment_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::UpFragmentId => {
                            if up_fragment_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("upFragmentId"));
                            }
                            up_fragment_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                        GeneratedField::DownFragmentId => {
                            if down_fragment_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("downFragmentId"));
                            }
                            down_fragment_id = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(GetStreamRequest {
                    up_fragment_id: up_fragment_id.unwrap_or_default(),
                    down_fragment_id: down_fragment_id.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetStreamRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetStreamResponse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.message.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.GetStreamResponse", len)?;
        if let Some(v) = self.message.as_ref() {
            struct_ser.serialize_field("message", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetStreamResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "message",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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

                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
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
            type Value = GetStreamResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetStreamResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetStreamResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut message = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Message => {
                            if message.is_some() {
                                return Err(serde::de::Error::duplicate_field("message"));
                            }
                            message = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetStreamResponse {
                    message,
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetStreamResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetTaskInfoRequest {
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
        let mut struct_ser = serializer.serialize_struct("task_service.GetTaskInfoRequest", len)?;
        if let Some(v) = self.task_id.as_ref() {
            struct_ser.serialize_field("taskId", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetTaskInfoRequest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskId",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
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
            type Value = GetTaskInfoRequest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetTaskInfoRequest")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetTaskInfoRequest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_id = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetTaskInfoRequest {
                    task_id,
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetTaskInfoRequest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GetTaskInfoResponse {
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
        if self.task_info.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.GetTaskInfoResponse", len)?;
        if let Some(v) = self.status.as_ref() {
            struct_ser.serialize_field("status", v)?;
        }
        if let Some(v) = self.task_info.as_ref() {
            struct_ser.serialize_field("taskInfo", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GetTaskInfoResponse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "status",
            "taskInfo",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Status,
            TaskInfo,
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
                            "taskInfo" => Ok(GeneratedField::TaskInfo),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GetTaskInfoResponse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.GetTaskInfoResponse")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GetTaskInfoResponse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut status = None;
                let mut task_info = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Status => {
                            if status.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status = Some(map.next_value()?);
                        }
                        GeneratedField::TaskInfo => {
                            if task_info.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskInfo"));
                            }
                            task_info = Some(map.next_value()?);
                        }
                    }
                }
                Ok(GetTaskInfoResponse {
                    status,
                    task_info,
                })
            }
        }
        deserializer.deserialize_struct("task_service.GetTaskInfoResponse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TaskInfo {
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
        if self.task_status != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("task_service.TaskInfo", len)?;
        if let Some(v) = self.task_id.as_ref() {
            struct_ser.serialize_field("taskId", v)?;
        }
        if self.task_status != 0 {
            let v = task_info::TaskStatus::from_i32(self.task_status)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.task_status)))?;
            struct_ser.serialize_field("taskStatus", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TaskInfo {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "taskId",
            "taskStatus",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TaskId,
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
                            "taskId" => Ok(GeneratedField::TaskId),
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
            type Value = TaskInfo;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct task_service.TaskInfo")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<TaskInfo, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut task_id = None;
                let mut task_status = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::TaskId => {
                            if task_id.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskId"));
                            }
                            task_id = Some(map.next_value()?);
                        }
                        GeneratedField::TaskStatus => {
                            if task_status.is_some() {
                                return Err(serde::de::Error::duplicate_field("taskStatus"));
                            }
                            task_status = Some(map.next_value::<task_info::TaskStatus>()? as i32);
                        }
                    }
                }
                Ok(TaskInfo {
                    task_id,
                    task_status: task_status.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("task_service.TaskInfo", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for task_info::TaskStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::NotFound => "NOT_FOUND",
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Failing => "FAILING",
            Self::Cancelling => "CANCELLING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for task_info::TaskStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "NOT_FOUND",
            "PENDING",
            "RUNNING",
            "FAILING",
            "CANCELLING",
            "FINISHED",
            "FAILED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = task_info::TaskStatus;

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
                    .and_then(task_info::TaskStatus::from_i32)
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
                    .and_then(task_info::TaskStatus::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "NOT_FOUND" => Ok(task_info::TaskStatus::NotFound),
                    "PENDING" => Ok(task_info::TaskStatus::Pending),
                    "RUNNING" => Ok(task_info::TaskStatus::Running),
                    "FAILING" => Ok(task_info::TaskStatus::Failing),
                    "CANCELLING" => Ok(task_info::TaskStatus::Cancelling),
                    "FINISHED" => Ok(task_info::TaskStatus::Finished),
                    "FAILED" => Ok(task_info::TaskStatus::Failed),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
