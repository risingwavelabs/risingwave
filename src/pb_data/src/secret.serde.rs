use crate::secret::*;
impl serde::Serialize for Secret {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.secret_backend.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("secret.Secret", len)?;
        if let Some(v) = self.secret_backend.as_ref() {
            match v {
                secret::SecretBackend::Meta(v) => {
                    struct_ser.serialize_field("meta", v)?;
                }
                secret::SecretBackend::HashicorpVault(v) => {
                    struct_ser.serialize_field("hashicorpVault", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Secret {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "meta",
            "hashicorp_vault",
            "hashicorpVault",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Meta,
            HashicorpVault,
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
                            "meta" => Ok(GeneratedField::Meta),
                            "hashicorpVault" | "hashicorp_vault" => Ok(GeneratedField::HashicorpVault),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Secret;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct secret.Secret")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Secret, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut secret_backend__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Meta => {
                            if secret_backend__.is_some() {
                                return Err(serde::de::Error::duplicate_field("meta"));
                            }
                            secret_backend__ = map_.next_value::<::std::option::Option<_>>()?.map(secret::SecretBackend::Meta)
;
                        }
                        GeneratedField::HashicorpVault => {
                            if secret_backend__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashicorpVault"));
                            }
                            secret_backend__ = map_.next_value::<::std::option::Option<_>>()?.map(secret::SecretBackend::HashicorpVault)
;
                        }
                    }
                }
                Ok(Secret {
                    secret_backend: secret_backend__,
                })
            }
        }
        deserializer.deserialize_struct("secret.Secret", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SecretHashicropValutBackend {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.host.is_empty() {
            len += 1;
        }
        if !self.vault_token.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("secret.SecretHashicropValutBackend", len)?;
        if !self.host.is_empty() {
            struct_ser.serialize_field("host", &self.host)?;
        }
        if !self.vault_token.is_empty() {
            struct_ser.serialize_field("vaultToken", &self.vault_token)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SecretHashicropValutBackend {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "host",
            "vault_token",
            "vaultToken",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Host,
            VaultToken,
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
                            "host" => Ok(GeneratedField::Host),
                            "vaultToken" | "vault_token" => Ok(GeneratedField::VaultToken),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SecretHashicropValutBackend;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct secret.SecretHashicropValutBackend")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SecretHashicropValutBackend, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut host__ = None;
                let mut vault_token__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Host => {
                            if host__.is_some() {
                                return Err(serde::de::Error::duplicate_field("host"));
                            }
                            host__ = Some(map_.next_value()?);
                        }
                        GeneratedField::VaultToken => {
                            if vault_token__.is_some() {
                                return Err(serde::de::Error::duplicate_field("vaultToken"));
                            }
                            vault_token__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SecretHashicropValutBackend {
                    host: host__.unwrap_or_default(),
                    vault_token: vault_token__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("secret.SecretHashicropValutBackend", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SecretMetaBackend {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.value.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("secret.SecretMetaBackend", len)?;
        if !self.value.is_empty() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("value", pbjson::private::base64::encode(&self.value).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SecretMetaBackend {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "value",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Value,
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
                            "value" => Ok(GeneratedField::Value),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SecretMetaBackend;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct secret.SecretMetaBackend")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SecretMetaBackend, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Value => {
                            if value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("value"));
                            }
                            value__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SecretMetaBackend {
                    value: value__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("secret.SecretMetaBackend", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SecretRef {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.secret_id != 0 {
            len += 1;
        }
        if self.ref_as != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("secret.SecretRef", len)?;
        if self.secret_id != 0 {
            struct_ser.serialize_field("secretId", &self.secret_id)?;
        }
        if self.ref_as != 0 {
            let v = secret_ref::RefAsType::try_from(self.ref_as)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.ref_as)))?;
            struct_ser.serialize_field("refAs", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SecretRef {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "secret_id",
            "secretId",
            "ref_as",
            "refAs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SecretId,
            RefAs,
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
                            "secretId" | "secret_id" => Ok(GeneratedField::SecretId),
                            "refAs" | "ref_as" => Ok(GeneratedField::RefAs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SecretRef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct secret.SecretRef")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SecretRef, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut secret_id__ = None;
                let mut ref_as__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SecretId => {
                            if secret_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("secretId"));
                            }
                            secret_id__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::RefAs => {
                            if ref_as__.is_some() {
                                return Err(serde::de::Error::duplicate_field("refAs"));
                            }
                            ref_as__ = Some(map_.next_value::<secret_ref::RefAsType>()? as i32);
                        }
                    }
                }
                Ok(SecretRef {
                    secret_id: secret_id__.unwrap_or_default(),
                    ref_as: ref_as__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("secret.SecretRef", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for secret_ref::RefAsType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Text => "TEXT",
            Self::File => "FILE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for secret_ref::RefAsType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "TEXT",
            "FILE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = secret_ref::RefAsType;

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
                    "UNSPECIFIED" => Ok(secret_ref::RefAsType::Unspecified),
                    "TEXT" => Ok(secret_ref::RefAsType::Text),
                    "FILE" => Ok(secret_ref::RefAsType::File),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
