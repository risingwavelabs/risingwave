use crate::expr::*;
impl serde::Serialize for AggCall {
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
        if !self.args.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.AggCall", len)?;
        if self.r#type != 0 {
            let v = agg_call::Type::from_i32(self.r#type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggCall {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "args",
            "returnType",
            "distinct",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Args,
            ReturnType,
            Distinct,
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
                            "args" => Ok(GeneratedField::Args),
                            "returnType" => Ok(GeneratedField::ReturnType),
                            "distinct" => Ok(GeneratedField::Distinct),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggCall;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.AggCall")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<AggCall, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type = None;
                let mut args = None;
                let mut return_type = None;
                let mut distinct = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value::<agg_call::Type>()? as i32);
                        }
                        GeneratedField::Args => {
                            if args.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args = Some(map.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type = Some(map.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct = Some(map.next_value()?);
                        }
                    }
                }
                Ok(AggCall {
                    r#type: r#type.unwrap_or_default(),
                    args: args.unwrap_or_default(),
                    return_type,
                    distinct: distinct.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.AggCall", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for agg_call::Arg {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.r#type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.AggCall.Arg", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.r#type.as_ref() {
            struct_ser.serialize_field("type", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for agg_call::Arg {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "type",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Type,
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
                            "input" => Ok(GeneratedField::Input),
                            "type" => Ok(GeneratedField::Type),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = agg_call::Arg;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.AggCall.Arg")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<agg_call::Arg, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input = None;
                let mut r#type = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input = Some(map.next_value()?);
                        }
                        GeneratedField::Type => {
                            if r#type.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type = Some(map.next_value()?);
                        }
                    }
                }
                Ok(agg_call::Arg {
                    input,
                    r#type,
                })
            }
        }
        deserializer.deserialize_struct("expr.AggCall.Arg", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for agg_call::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Count => "COUNT",
            Self::Avg => "AVG",
            Self::StringAgg => "STRING_AGG",
            Self::SingleValue => "SINGLE_VALUE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for agg_call::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "SUM",
            "MIN",
            "MAX",
            "COUNT",
            "AVG",
            "STRING_AGG",
            "SINGLE_VALUE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = agg_call::Type;

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
                    .and_then(agg_call::Type::from_i32)
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
                    .and_then(agg_call::Type::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(agg_call::Type::Invalid),
                    "SUM" => Ok(agg_call::Type::Sum),
                    "MIN" => Ok(agg_call::Type::Min),
                    "MAX" => Ok(agg_call::Type::Max),
                    "COUNT" => Ok(agg_call::Type::Count),
                    "AVG" => Ok(agg_call::Type::Avg),
                    "STRING_AGG" => Ok(agg_call::Type::StringAgg),
                    "SINGLE_VALUE" => Ok(agg_call::Type::SingleValue),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for ConstantValue {
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
        let mut struct_ser = serializer.serialize_struct("expr.ConstantValue", len)?;
        if !self.body.is_empty() {
            struct_ser.serialize_field("body", pbjson::private::base64::encode(&self.body).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ConstantValue {
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
            type Value = ConstantValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.ConstantValue")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ConstantValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut body = None;
                while let Some(k) = map.next_key()? {
                    match k {
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
                Ok(ConstantValue {
                    body: body.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.ConstantValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr_type != 0 {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if self.rex_node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.ExprNode", len)?;
        if self.expr_type != 0 {
            let v = expr_node::Type::from_i32(self.expr_type)
                .ok_or_else(|| serde::ser::Error::custom(format!("Invalid variant {}", self.expr_type)))?;
            struct_ser.serialize_field("exprType", &v)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        if let Some(v) = self.rex_node.as_ref() {
            match v {
                expr_node::RexNode::InputRef(v) => {
                    struct_ser.serialize_field("inputRef", v)?;
                }
                expr_node::RexNode::Constant(v) => {
                    struct_ser.serialize_field("constant", v)?;
                }
                expr_node::RexNode::FuncCall(v) => {
                    struct_ser.serialize_field("funcCall", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exprType",
            "returnType",
            "inputRef",
            "constant",
            "funcCall",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ExprType,
            ReturnType,
            InputRef,
            Constant,
            FuncCall,
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
                            "exprType" => Ok(GeneratedField::ExprType),
                            "returnType" => Ok(GeneratedField::ReturnType),
                            "inputRef" => Ok(GeneratedField::InputRef),
                            "constant" => Ok(GeneratedField::Constant),
                            "funcCall" => Ok(GeneratedField::FuncCall),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.ExprNode")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr_type = None;
                let mut return_type = None;
                let mut rex_node = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ExprType => {
                            if expr_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("exprType"));
                            }
                            expr_type = Some(map.next_value::<expr_node::Type>()? as i32);
                        }
                        GeneratedField::ReturnType => {
                            if return_type.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type = Some(map.next_value()?);
                        }
                        GeneratedField::InputRef => {
                            if rex_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputRef"));
                            }
                            rex_node = Some(expr_node::RexNode::InputRef(map.next_value()?));
                        }
                        GeneratedField::Constant => {
                            if rex_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("constant"));
                            }
                            rex_node = Some(expr_node::RexNode::Constant(map.next_value()?));
                        }
                        GeneratedField::FuncCall => {
                            if rex_node.is_some() {
                                return Err(serde::de::Error::duplicate_field("funcCall"));
                            }
                            rex_node = Some(expr_node::RexNode::FuncCall(map.next_value()?));
                        }
                    }
                }
                Ok(ExprNode {
                    expr_type: expr_type.unwrap_or_default(),
                    return_type,
                    rex_node,
                })
            }
        }
        deserializer.deserialize_struct("expr.ExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for expr_node::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Invalid => "INVALID",
            Self::InputRef => "INPUT_REF",
            Self::ConstantValue => "CONSTANT_VALUE",
            Self::Add => "ADD",
            Self::Subtract => "SUBTRACT",
            Self::Multiply => "MULTIPLY",
            Self::Divide => "DIVIDE",
            Self::Modulus => "MODULUS",
            Self::Equal => "EQUAL",
            Self::NotEqual => "NOT_EQUAL",
            Self::LessThan => "LESS_THAN",
            Self::LessThanOrEqual => "LESS_THAN_OR_EQUAL",
            Self::GreaterThan => "GREATER_THAN",
            Self::GreaterThanOrEqual => "GREATER_THAN_OR_EQUAL",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Not => "NOT",
            Self::In => "IN",
            Self::Extract => "EXTRACT",
            Self::PgSleep => "PG_SLEEP",
            Self::TumbleStart => "TUMBLE_START",
            Self::Cast => "CAST",
            Self::Substr => "SUBSTR",
            Self::Length => "LENGTH",
            Self::Like => "LIKE",
            Self::Upper => "UPPER",
            Self::Lower => "LOWER",
            Self::Trim => "TRIM",
            Self::Replace => "REPLACE",
            Self::Position => "POSITION",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Case => "CASE",
            Self::RoundDigit => "ROUND_DIGIT",
            Self::Round => "ROUND",
            Self::Ascii => "ASCII",
            Self::Translate => "TRANSLATE",
            Self::IsTrue => "IS_TRUE",
            Self::IsNotTrue => "IS_NOT_TRUE",
            Self::IsFalse => "IS_FALSE",
            Self::IsNotFalse => "IS_NOT_FALSE",
            Self::IsNull => "IS_NULL",
            Self::IsNotNull => "IS_NOT_NULL",
            Self::Neg => "NEG",
            Self::Search => "SEARCH",
            Self::Sarg => "SARG",
            Self::StreamNullByRowCount => "STREAM_NULL_BY_ROW_COUNT",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for expr_node::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "INVALID",
            "INPUT_REF",
            "CONSTANT_VALUE",
            "ADD",
            "SUBTRACT",
            "MULTIPLY",
            "DIVIDE",
            "MODULUS",
            "EQUAL",
            "NOT_EQUAL",
            "LESS_THAN",
            "LESS_THAN_OR_EQUAL",
            "GREATER_THAN",
            "GREATER_THAN_OR_EQUAL",
            "AND",
            "OR",
            "NOT",
            "IN",
            "EXTRACT",
            "PG_SLEEP",
            "TUMBLE_START",
            "CAST",
            "SUBSTR",
            "LENGTH",
            "LIKE",
            "UPPER",
            "LOWER",
            "TRIM",
            "REPLACE",
            "POSITION",
            "LTRIM",
            "RTRIM",
            "CASE",
            "ROUND_DIGIT",
            "ROUND",
            "ASCII",
            "TRANSLATE",
            "IS_TRUE",
            "IS_NOT_TRUE",
            "IS_FALSE",
            "IS_NOT_FALSE",
            "IS_NULL",
            "IS_NOT_NULL",
            "NEG",
            "SEARCH",
            "SARG",
            "STREAM_NULL_BY_ROW_COUNT",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = expr_node::Type;

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
                    .and_then(expr_node::Type::from_i32)
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
                    .and_then(expr_node::Type::from_i32)
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "INVALID" => Ok(expr_node::Type::Invalid),
                    "INPUT_REF" => Ok(expr_node::Type::InputRef),
                    "CONSTANT_VALUE" => Ok(expr_node::Type::ConstantValue),
                    "ADD" => Ok(expr_node::Type::Add),
                    "SUBTRACT" => Ok(expr_node::Type::Subtract),
                    "MULTIPLY" => Ok(expr_node::Type::Multiply),
                    "DIVIDE" => Ok(expr_node::Type::Divide),
                    "MODULUS" => Ok(expr_node::Type::Modulus),
                    "EQUAL" => Ok(expr_node::Type::Equal),
                    "NOT_EQUAL" => Ok(expr_node::Type::NotEqual),
                    "LESS_THAN" => Ok(expr_node::Type::LessThan),
                    "LESS_THAN_OR_EQUAL" => Ok(expr_node::Type::LessThanOrEqual),
                    "GREATER_THAN" => Ok(expr_node::Type::GreaterThan),
                    "GREATER_THAN_OR_EQUAL" => Ok(expr_node::Type::GreaterThanOrEqual),
                    "AND" => Ok(expr_node::Type::And),
                    "OR" => Ok(expr_node::Type::Or),
                    "NOT" => Ok(expr_node::Type::Not),
                    "IN" => Ok(expr_node::Type::In),
                    "EXTRACT" => Ok(expr_node::Type::Extract),
                    "PG_SLEEP" => Ok(expr_node::Type::PgSleep),
                    "TUMBLE_START" => Ok(expr_node::Type::TumbleStart),
                    "CAST" => Ok(expr_node::Type::Cast),
                    "SUBSTR" => Ok(expr_node::Type::Substr),
                    "LENGTH" => Ok(expr_node::Type::Length),
                    "LIKE" => Ok(expr_node::Type::Like),
                    "UPPER" => Ok(expr_node::Type::Upper),
                    "LOWER" => Ok(expr_node::Type::Lower),
                    "TRIM" => Ok(expr_node::Type::Trim),
                    "REPLACE" => Ok(expr_node::Type::Replace),
                    "POSITION" => Ok(expr_node::Type::Position),
                    "LTRIM" => Ok(expr_node::Type::Ltrim),
                    "RTRIM" => Ok(expr_node::Type::Rtrim),
                    "CASE" => Ok(expr_node::Type::Case),
                    "ROUND_DIGIT" => Ok(expr_node::Type::RoundDigit),
                    "ROUND" => Ok(expr_node::Type::Round),
                    "ASCII" => Ok(expr_node::Type::Ascii),
                    "TRANSLATE" => Ok(expr_node::Type::Translate),
                    "IS_TRUE" => Ok(expr_node::Type::IsTrue),
                    "IS_NOT_TRUE" => Ok(expr_node::Type::IsNotTrue),
                    "IS_FALSE" => Ok(expr_node::Type::IsFalse),
                    "IS_NOT_FALSE" => Ok(expr_node::Type::IsNotFalse),
                    "IS_NULL" => Ok(expr_node::Type::IsNull),
                    "IS_NOT_NULL" => Ok(expr_node::Type::IsNotNull),
                    "NEG" => Ok(expr_node::Type::Neg),
                    "SEARCH" => Ok(expr_node::Type::Search),
                    "SARG" => Ok(expr_node::Type::Sarg),
                    "STREAM_NULL_BY_ROW_COUNT" => Ok(expr_node::Type::StreamNullByRowCount),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for FunctionCall {
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
        let mut struct_ser = serializer.serialize_struct("expr.FunctionCall", len)?;
        if !self.children.is_empty() {
            struct_ser.serialize_field("children", &self.children)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FunctionCall {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "children",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Children,
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
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FunctionCall;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.FunctionCall")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<FunctionCall, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::Children => {
                            if children.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children = Some(map.next_value()?);
                        }
                    }
                }
                Ok(FunctionCall {
                    children: children.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.FunctionCall", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InputRefExpr {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.column_idx != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.InputRefExpr", len)?;
        if self.column_idx != 0 {
            struct_ser.serialize_field("columnIdx", &self.column_idx)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InputRefExpr {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columnIdx",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ColumnIdx,
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
                            "columnIdx" => Ok(GeneratedField::ColumnIdx),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InputRefExpr;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.InputRefExpr")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<InputRefExpr, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut column_idx = None;
                while let Some(k) = map.next_key()? {
                    match k {
                        GeneratedField::ColumnIdx => {
                            if column_idx.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIdx"));
                            }
                            column_idx = Some(
                                map.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0
                            );
                        }
                    }
                }
                Ok(InputRefExpr {
                    column_idx: column_idx.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.InputRefExpr", FIELDS, GeneratedVisitor)
    }
}
