use crate::expr::*;
impl serde::Serialize for AggCall {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind != 0 {
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
        if !self.order_by.is_empty() {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        if !self.direct_args.is_empty() {
            len += 1;
        }
        if self.udf.is_some() {
            len += 1;
        }
        if self.scalar.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.AggCall", len)?;
        if self.kind != 0 {
            let v = agg_call::Kind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
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
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        if !self.direct_args.is_empty() {
            struct_ser.serialize_field("directArgs", &self.direct_args)?;
        }
        if let Some(v) = self.udf.as_ref() {
            struct_ser.serialize_field("udf", v)?;
        }
        if let Some(v) = self.scalar.as_ref() {
            struct_ser.serialize_field("scalar", v)?;
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
            "kind",
            "args",
            "return_type",
            "returnType",
            "distinct",
            "order_by",
            "orderBy",
            "filter",
            "direct_args",
            "directArgs",
            "udf",
            "scalar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Kind,
            Args,
            ReturnType,
            Distinct,
            OrderBy,
            Filter,
            DirectArgs,
            Udf,
            Scalar,
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
                            "kind" => Ok(GeneratedField::Kind),
                            "args" => Ok(GeneratedField::Args),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            "filter" => Ok(GeneratedField::Filter),
                            "directArgs" | "direct_args" => Ok(GeneratedField::DirectArgs),
                            "udf" => Ok(GeneratedField::Udf),
                            "scalar" => Ok(GeneratedField::Scalar),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggCall, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                let mut args__ = None;
                let mut return_type__ = None;
                let mut distinct__ = None;
                let mut order_by__ = None;
                let mut filter__ = None;
                let mut direct_args__ = None;
                let mut udf__ = None;
                let mut scalar__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<agg_call::Kind>()? as i32);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                        GeneratedField::DirectArgs => {
                            if direct_args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("directArgs"));
                            }
                            direct_args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Udf => {
                            if udf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udf"));
                            }
                            udf__ = map_.next_value()?;
                        }
                        GeneratedField::Scalar => {
                            if scalar__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalar"));
                            }
                            scalar__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AggCall {
                    kind: kind__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    return_type: return_type__,
                    distinct: distinct__.unwrap_or_default(),
                    order_by: order_by__.unwrap_or_default(),
                    filter: filter__,
                    direct_args: direct_args__.unwrap_or_default(),
                    udf: udf__,
                    scalar: scalar__,
                })
            }
        }
        deserializer.deserialize_struct("expr.AggCall", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for agg_call::Kind {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Count => "COUNT",
            Self::Avg => "AVG",
            Self::StringAgg => "STRING_AGG",
            Self::ApproxCountDistinct => "APPROX_COUNT_DISTINCT",
            Self::ArrayAgg => "ARRAY_AGG",
            Self::FirstValue => "FIRST_VALUE",
            Self::Sum0 => "SUM0",
            Self::VarPop => "VAR_POP",
            Self::VarSamp => "VAR_SAMP",
            Self::StddevPop => "STDDEV_POP",
            Self::StddevSamp => "STDDEV_SAMP",
            Self::BitAnd => "BIT_AND",
            Self::BitOr => "BIT_OR",
            Self::BitXor => "BIT_XOR",
            Self::BoolAnd => "BOOL_AND",
            Self::BoolOr => "BOOL_OR",
            Self::JsonbAgg => "JSONB_AGG",
            Self::JsonbObjectAgg => "JSONB_OBJECT_AGG",
            Self::PercentileCont => "PERCENTILE_CONT",
            Self::PercentileDisc => "PERCENTILE_DISC",
            Self::Mode => "MODE",
            Self::LastValue => "LAST_VALUE",
            Self::Grouping => "GROUPING",
            Self::InternalLastSeenValue => "INTERNAL_LAST_SEEN_VALUE",
            Self::ApproxPercentile => "APPROX_PERCENTILE",
            Self::UserDefined => "USER_DEFINED",
            Self::WrapScalar => "WRAP_SCALAR",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for agg_call::Kind {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "SUM",
            "MIN",
            "MAX",
            "COUNT",
            "AVG",
            "STRING_AGG",
            "APPROX_COUNT_DISTINCT",
            "ARRAY_AGG",
            "FIRST_VALUE",
            "SUM0",
            "VAR_POP",
            "VAR_SAMP",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "BIT_AND",
            "BIT_OR",
            "BIT_XOR",
            "BOOL_AND",
            "BOOL_OR",
            "JSONB_AGG",
            "JSONB_OBJECT_AGG",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "MODE",
            "LAST_VALUE",
            "GROUPING",
            "INTERNAL_LAST_SEEN_VALUE",
            "APPROX_PERCENTILE",
            "USER_DEFINED",
            "WRAP_SCALAR",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = agg_call::Kind;

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
                    "UNSPECIFIED" => Ok(agg_call::Kind::Unspecified),
                    "SUM" => Ok(agg_call::Kind::Sum),
                    "MIN" => Ok(agg_call::Kind::Min),
                    "MAX" => Ok(agg_call::Kind::Max),
                    "COUNT" => Ok(agg_call::Kind::Count),
                    "AVG" => Ok(agg_call::Kind::Avg),
                    "STRING_AGG" => Ok(agg_call::Kind::StringAgg),
                    "APPROX_COUNT_DISTINCT" => Ok(agg_call::Kind::ApproxCountDistinct),
                    "ARRAY_AGG" => Ok(agg_call::Kind::ArrayAgg),
                    "FIRST_VALUE" => Ok(agg_call::Kind::FirstValue),
                    "SUM0" => Ok(agg_call::Kind::Sum0),
                    "VAR_POP" => Ok(agg_call::Kind::VarPop),
                    "VAR_SAMP" => Ok(agg_call::Kind::VarSamp),
                    "STDDEV_POP" => Ok(agg_call::Kind::StddevPop),
                    "STDDEV_SAMP" => Ok(agg_call::Kind::StddevSamp),
                    "BIT_AND" => Ok(agg_call::Kind::BitAnd),
                    "BIT_OR" => Ok(agg_call::Kind::BitOr),
                    "BIT_XOR" => Ok(agg_call::Kind::BitXor),
                    "BOOL_AND" => Ok(agg_call::Kind::BoolAnd),
                    "BOOL_OR" => Ok(agg_call::Kind::BoolOr),
                    "JSONB_AGG" => Ok(agg_call::Kind::JsonbAgg),
                    "JSONB_OBJECT_AGG" => Ok(agg_call::Kind::JsonbObjectAgg),
                    "PERCENTILE_CONT" => Ok(agg_call::Kind::PercentileCont),
                    "PERCENTILE_DISC" => Ok(agg_call::Kind::PercentileDisc),
                    "MODE" => Ok(agg_call::Kind::Mode),
                    "LAST_VALUE" => Ok(agg_call::Kind::LastValue),
                    "GROUPING" => Ok(agg_call::Kind::Grouping),
                    "INTERNAL_LAST_SEEN_VALUE" => Ok(agg_call::Kind::InternalLastSeenValue),
                    "APPROX_PERCENTILE" => Ok(agg_call::Kind::ApproxPercentile),
                    "USER_DEFINED" => Ok(agg_call::Kind::UserDefined),
                    "WRAP_SCALAR" => Ok(agg_call::Kind::WrapScalar),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AggType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.kind != 0 {
            len += 1;
        }
        if self.udf_meta.is_some() {
            len += 1;
        }
        if self.scalar_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.AggType", len)?;
        if self.kind != 0 {
            let v = agg_call::Kind::try_from(self.kind)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.kind)))?;
            struct_ser.serialize_field("kind", &v)?;
        }
        if let Some(v) = self.udf_meta.as_ref() {
            struct_ser.serialize_field("udfMeta", v)?;
        }
        if let Some(v) = self.scalar_expr.as_ref() {
            struct_ser.serialize_field("scalarExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "kind",
            "udf_meta",
            "udfMeta",
            "scalar_expr",
            "scalarExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Kind,
            UdfMeta,
            ScalarExpr,
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
                            "kind" => Ok(GeneratedField::Kind),
                            "udfMeta" | "udf_meta" => Ok(GeneratedField::UdfMeta),
                            "scalarExpr" | "scalar_expr" => Ok(GeneratedField::ScalarExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.AggType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut kind__ = None;
                let mut udf_meta__ = None;
                let mut scalar_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Kind => {
                            if kind__.is_some() {
                                return Err(serde::de::Error::duplicate_field("kind"));
                            }
                            kind__ = Some(map_.next_value::<agg_call::Kind>()? as i32);
                        }
                        GeneratedField::UdfMeta => {
                            if udf_meta__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udfMeta"));
                            }
                            udf_meta__ = map_.next_value()?;
                        }
                        GeneratedField::ScalarExpr => {
                            if scalar_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarExpr"));
                            }
                            scalar_expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AggType {
                    kind: kind__.unwrap_or_default(),
                    udf_meta: udf_meta__,
                    scalar_expr: scalar_expr__,
                })
            }
        }
        deserializer.deserialize_struct("expr.AggType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Constant {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.datum.is_some() {
            len += 1;
        }
        if self.r#type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.Constant", len)?;
        if let Some(v) = self.datum.as_ref() {
            struct_ser.serialize_field("datum", v)?;
        }
        if let Some(v) = self.r#type.as_ref() {
            struct_ser.serialize_field("type", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Constant {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "datum",
            "type",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Datum,
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "datum" => Ok(GeneratedField::Datum),
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
            type Value = Constant;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.Constant")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Constant, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut datum__ = None;
                let mut r#type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Datum => {
                            if datum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("datum"));
                            }
                            datum__ = map_.next_value()?;
                        }
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Constant {
                    datum: datum__,
                    r#type: r#type__,
                })
            }
        }
        deserializer.deserialize_struct("expr.Constant", FIELDS, GeneratedVisitor)
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
        if self.function_type != 0 {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if self.rex_node.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.ExprNode", len)?;
        if self.function_type != 0 {
            let v = expr_node::Type::try_from(self.function_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.function_type)))?;
            struct_ser.serialize_field("functionType", &v)?;
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
                expr_node::RexNode::Udf(v) => {
                    struct_ser.serialize_field("udf", v)?;
                }
                expr_node::RexNode::Now(v) => {
                    struct_ser.serialize_field("now", v)?;
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
            "function_type",
            "functionType",
            "return_type",
            "returnType",
            "input_ref",
            "inputRef",
            "constant",
            "func_call",
            "funcCall",
            "udf",
            "now",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunctionType,
            ReturnType,
            InputRef,
            Constant,
            FuncCall,
            Udf,
            Now,
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
                            "functionType" | "function_type" => Ok(GeneratedField::FunctionType),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            "inputRef" | "input_ref" => Ok(GeneratedField::InputRef),
                            "constant" => Ok(GeneratedField::Constant),
                            "funcCall" | "func_call" => Ok(GeneratedField::FuncCall),
                            "udf" => Ok(GeneratedField::Udf),
                            "now" => Ok(GeneratedField::Now),
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut function_type__ = None;
                let mut return_type__ = None;
                let mut rex_node__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FunctionType => {
                            if function_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("functionType"));
                            }
                            function_type__ = Some(map_.next_value::<expr_node::Type>()? as i32);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                        GeneratedField::InputRef => {
                            if rex_node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputRef"));
                            }
                            rex_node__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| expr_node::RexNode::InputRef(x.0));
                        }
                        GeneratedField::Constant => {
                            if rex_node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constant"));
                            }
                            rex_node__ = map_.next_value::<::std::option::Option<_>>()?.map(expr_node::RexNode::Constant)
;
                        }
                        GeneratedField::FuncCall => {
                            if rex_node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funcCall"));
                            }
                            rex_node__ = map_.next_value::<::std::option::Option<_>>()?.map(expr_node::RexNode::FuncCall)
;
                        }
                        GeneratedField::Udf => {
                            if rex_node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udf"));
                            }
                            rex_node__ = map_.next_value::<::std::option::Option<_>>()?.map(expr_node::RexNode::Udf)
;
                        }
                        GeneratedField::Now => {
                            if rex_node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("now"));
                            }
                            rex_node__ = map_.next_value::<::std::option::Option<_>>()?.map(expr_node::RexNode::Now)
;
                        }
                    }
                }
                Ok(ExprNode {
                    function_type: function_type__.unwrap_or_default(),
                    return_type: return_type__,
                    rex_node: rex_node__,
                })
            }
        }
        deserializer.deserialize_struct("expr.ExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for expr_node::NowRexNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let len = 0;
        let struct_ser = serializer.serialize_struct("expr.ExprNode.NowRexNode", len)?;
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for expr_node::NowRexNode {
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
            type Value = expr_node::NowRexNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.ExprNode.NowRexNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<expr_node::NowRexNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                while map_.next_key::<GeneratedField>()?.is_some() {
                    let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                }
                Ok(expr_node::NowRexNode {
                })
            }
        }
        deserializer.deserialize_struct("expr.ExprNode.NowRexNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for expr_node::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
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
            Self::Greatest => "GREATEST",
            Self::Least => "LEAST",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Not => "NOT",
            Self::In => "IN",
            Self::Some => "SOME",
            Self::All => "ALL",
            Self::BitwiseAnd => "BITWISE_AND",
            Self::BitwiseOr => "BITWISE_OR",
            Self::BitwiseXor => "BITWISE_XOR",
            Self::BitwiseNot => "BITWISE_NOT",
            Self::BitwiseShiftLeft => "BITWISE_SHIFT_LEFT",
            Self::BitwiseShiftRight => "BITWISE_SHIFT_RIGHT",
            Self::Extract => "EXTRACT",
            Self::DatePart => "DATE_PART",
            Self::TumbleStart => "TUMBLE_START",
            Self::MakeDate => "MAKE_DATE",
            Self::MakeTime => "MAKE_TIME",
            Self::MakeTimestamp => "MAKE_TIMESTAMP",
            Self::DateBin => "DATE_BIN",
            Self::SecToTimestamptz => "SEC_TO_TIMESTAMPTZ",
            Self::AtTimeZone => "AT_TIME_ZONE",
            Self::DateTrunc => "DATE_TRUNC",
            Self::CharToTimestamptz => "CHAR_TO_TIMESTAMPTZ",
            Self::CharToDate => "CHAR_TO_DATE",
            Self::CastWithTimeZone => "CAST_WITH_TIME_ZONE",
            Self::AddWithTimeZone => "ADD_WITH_TIME_ZONE",
            Self::SubtractWithTimeZone => "SUBTRACT_WITH_TIME_ZONE",
            Self::MakeTimestamptz => "MAKE_TIMESTAMPTZ",
            Self::Cast => "CAST",
            Self::Substr => "SUBSTR",
            Self::Length => "LENGTH",
            Self::Like => "LIKE",
            Self::ILike => "I_LIKE",
            Self::SimilarToEscape => "SIMILAR_TO_ESCAPE",
            Self::Upper => "UPPER",
            Self::Lower => "LOWER",
            Self::Trim => "TRIM",
            Self::Replace => "REPLACE",
            Self::Position => "POSITION",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Case => "CASE",
            Self::ConstantLookup => "CONSTANT_LOOKUP",
            Self::RoundDigit => "ROUND_DIGIT",
            Self::Round => "ROUND",
            Self::Ascii => "ASCII",
            Self::Translate => "TRANSLATE",
            Self::Coalesce => "COALESCE",
            Self::ConcatWs => "CONCAT_WS",
            Self::ConcatWsVariadic => "CONCAT_WS_VARIADIC",
            Self::Abs => "ABS",
            Self::SplitPart => "SPLIT_PART",
            Self::Ceil => "CEIL",
            Self::Floor => "FLOOR",
            Self::ToChar => "TO_CHAR",
            Self::Md5 => "MD5",
            Self::CharLength => "CHAR_LENGTH",
            Self::Repeat => "REPEAT",
            Self::ConcatOp => "CONCAT_OP",
            Self::Concat => "CONCAT",
            Self::ConcatVariadic => "CONCAT_VARIADIC",
            Self::BoolOut => "BOOL_OUT",
            Self::OctetLength => "OCTET_LENGTH",
            Self::BitLength => "BIT_LENGTH",
            Self::Overlay => "OVERLAY",
            Self::RegexpMatch => "REGEXP_MATCH",
            Self::RegexpReplace => "REGEXP_REPLACE",
            Self::RegexpCount => "REGEXP_COUNT",
            Self::RegexpSplitToArray => "REGEXP_SPLIT_TO_ARRAY",
            Self::RegexpEq => "REGEXP_EQ",
            Self::Pow => "POW",
            Self::Exp => "EXP",
            Self::Chr => "CHR",
            Self::StartsWith => "STARTS_WITH",
            Self::Initcap => "INITCAP",
            Self::Lpad => "LPAD",
            Self::Rpad => "RPAD",
            Self::Reverse => "REVERSE",
            Self::Strpos => "STRPOS",
            Self::ToAscii => "TO_ASCII",
            Self::ToHex => "TO_HEX",
            Self::QuoteIdent => "QUOTE_IDENT",
            Self::Sin => "SIN",
            Self::Cos => "COS",
            Self::Tan => "TAN",
            Self::Cot => "COT",
            Self::Asin => "ASIN",
            Self::Acos => "ACOS",
            Self::Atan => "ATAN",
            Self::Atan2 => "ATAN2",
            Self::Sind => "SIND",
            Self::Cosd => "COSD",
            Self::Cotd => "COTD",
            Self::Tand => "TAND",
            Self::Asind => "ASIND",
            Self::Sqrt => "SQRT",
            Self::Degrees => "DEGREES",
            Self::Radians => "RADIANS",
            Self::Cosh => "COSH",
            Self::Tanh => "TANH",
            Self::Coth => "COTH",
            Self::Asinh => "ASINH",
            Self::Acosh => "ACOSH",
            Self::Atanh => "ATANH",
            Self::Sinh => "SINH",
            Self::Acosd => "ACOSD",
            Self::Atand => "ATAND",
            Self::Atan2d => "ATAN2D",
            Self::Trunc => "TRUNC",
            Self::Ln => "LN",
            Self::Log10 => "LOG10",
            Self::Cbrt => "CBRT",
            Self::Sign => "SIGN",
            Self::Scale => "SCALE",
            Self::MinScale => "MIN_SCALE",
            Self::TrimScale => "TRIM_SCALE",
            Self::IsTrue => "IS_TRUE",
            Self::IsNotTrue => "IS_NOT_TRUE",
            Self::IsFalse => "IS_FALSE",
            Self::IsNotFalse => "IS_NOT_FALSE",
            Self::IsNull => "IS_NULL",
            Self::IsNotNull => "IS_NOT_NULL",
            Self::IsDistinctFrom => "IS_DISTINCT_FROM",
            Self::IsNotDistinctFrom => "IS_NOT_DISTINCT_FROM",
            Self::Encode => "ENCODE",
            Self::Decode => "DECODE",
            Self::Sha1 => "SHA1",
            Self::Sha224 => "SHA224",
            Self::Sha256 => "SHA256",
            Self::Sha384 => "SHA384",
            Self::Sha512 => "SHA512",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Format => "FORMAT",
            Self::FormatVariadic => "FORMAT_VARIADIC",
            Self::PgwireSend => "PGWIRE_SEND",
            Self::PgwireRecv => "PGWIRE_RECV",
            Self::ConvertFrom => "CONVERT_FROM",
            Self::ConvertTo => "CONVERT_TO",
            Self::Decrypt => "DECRYPT",
            Self::Encrypt => "ENCRYPT",
            Self::InetAton => "INET_ATON",
            Self::InetNtoa => "INET_NTOA",
            Self::QuoteLiteral => "QUOTE_LITERAL",
            Self::QuoteNullable => "QUOTE_NULLABLE",
            Self::Hmac => "HMAC",
            Self::SecureCompare => "SECURE_COMPARE",
            Self::CheckNotNull => "CHECK_NOT_NULL",
            Self::Neg => "NEG",
            Self::Field => "FIELD",
            Self::Array => "ARRAY",
            Self::ArrayAccess => "ARRAY_ACCESS",
            Self::Row => "ROW",
            Self::ArrayToString => "ARRAY_TO_STRING",
            Self::ArrayRangeAccess => "ARRAY_RANGE_ACCESS",
            Self::ArrayCat => "ARRAY_CAT",
            Self::ArrayAppend => "ARRAY_APPEND",
            Self::ArrayPrepend => "ARRAY_PREPEND",
            Self::FormatType => "FORMAT_TYPE",
            Self::ArrayDistinct => "ARRAY_DISTINCT",
            Self::ArrayLength => "ARRAY_LENGTH",
            Self::Cardinality => "CARDINALITY",
            Self::ArrayRemove => "ARRAY_REMOVE",
            Self::ArrayPositions => "ARRAY_POSITIONS",
            Self::TrimArray => "TRIM_ARRAY",
            Self::StringToArray => "STRING_TO_ARRAY",
            Self::ArrayPosition => "ARRAY_POSITION",
            Self::ArrayReplace => "ARRAY_REPLACE",
            Self::ArrayDims => "ARRAY_DIMS",
            Self::ArrayTransform => "ARRAY_TRANSFORM",
            Self::ArrayMin => "ARRAY_MIN",
            Self::ArrayMax => "ARRAY_MAX",
            Self::ArraySum => "ARRAY_SUM",
            Self::ArraySort => "ARRAY_SORT",
            Self::ArrayContains => "ARRAY_CONTAINS",
            Self::ArrayContained => "ARRAY_CONTAINED",
            Self::ArrayFlatten => "ARRAY_FLATTEN",
            Self::HexToInt256 => "HEX_TO_INT256",
            Self::JsonbAccess => "JSONB_ACCESS",
            Self::JsonbAccessStr => "JSONB_ACCESS_STR",
            Self::JsonbExtractPath => "JSONB_EXTRACT_PATH",
            Self::JsonbExtractPathVariadic => "JSONB_EXTRACT_PATH_VARIADIC",
            Self::JsonbExtractPathText => "JSONB_EXTRACT_PATH_TEXT",
            Self::JsonbExtractPathTextVariadic => "JSONB_EXTRACT_PATH_TEXT_VARIADIC",
            Self::JsonbTypeof => "JSONB_TYPEOF",
            Self::JsonbArrayLength => "JSONB_ARRAY_LENGTH",
            Self::IsJson => "IS_JSON",
            Self::JsonbConcat => "JSONB_CONCAT",
            Self::JsonbObject => "JSONB_OBJECT",
            Self::JsonbPretty => "JSONB_PRETTY",
            Self::JsonbContains => "JSONB_CONTAINS",
            Self::JsonbContained => "JSONB_CONTAINED",
            Self::JsonbExists => "JSONB_EXISTS",
            Self::JsonbExistsAny => "JSONB_EXISTS_ANY",
            Self::JsonbExistsAll => "JSONB_EXISTS_ALL",
            Self::JsonbDeletePath => "JSONB_DELETE_PATH",
            Self::JsonbStripNulls => "JSONB_STRIP_NULLS",
            Self::ToJsonb => "TO_JSONB",
            Self::JsonbBuildArray => "JSONB_BUILD_ARRAY",
            Self::JsonbBuildArrayVariadic => "JSONB_BUILD_ARRAY_VARIADIC",
            Self::JsonbBuildObject => "JSONB_BUILD_OBJECT",
            Self::JsonbBuildObjectVariadic => "JSONB_BUILD_OBJECT_VARIADIC",
            Self::JsonbPathExists => "JSONB_PATH_EXISTS",
            Self::JsonbPathMatch => "JSONB_PATH_MATCH",
            Self::JsonbPathQueryArray => "JSONB_PATH_QUERY_ARRAY",
            Self::JsonbPathQueryFirst => "JSONB_PATH_QUERY_FIRST",
            Self::JsonbPopulateRecord => "JSONB_POPULATE_RECORD",
            Self::JsonbToRecord => "JSONB_TO_RECORD",
            Self::JsonbSet => "JSONB_SET",
            Self::JsonbPopulateMap => "JSONB_POPULATE_MAP",
            Self::MapFromEntries => "MAP_FROM_ENTRIES",
            Self::MapAccess => "MAP_ACCESS",
            Self::MapKeys => "MAP_KEYS",
            Self::MapValues => "MAP_VALUES",
            Self::MapEntries => "MAP_ENTRIES",
            Self::MapFromKeyValues => "MAP_FROM_KEY_VALUES",
            Self::MapLength => "MAP_LENGTH",
            Self::MapContains => "MAP_CONTAINS",
            Self::MapCat => "MAP_CAT",
            Self::MapInsert => "MAP_INSERT",
            Self::MapDelete => "MAP_DELETE",
            Self::CompositeCast => "COMPOSITE_CAST",
            Self::Vnode => "VNODE",
            Self::TestPaidTier => "TEST_PAID_TIER",
            Self::VnodeUser => "VNODE_USER",
            Self::License => "LICENSE",
            Self::Proctime => "PROCTIME",
            Self::PgSleep => "PG_SLEEP",
            Self::PgSleepFor => "PG_SLEEP_FOR",
            Self::PgSleepUntil => "PG_SLEEP_UNTIL",
            Self::CastRegclass => "CAST_REGCLASS",
            Self::PgGetIndexdef => "PG_GET_INDEXDEF",
            Self::ColDescription => "COL_DESCRIPTION",
            Self::PgGetViewdef => "PG_GET_VIEWDEF",
            Self::PgGetUserbyid => "PG_GET_USERBYID",
            Self::PgIndexesSize => "PG_INDEXES_SIZE",
            Self::PgRelationSize => "PG_RELATION_SIZE",
            Self::PgGetSerialSequence => "PG_GET_SERIAL_SEQUENCE",
            Self::PgIndexColumnHasProperty => "PG_INDEX_COLUMN_HAS_PROPERTY",
            Self::HasTablePrivilege => "HAS_TABLE_PRIVILEGE",
            Self::HasAnyColumnPrivilege => "HAS_ANY_COLUMN_PRIVILEGE",
            Self::HasSchemaPrivilege => "HAS_SCHEMA_PRIVILEGE",
            Self::PgIsInRecovery => "PG_IS_IN_RECOVERY",
            Self::RwRecoveryStatus => "RW_RECOVERY_STATUS",
            Self::RwEpochToTs => "RW_EPOCH_TO_TS",
            Self::PgTableIsVisible => "PG_TABLE_IS_VISIBLE",
            Self::HasFunctionPrivilege => "HAS_FUNCTION_PRIVILEGE",
            Self::IcebergTransform => "ICEBERG_TRANSFORM",
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
            "UNSPECIFIED",
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
            "GREATEST",
            "LEAST",
            "AND",
            "OR",
            "NOT",
            "IN",
            "SOME",
            "ALL",
            "BITWISE_AND",
            "BITWISE_OR",
            "BITWISE_XOR",
            "BITWISE_NOT",
            "BITWISE_SHIFT_LEFT",
            "BITWISE_SHIFT_RIGHT",
            "EXTRACT",
            "DATE_PART",
            "TUMBLE_START",
            "MAKE_DATE",
            "MAKE_TIME",
            "MAKE_TIMESTAMP",
            "DATE_BIN",
            "SEC_TO_TIMESTAMPTZ",
            "AT_TIME_ZONE",
            "DATE_TRUNC",
            "CHAR_TO_TIMESTAMPTZ",
            "CHAR_TO_DATE",
            "CAST_WITH_TIME_ZONE",
            "ADD_WITH_TIME_ZONE",
            "SUBTRACT_WITH_TIME_ZONE",
            "MAKE_TIMESTAMPTZ",
            "CAST",
            "SUBSTR",
            "LENGTH",
            "LIKE",
            "I_LIKE",
            "SIMILAR_TO_ESCAPE",
            "UPPER",
            "LOWER",
            "TRIM",
            "REPLACE",
            "POSITION",
            "LTRIM",
            "RTRIM",
            "CASE",
            "CONSTANT_LOOKUP",
            "ROUND_DIGIT",
            "ROUND",
            "ASCII",
            "TRANSLATE",
            "COALESCE",
            "CONCAT_WS",
            "CONCAT_WS_VARIADIC",
            "ABS",
            "SPLIT_PART",
            "CEIL",
            "FLOOR",
            "TO_CHAR",
            "MD5",
            "CHAR_LENGTH",
            "REPEAT",
            "CONCAT_OP",
            "CONCAT",
            "CONCAT_VARIADIC",
            "BOOL_OUT",
            "OCTET_LENGTH",
            "BIT_LENGTH",
            "OVERLAY",
            "REGEXP_MATCH",
            "REGEXP_REPLACE",
            "REGEXP_COUNT",
            "REGEXP_SPLIT_TO_ARRAY",
            "REGEXP_EQ",
            "POW",
            "EXP",
            "CHR",
            "STARTS_WITH",
            "INITCAP",
            "LPAD",
            "RPAD",
            "REVERSE",
            "STRPOS",
            "TO_ASCII",
            "TO_HEX",
            "QUOTE_IDENT",
            "SIN",
            "COS",
            "TAN",
            "COT",
            "ASIN",
            "ACOS",
            "ATAN",
            "ATAN2",
            "SIND",
            "COSD",
            "COTD",
            "TAND",
            "ASIND",
            "SQRT",
            "DEGREES",
            "RADIANS",
            "COSH",
            "TANH",
            "COTH",
            "ASINH",
            "ACOSH",
            "ATANH",
            "SINH",
            "ACOSD",
            "ATAND",
            "ATAN2D",
            "TRUNC",
            "LN",
            "LOG10",
            "CBRT",
            "SIGN",
            "SCALE",
            "MIN_SCALE",
            "TRIM_SCALE",
            "IS_TRUE",
            "IS_NOT_TRUE",
            "IS_FALSE",
            "IS_NOT_FALSE",
            "IS_NULL",
            "IS_NOT_NULL",
            "IS_DISTINCT_FROM",
            "IS_NOT_DISTINCT_FROM",
            "ENCODE",
            "DECODE",
            "SHA1",
            "SHA224",
            "SHA256",
            "SHA384",
            "SHA512",
            "LEFT",
            "RIGHT",
            "FORMAT",
            "FORMAT_VARIADIC",
            "PGWIRE_SEND",
            "PGWIRE_RECV",
            "CONVERT_FROM",
            "CONVERT_TO",
            "DECRYPT",
            "ENCRYPT",
            "INET_ATON",
            "INET_NTOA",
            "QUOTE_LITERAL",
            "QUOTE_NULLABLE",
            "HMAC",
            "SECURE_COMPARE",
            "CHECK_NOT_NULL",
            "NEG",
            "FIELD",
            "ARRAY",
            "ARRAY_ACCESS",
            "ROW",
            "ARRAY_TO_STRING",
            "ARRAY_RANGE_ACCESS",
            "ARRAY_CAT",
            "ARRAY_APPEND",
            "ARRAY_PREPEND",
            "FORMAT_TYPE",
            "ARRAY_DISTINCT",
            "ARRAY_LENGTH",
            "CARDINALITY",
            "ARRAY_REMOVE",
            "ARRAY_POSITIONS",
            "TRIM_ARRAY",
            "STRING_TO_ARRAY",
            "ARRAY_POSITION",
            "ARRAY_REPLACE",
            "ARRAY_DIMS",
            "ARRAY_TRANSFORM",
            "ARRAY_MIN",
            "ARRAY_MAX",
            "ARRAY_SUM",
            "ARRAY_SORT",
            "ARRAY_CONTAINS",
            "ARRAY_CONTAINED",
            "ARRAY_FLATTEN",
            "HEX_TO_INT256",
            "JSONB_ACCESS",
            "JSONB_ACCESS_STR",
            "JSONB_EXTRACT_PATH",
            "JSONB_EXTRACT_PATH_VARIADIC",
            "JSONB_EXTRACT_PATH_TEXT",
            "JSONB_EXTRACT_PATH_TEXT_VARIADIC",
            "JSONB_TYPEOF",
            "JSONB_ARRAY_LENGTH",
            "IS_JSON",
            "JSONB_CONCAT",
            "JSONB_OBJECT",
            "JSONB_PRETTY",
            "JSONB_CONTAINS",
            "JSONB_CONTAINED",
            "JSONB_EXISTS",
            "JSONB_EXISTS_ANY",
            "JSONB_EXISTS_ALL",
            "JSONB_DELETE_PATH",
            "JSONB_STRIP_NULLS",
            "TO_JSONB",
            "JSONB_BUILD_ARRAY",
            "JSONB_BUILD_ARRAY_VARIADIC",
            "JSONB_BUILD_OBJECT",
            "JSONB_BUILD_OBJECT_VARIADIC",
            "JSONB_PATH_EXISTS",
            "JSONB_PATH_MATCH",
            "JSONB_PATH_QUERY_ARRAY",
            "JSONB_PATH_QUERY_FIRST",
            "JSONB_POPULATE_RECORD",
            "JSONB_TO_RECORD",
            "JSONB_SET",
            "JSONB_POPULATE_MAP",
            "MAP_FROM_ENTRIES",
            "MAP_ACCESS",
            "MAP_KEYS",
            "MAP_VALUES",
            "MAP_ENTRIES",
            "MAP_FROM_KEY_VALUES",
            "MAP_LENGTH",
            "MAP_CONTAINS",
            "MAP_CAT",
            "MAP_INSERT",
            "MAP_DELETE",
            "COMPOSITE_CAST",
            "VNODE",
            "TEST_PAID_TIER",
            "VNODE_USER",
            "LICENSE",
            "PROCTIME",
            "PG_SLEEP",
            "PG_SLEEP_FOR",
            "PG_SLEEP_UNTIL",
            "CAST_REGCLASS",
            "PG_GET_INDEXDEF",
            "COL_DESCRIPTION",
            "PG_GET_VIEWDEF",
            "PG_GET_USERBYID",
            "PG_INDEXES_SIZE",
            "PG_RELATION_SIZE",
            "PG_GET_SERIAL_SEQUENCE",
            "PG_INDEX_COLUMN_HAS_PROPERTY",
            "HAS_TABLE_PRIVILEGE",
            "HAS_ANY_COLUMN_PRIVILEGE",
            "HAS_SCHEMA_PRIVILEGE",
            "PG_IS_IN_RECOVERY",
            "RW_RECOVERY_STATUS",
            "RW_EPOCH_TO_TS",
            "PG_TABLE_IS_VISIBLE",
            "HAS_FUNCTION_PRIVILEGE",
            "ICEBERG_TRANSFORM",
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
                    "UNSPECIFIED" => Ok(expr_node::Type::Unspecified),
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
                    "GREATEST" => Ok(expr_node::Type::Greatest),
                    "LEAST" => Ok(expr_node::Type::Least),
                    "AND" => Ok(expr_node::Type::And),
                    "OR" => Ok(expr_node::Type::Or),
                    "NOT" => Ok(expr_node::Type::Not),
                    "IN" => Ok(expr_node::Type::In),
                    "SOME" => Ok(expr_node::Type::Some),
                    "ALL" => Ok(expr_node::Type::All),
                    "BITWISE_AND" => Ok(expr_node::Type::BitwiseAnd),
                    "BITWISE_OR" => Ok(expr_node::Type::BitwiseOr),
                    "BITWISE_XOR" => Ok(expr_node::Type::BitwiseXor),
                    "BITWISE_NOT" => Ok(expr_node::Type::BitwiseNot),
                    "BITWISE_SHIFT_LEFT" => Ok(expr_node::Type::BitwiseShiftLeft),
                    "BITWISE_SHIFT_RIGHT" => Ok(expr_node::Type::BitwiseShiftRight),
                    "EXTRACT" => Ok(expr_node::Type::Extract),
                    "DATE_PART" => Ok(expr_node::Type::DatePart),
                    "TUMBLE_START" => Ok(expr_node::Type::TumbleStart),
                    "MAKE_DATE" => Ok(expr_node::Type::MakeDate),
                    "MAKE_TIME" => Ok(expr_node::Type::MakeTime),
                    "MAKE_TIMESTAMP" => Ok(expr_node::Type::MakeTimestamp),
                    "DATE_BIN" => Ok(expr_node::Type::DateBin),
                    "SEC_TO_TIMESTAMPTZ" => Ok(expr_node::Type::SecToTimestamptz),
                    "AT_TIME_ZONE" => Ok(expr_node::Type::AtTimeZone),
                    "DATE_TRUNC" => Ok(expr_node::Type::DateTrunc),
                    "CHAR_TO_TIMESTAMPTZ" => Ok(expr_node::Type::CharToTimestamptz),
                    "CHAR_TO_DATE" => Ok(expr_node::Type::CharToDate),
                    "CAST_WITH_TIME_ZONE" => Ok(expr_node::Type::CastWithTimeZone),
                    "ADD_WITH_TIME_ZONE" => Ok(expr_node::Type::AddWithTimeZone),
                    "SUBTRACT_WITH_TIME_ZONE" => Ok(expr_node::Type::SubtractWithTimeZone),
                    "MAKE_TIMESTAMPTZ" => Ok(expr_node::Type::MakeTimestamptz),
                    "CAST" => Ok(expr_node::Type::Cast),
                    "SUBSTR" => Ok(expr_node::Type::Substr),
                    "LENGTH" => Ok(expr_node::Type::Length),
                    "LIKE" => Ok(expr_node::Type::Like),
                    "I_LIKE" => Ok(expr_node::Type::ILike),
                    "SIMILAR_TO_ESCAPE" => Ok(expr_node::Type::SimilarToEscape),
                    "UPPER" => Ok(expr_node::Type::Upper),
                    "LOWER" => Ok(expr_node::Type::Lower),
                    "TRIM" => Ok(expr_node::Type::Trim),
                    "REPLACE" => Ok(expr_node::Type::Replace),
                    "POSITION" => Ok(expr_node::Type::Position),
                    "LTRIM" => Ok(expr_node::Type::Ltrim),
                    "RTRIM" => Ok(expr_node::Type::Rtrim),
                    "CASE" => Ok(expr_node::Type::Case),
                    "CONSTANT_LOOKUP" => Ok(expr_node::Type::ConstantLookup),
                    "ROUND_DIGIT" => Ok(expr_node::Type::RoundDigit),
                    "ROUND" => Ok(expr_node::Type::Round),
                    "ASCII" => Ok(expr_node::Type::Ascii),
                    "TRANSLATE" => Ok(expr_node::Type::Translate),
                    "COALESCE" => Ok(expr_node::Type::Coalesce),
                    "CONCAT_WS" => Ok(expr_node::Type::ConcatWs),
                    "CONCAT_WS_VARIADIC" => Ok(expr_node::Type::ConcatWsVariadic),
                    "ABS" => Ok(expr_node::Type::Abs),
                    "SPLIT_PART" => Ok(expr_node::Type::SplitPart),
                    "CEIL" => Ok(expr_node::Type::Ceil),
                    "FLOOR" => Ok(expr_node::Type::Floor),
                    "TO_CHAR" => Ok(expr_node::Type::ToChar),
                    "MD5" => Ok(expr_node::Type::Md5),
                    "CHAR_LENGTH" => Ok(expr_node::Type::CharLength),
                    "REPEAT" => Ok(expr_node::Type::Repeat),
                    "CONCAT_OP" => Ok(expr_node::Type::ConcatOp),
                    "CONCAT" => Ok(expr_node::Type::Concat),
                    "CONCAT_VARIADIC" => Ok(expr_node::Type::ConcatVariadic),
                    "BOOL_OUT" => Ok(expr_node::Type::BoolOut),
                    "OCTET_LENGTH" => Ok(expr_node::Type::OctetLength),
                    "BIT_LENGTH" => Ok(expr_node::Type::BitLength),
                    "OVERLAY" => Ok(expr_node::Type::Overlay),
                    "REGEXP_MATCH" => Ok(expr_node::Type::RegexpMatch),
                    "REGEXP_REPLACE" => Ok(expr_node::Type::RegexpReplace),
                    "REGEXP_COUNT" => Ok(expr_node::Type::RegexpCount),
                    "REGEXP_SPLIT_TO_ARRAY" => Ok(expr_node::Type::RegexpSplitToArray),
                    "REGEXP_EQ" => Ok(expr_node::Type::RegexpEq),
                    "POW" => Ok(expr_node::Type::Pow),
                    "EXP" => Ok(expr_node::Type::Exp),
                    "CHR" => Ok(expr_node::Type::Chr),
                    "STARTS_WITH" => Ok(expr_node::Type::StartsWith),
                    "INITCAP" => Ok(expr_node::Type::Initcap),
                    "LPAD" => Ok(expr_node::Type::Lpad),
                    "RPAD" => Ok(expr_node::Type::Rpad),
                    "REVERSE" => Ok(expr_node::Type::Reverse),
                    "STRPOS" => Ok(expr_node::Type::Strpos),
                    "TO_ASCII" => Ok(expr_node::Type::ToAscii),
                    "TO_HEX" => Ok(expr_node::Type::ToHex),
                    "QUOTE_IDENT" => Ok(expr_node::Type::QuoteIdent),
                    "SIN" => Ok(expr_node::Type::Sin),
                    "COS" => Ok(expr_node::Type::Cos),
                    "TAN" => Ok(expr_node::Type::Tan),
                    "COT" => Ok(expr_node::Type::Cot),
                    "ASIN" => Ok(expr_node::Type::Asin),
                    "ACOS" => Ok(expr_node::Type::Acos),
                    "ATAN" => Ok(expr_node::Type::Atan),
                    "ATAN2" => Ok(expr_node::Type::Atan2),
                    "SIND" => Ok(expr_node::Type::Sind),
                    "COSD" => Ok(expr_node::Type::Cosd),
                    "COTD" => Ok(expr_node::Type::Cotd),
                    "TAND" => Ok(expr_node::Type::Tand),
                    "ASIND" => Ok(expr_node::Type::Asind),
                    "SQRT" => Ok(expr_node::Type::Sqrt),
                    "DEGREES" => Ok(expr_node::Type::Degrees),
                    "RADIANS" => Ok(expr_node::Type::Radians),
                    "COSH" => Ok(expr_node::Type::Cosh),
                    "TANH" => Ok(expr_node::Type::Tanh),
                    "COTH" => Ok(expr_node::Type::Coth),
                    "ASINH" => Ok(expr_node::Type::Asinh),
                    "ACOSH" => Ok(expr_node::Type::Acosh),
                    "ATANH" => Ok(expr_node::Type::Atanh),
                    "SINH" => Ok(expr_node::Type::Sinh),
                    "ACOSD" => Ok(expr_node::Type::Acosd),
                    "ATAND" => Ok(expr_node::Type::Atand),
                    "ATAN2D" => Ok(expr_node::Type::Atan2d),
                    "TRUNC" => Ok(expr_node::Type::Trunc),
                    "LN" => Ok(expr_node::Type::Ln),
                    "LOG10" => Ok(expr_node::Type::Log10),
                    "CBRT" => Ok(expr_node::Type::Cbrt),
                    "SIGN" => Ok(expr_node::Type::Sign),
                    "SCALE" => Ok(expr_node::Type::Scale),
                    "MIN_SCALE" => Ok(expr_node::Type::MinScale),
                    "TRIM_SCALE" => Ok(expr_node::Type::TrimScale),
                    "IS_TRUE" => Ok(expr_node::Type::IsTrue),
                    "IS_NOT_TRUE" => Ok(expr_node::Type::IsNotTrue),
                    "IS_FALSE" => Ok(expr_node::Type::IsFalse),
                    "IS_NOT_FALSE" => Ok(expr_node::Type::IsNotFalse),
                    "IS_NULL" => Ok(expr_node::Type::IsNull),
                    "IS_NOT_NULL" => Ok(expr_node::Type::IsNotNull),
                    "IS_DISTINCT_FROM" => Ok(expr_node::Type::IsDistinctFrom),
                    "IS_NOT_DISTINCT_FROM" => Ok(expr_node::Type::IsNotDistinctFrom),
                    "ENCODE" => Ok(expr_node::Type::Encode),
                    "DECODE" => Ok(expr_node::Type::Decode),
                    "SHA1" => Ok(expr_node::Type::Sha1),
                    "SHA224" => Ok(expr_node::Type::Sha224),
                    "SHA256" => Ok(expr_node::Type::Sha256),
                    "SHA384" => Ok(expr_node::Type::Sha384),
                    "SHA512" => Ok(expr_node::Type::Sha512),
                    "LEFT" => Ok(expr_node::Type::Left),
                    "RIGHT" => Ok(expr_node::Type::Right),
                    "FORMAT" => Ok(expr_node::Type::Format),
                    "FORMAT_VARIADIC" => Ok(expr_node::Type::FormatVariadic),
                    "PGWIRE_SEND" => Ok(expr_node::Type::PgwireSend),
                    "PGWIRE_RECV" => Ok(expr_node::Type::PgwireRecv),
                    "CONVERT_FROM" => Ok(expr_node::Type::ConvertFrom),
                    "CONVERT_TO" => Ok(expr_node::Type::ConvertTo),
                    "DECRYPT" => Ok(expr_node::Type::Decrypt),
                    "ENCRYPT" => Ok(expr_node::Type::Encrypt),
                    "INET_ATON" => Ok(expr_node::Type::InetAton),
                    "INET_NTOA" => Ok(expr_node::Type::InetNtoa),
                    "QUOTE_LITERAL" => Ok(expr_node::Type::QuoteLiteral),
                    "QUOTE_NULLABLE" => Ok(expr_node::Type::QuoteNullable),
                    "HMAC" => Ok(expr_node::Type::Hmac),
                    "SECURE_COMPARE" => Ok(expr_node::Type::SecureCompare),
                    "CHECK_NOT_NULL" => Ok(expr_node::Type::CheckNotNull),
                    "NEG" => Ok(expr_node::Type::Neg),
                    "FIELD" => Ok(expr_node::Type::Field),
                    "ARRAY" => Ok(expr_node::Type::Array),
                    "ARRAY_ACCESS" => Ok(expr_node::Type::ArrayAccess),
                    "ROW" => Ok(expr_node::Type::Row),
                    "ARRAY_TO_STRING" => Ok(expr_node::Type::ArrayToString),
                    "ARRAY_RANGE_ACCESS" => Ok(expr_node::Type::ArrayRangeAccess),
                    "ARRAY_CAT" => Ok(expr_node::Type::ArrayCat),
                    "ARRAY_APPEND" => Ok(expr_node::Type::ArrayAppend),
                    "ARRAY_PREPEND" => Ok(expr_node::Type::ArrayPrepend),
                    "FORMAT_TYPE" => Ok(expr_node::Type::FormatType),
                    "ARRAY_DISTINCT" => Ok(expr_node::Type::ArrayDistinct),
                    "ARRAY_LENGTH" => Ok(expr_node::Type::ArrayLength),
                    "CARDINALITY" => Ok(expr_node::Type::Cardinality),
                    "ARRAY_REMOVE" => Ok(expr_node::Type::ArrayRemove),
                    "ARRAY_POSITIONS" => Ok(expr_node::Type::ArrayPositions),
                    "TRIM_ARRAY" => Ok(expr_node::Type::TrimArray),
                    "STRING_TO_ARRAY" => Ok(expr_node::Type::StringToArray),
                    "ARRAY_POSITION" => Ok(expr_node::Type::ArrayPosition),
                    "ARRAY_REPLACE" => Ok(expr_node::Type::ArrayReplace),
                    "ARRAY_DIMS" => Ok(expr_node::Type::ArrayDims),
                    "ARRAY_TRANSFORM" => Ok(expr_node::Type::ArrayTransform),
                    "ARRAY_MIN" => Ok(expr_node::Type::ArrayMin),
                    "ARRAY_MAX" => Ok(expr_node::Type::ArrayMax),
                    "ARRAY_SUM" => Ok(expr_node::Type::ArraySum),
                    "ARRAY_SORT" => Ok(expr_node::Type::ArraySort),
                    "ARRAY_CONTAINS" => Ok(expr_node::Type::ArrayContains),
                    "ARRAY_CONTAINED" => Ok(expr_node::Type::ArrayContained),
                    "ARRAY_FLATTEN" => Ok(expr_node::Type::ArrayFlatten),
                    "HEX_TO_INT256" => Ok(expr_node::Type::HexToInt256),
                    "JSONB_ACCESS" => Ok(expr_node::Type::JsonbAccess),
                    "JSONB_ACCESS_STR" => Ok(expr_node::Type::JsonbAccessStr),
                    "JSONB_EXTRACT_PATH" => Ok(expr_node::Type::JsonbExtractPath),
                    "JSONB_EXTRACT_PATH_VARIADIC" => Ok(expr_node::Type::JsonbExtractPathVariadic),
                    "JSONB_EXTRACT_PATH_TEXT" => Ok(expr_node::Type::JsonbExtractPathText),
                    "JSONB_EXTRACT_PATH_TEXT_VARIADIC" => Ok(expr_node::Type::JsonbExtractPathTextVariadic),
                    "JSONB_TYPEOF" => Ok(expr_node::Type::JsonbTypeof),
                    "JSONB_ARRAY_LENGTH" => Ok(expr_node::Type::JsonbArrayLength),
                    "IS_JSON" => Ok(expr_node::Type::IsJson),
                    "JSONB_CONCAT" => Ok(expr_node::Type::JsonbConcat),
                    "JSONB_OBJECT" => Ok(expr_node::Type::JsonbObject),
                    "JSONB_PRETTY" => Ok(expr_node::Type::JsonbPretty),
                    "JSONB_CONTAINS" => Ok(expr_node::Type::JsonbContains),
                    "JSONB_CONTAINED" => Ok(expr_node::Type::JsonbContained),
                    "JSONB_EXISTS" => Ok(expr_node::Type::JsonbExists),
                    "JSONB_EXISTS_ANY" => Ok(expr_node::Type::JsonbExistsAny),
                    "JSONB_EXISTS_ALL" => Ok(expr_node::Type::JsonbExistsAll),
                    "JSONB_DELETE_PATH" => Ok(expr_node::Type::JsonbDeletePath),
                    "JSONB_STRIP_NULLS" => Ok(expr_node::Type::JsonbStripNulls),
                    "TO_JSONB" => Ok(expr_node::Type::ToJsonb),
                    "JSONB_BUILD_ARRAY" => Ok(expr_node::Type::JsonbBuildArray),
                    "JSONB_BUILD_ARRAY_VARIADIC" => Ok(expr_node::Type::JsonbBuildArrayVariadic),
                    "JSONB_BUILD_OBJECT" => Ok(expr_node::Type::JsonbBuildObject),
                    "JSONB_BUILD_OBJECT_VARIADIC" => Ok(expr_node::Type::JsonbBuildObjectVariadic),
                    "JSONB_PATH_EXISTS" => Ok(expr_node::Type::JsonbPathExists),
                    "JSONB_PATH_MATCH" => Ok(expr_node::Type::JsonbPathMatch),
                    "JSONB_PATH_QUERY_ARRAY" => Ok(expr_node::Type::JsonbPathQueryArray),
                    "JSONB_PATH_QUERY_FIRST" => Ok(expr_node::Type::JsonbPathQueryFirst),
                    "JSONB_POPULATE_RECORD" => Ok(expr_node::Type::JsonbPopulateRecord),
                    "JSONB_TO_RECORD" => Ok(expr_node::Type::JsonbToRecord),
                    "JSONB_SET" => Ok(expr_node::Type::JsonbSet),
                    "JSONB_POPULATE_MAP" => Ok(expr_node::Type::JsonbPopulateMap),
                    "MAP_FROM_ENTRIES" => Ok(expr_node::Type::MapFromEntries),
                    "MAP_ACCESS" => Ok(expr_node::Type::MapAccess),
                    "MAP_KEYS" => Ok(expr_node::Type::MapKeys),
                    "MAP_VALUES" => Ok(expr_node::Type::MapValues),
                    "MAP_ENTRIES" => Ok(expr_node::Type::MapEntries),
                    "MAP_FROM_KEY_VALUES" => Ok(expr_node::Type::MapFromKeyValues),
                    "MAP_LENGTH" => Ok(expr_node::Type::MapLength),
                    "MAP_CONTAINS" => Ok(expr_node::Type::MapContains),
                    "MAP_CAT" => Ok(expr_node::Type::MapCat),
                    "MAP_INSERT" => Ok(expr_node::Type::MapInsert),
                    "MAP_DELETE" => Ok(expr_node::Type::MapDelete),
                    "COMPOSITE_CAST" => Ok(expr_node::Type::CompositeCast),
                    "VNODE" => Ok(expr_node::Type::Vnode),
                    "TEST_PAID_TIER" => Ok(expr_node::Type::TestPaidTier),
                    "VNODE_USER" => Ok(expr_node::Type::VnodeUser),
                    "LICENSE" => Ok(expr_node::Type::License),
                    "PROCTIME" => Ok(expr_node::Type::Proctime),
                    "PG_SLEEP" => Ok(expr_node::Type::PgSleep),
                    "PG_SLEEP_FOR" => Ok(expr_node::Type::PgSleepFor),
                    "PG_SLEEP_UNTIL" => Ok(expr_node::Type::PgSleepUntil),
                    "CAST_REGCLASS" => Ok(expr_node::Type::CastRegclass),
                    "PG_GET_INDEXDEF" => Ok(expr_node::Type::PgGetIndexdef),
                    "COL_DESCRIPTION" => Ok(expr_node::Type::ColDescription),
                    "PG_GET_VIEWDEF" => Ok(expr_node::Type::PgGetViewdef),
                    "PG_GET_USERBYID" => Ok(expr_node::Type::PgGetUserbyid),
                    "PG_INDEXES_SIZE" => Ok(expr_node::Type::PgIndexesSize),
                    "PG_RELATION_SIZE" => Ok(expr_node::Type::PgRelationSize),
                    "PG_GET_SERIAL_SEQUENCE" => Ok(expr_node::Type::PgGetSerialSequence),
                    "PG_INDEX_COLUMN_HAS_PROPERTY" => Ok(expr_node::Type::PgIndexColumnHasProperty),
                    "HAS_TABLE_PRIVILEGE" => Ok(expr_node::Type::HasTablePrivilege),
                    "HAS_ANY_COLUMN_PRIVILEGE" => Ok(expr_node::Type::HasAnyColumnPrivilege),
                    "HAS_SCHEMA_PRIVILEGE" => Ok(expr_node::Type::HasSchemaPrivilege),
                    "PG_IS_IN_RECOVERY" => Ok(expr_node::Type::PgIsInRecovery),
                    "RW_RECOVERY_STATUS" => Ok(expr_node::Type::RwRecoveryStatus),
                    "RW_EPOCH_TO_TS" => Ok(expr_node::Type::RwEpochToTs),
                    "PG_TABLE_IS_VISIBLE" => Ok(expr_node::Type::PgTableIsVisible),
                    "HAS_FUNCTION_PRIVILEGE" => Ok(expr_node::Type::HasFunctionPrivilege),
                    "ICEBERG_TRANSFORM" => Ok(expr_node::Type::IcebergTransform),
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

                    #[allow(unused_variables)]
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

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FunctionCall, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Children => {
                            if children__.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FunctionCall {
                    children: children__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.FunctionCall", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InputRef {
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
        if self.r#type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.InputRef", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if let Some(v) = self.r#type.as_ref() {
            struct_ser.serialize_field("type", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InputRef {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "type",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
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
            type Value = InputRef;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.InputRef")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<InputRef, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut r#type__ = None;
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
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(InputRef {
                    index: index__.unwrap_or_default(),
                    r#type: r#type__,
                })
            }
        }
        deserializer.deserialize_struct("expr.InputRef", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectSetSelectItem {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.select_item.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.ProjectSetSelectItem", len)?;
        if let Some(v) = self.select_item.as_ref() {
            match v {
                project_set_select_item::SelectItem::Expr(v) => {
                    struct_ser.serialize_field("expr", v)?;
                }
                project_set_select_item::SelectItem::TableFunction(v) => {
                    struct_ser.serialize_field("tableFunction", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectSetSelectItem {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "table_function",
            "tableFunction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            TableFunction,
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
                            "tableFunction" | "table_function" => Ok(GeneratedField::TableFunction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectSetSelectItem;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.ProjectSetSelectItem")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProjectSetSelectItem, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut select_item__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if select_item__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            select_item__ = map_.next_value::<::std::option::Option<_>>()?.map(project_set_select_item::SelectItem::Expr)
;
                        }
                        GeneratedField::TableFunction => {
                            if select_item__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableFunction"));
                            }
                            select_item__ = map_.next_value::<::std::option::Option<_>>()?.map(project_set_select_item::SelectItem::TableFunction)
;
                        }
                    }
                }
                Ok(ProjectSetSelectItem {
                    select_item: select_item__,
                })
            }
        }
        deserializer.deserialize_struct("expr.ProjectSetSelectItem", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.function_type != 0 {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if self.udf.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.TableFunction", len)?;
        if self.function_type != 0 {
            let v = table_function::Type::try_from(self.function_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.function_type)))?;
            struct_ser.serialize_field("functionType", &v)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        if let Some(v) = self.udf.as_ref() {
            struct_ser.serialize_field("udf", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "function_type",
            "functionType",
            "args",
            "return_type",
            "returnType",
            "udf",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunctionType,
            Args,
            ReturnType,
            Udf,
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
                            "functionType" | "function_type" => Ok(GeneratedField::FunctionType),
                            "args" => Ok(GeneratedField::Args),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            "udf" => Ok(GeneratedField::Udf),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.TableFunction")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TableFunction, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut function_type__ = None;
                let mut args__ = None;
                let mut return_type__ = None;
                let mut udf__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FunctionType => {
                            if function_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("functionType"));
                            }
                            function_type__ = Some(map_.next_value::<table_function::Type>()? as i32);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                        GeneratedField::Udf => {
                            if udf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udf"));
                            }
                            udf__ = map_.next_value()?;
                        }
                    }
                }
                Ok(TableFunction {
                    function_type: function_type__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    return_type: return_type__,
                    udf: udf__,
                })
            }
        }
        deserializer.deserialize_struct("expr.TableFunction", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for table_function::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::GenerateSeries => "GENERATE_SERIES",
            Self::Unnest => "UNNEST",
            Self::RegexpMatches => "REGEXP_MATCHES",
            Self::Range => "RANGE",
            Self::GenerateSubscripts => "GENERATE_SUBSCRIPTS",
            Self::PgExpandarray => "_PG_EXPANDARRAY",
            Self::PgGetKeywords => "PG_GET_KEYWORDS",
            Self::JsonbArrayElements => "JSONB_ARRAY_ELEMENTS",
            Self::JsonbArrayElementsText => "JSONB_ARRAY_ELEMENTS_TEXT",
            Self::JsonbEach => "JSONB_EACH",
            Self::JsonbEachText => "JSONB_EACH_TEXT",
            Self::JsonbObjectKeys => "JSONB_OBJECT_KEYS",
            Self::JsonbPathQuery => "JSONB_PATH_QUERY",
            Self::JsonbPopulateRecordset => "JSONB_POPULATE_RECORDSET",
            Self::JsonbToRecordset => "JSONB_TO_RECORDSET",
            Self::FileScan => "FILE_SCAN",
            Self::PostgresQuery => "POSTGRES_QUERY",
            Self::MysqlQuery => "MYSQL_QUERY",
            Self::UserDefined => "USER_DEFINED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for table_function::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "GENERATE_SERIES",
            "UNNEST",
            "REGEXP_MATCHES",
            "RANGE",
            "GENERATE_SUBSCRIPTS",
            "_PG_EXPANDARRAY",
            "PG_GET_KEYWORDS",
            "JSONB_ARRAY_ELEMENTS",
            "JSONB_ARRAY_ELEMENTS_TEXT",
            "JSONB_EACH",
            "JSONB_EACH_TEXT",
            "JSONB_OBJECT_KEYS",
            "JSONB_PATH_QUERY",
            "JSONB_POPULATE_RECORDSET",
            "JSONB_TO_RECORDSET",
            "FILE_SCAN",
            "POSTGRES_QUERY",
            "MYSQL_QUERY",
            "USER_DEFINED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = table_function::Type;

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
                    "UNSPECIFIED" => Ok(table_function::Type::Unspecified),
                    "GENERATE_SERIES" => Ok(table_function::Type::GenerateSeries),
                    "UNNEST" => Ok(table_function::Type::Unnest),
                    "REGEXP_MATCHES" => Ok(table_function::Type::RegexpMatches),
                    "RANGE" => Ok(table_function::Type::Range),
                    "GENERATE_SUBSCRIPTS" => Ok(table_function::Type::GenerateSubscripts),
                    "_PG_EXPANDARRAY" => Ok(table_function::Type::PgExpandarray),
                    "PG_GET_KEYWORDS" => Ok(table_function::Type::PgGetKeywords),
                    "JSONB_ARRAY_ELEMENTS" => Ok(table_function::Type::JsonbArrayElements),
                    "JSONB_ARRAY_ELEMENTS_TEXT" => Ok(table_function::Type::JsonbArrayElementsText),
                    "JSONB_EACH" => Ok(table_function::Type::JsonbEach),
                    "JSONB_EACH_TEXT" => Ok(table_function::Type::JsonbEachText),
                    "JSONB_OBJECT_KEYS" => Ok(table_function::Type::JsonbObjectKeys),
                    "JSONB_PATH_QUERY" => Ok(table_function::Type::JsonbPathQuery),
                    "JSONB_POPULATE_RECORDSET" => Ok(table_function::Type::JsonbPopulateRecordset),
                    "JSONB_TO_RECORDSET" => Ok(table_function::Type::JsonbToRecordset),
                    "FILE_SCAN" => Ok(table_function::Type::FileScan),
                    "POSTGRES_QUERY" => Ok(table_function::Type::PostgresQuery),
                    "MYSQL_QUERY" => Ok(table_function::Type::MysqlQuery),
                    "USER_DEFINED" => Ok(table_function::Type::UserDefined),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for UdfExprVersion {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UDF_EXPR_VERSION_UNSPECIFIED",
            Self::NameInRuntime => "UDF_EXPR_VERSION_NAME_IN_RUNTIME",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for UdfExprVersion {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UDF_EXPR_VERSION_UNSPECIFIED",
            "UDF_EXPR_VERSION_NAME_IN_RUNTIME",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UdfExprVersion;

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
                    "UDF_EXPR_VERSION_UNSPECIFIED" => Ok(UdfExprVersion::Unspecified),
                    "UDF_EXPR_VERSION_NAME_IN_RUNTIME" => Ok(UdfExprVersion::NameInRuntime),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for UserDefinedFunction {
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
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.arg_names.is_empty() {
            len += 1;
        }
        if !self.arg_types.is_empty() {
            len += 1;
        }
        if !self.language.is_empty() {
            len += 1;
        }
        if self.link.is_some() {
            len += 1;
        }
        if self.identifier.is_some() {
            len += 1;
        }
        if self.body.is_some() {
            len += 1;
        }
        if self.compressed_binary.is_some() {
            len += 1;
        }
        if self.always_retry_on_network_error {
            len += 1;
        }
        if self.runtime.is_some() {
            len += 1;
        }
        if self.is_async.is_some() {
            len += 1;
        }
        if self.is_batched.is_some() {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.UserDefinedFunction", len)?;
        if !self.children.is_empty() {
            struct_ser.serialize_field("children", &self.children)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.arg_names.is_empty() {
            struct_ser.serialize_field("argNames", &self.arg_names)?;
        }
        if !self.arg_types.is_empty() {
            struct_ser.serialize_field("argTypes", &self.arg_types)?;
        }
        if !self.language.is_empty() {
            struct_ser.serialize_field("language", &self.language)?;
        }
        if let Some(v) = self.link.as_ref() {
            struct_ser.serialize_field("link", v)?;
        }
        if let Some(v) = self.identifier.as_ref() {
            struct_ser.serialize_field("identifier", v)?;
        }
        if let Some(v) = self.body.as_ref() {
            struct_ser.serialize_field("body", v)?;
        }
        if let Some(v) = self.compressed_binary.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("compressedBinary", pbjson::private::base64::encode(&v).as_str())?;
        }
        if self.always_retry_on_network_error {
            struct_ser.serialize_field("alwaysRetryOnNetworkError", &self.always_retry_on_network_error)?;
        }
        if let Some(v) = self.runtime.as_ref() {
            struct_ser.serialize_field("runtime", v)?;
        }
        if let Some(v) = self.is_async.as_ref() {
            struct_ser.serialize_field("isAsync", v)?;
        }
        if let Some(v) = self.is_batched.as_ref() {
            struct_ser.serialize_field("isBatched", v)?;
        }
        if self.version != 0 {
            let v = UdfExprVersion::try_from(self.version)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.version)))?;
            struct_ser.serialize_field("version", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UserDefinedFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "children",
            "name",
            "arg_names",
            "argNames",
            "arg_types",
            "argTypes",
            "language",
            "link",
            "identifier",
            "body",
            "compressed_binary",
            "compressedBinary",
            "always_retry_on_network_error",
            "alwaysRetryOnNetworkError",
            "runtime",
            "is_async",
            "isAsync",
            "is_batched",
            "isBatched",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Children,
            Name,
            ArgNames,
            ArgTypes,
            Language,
            Link,
            Identifier,
            Body,
            CompressedBinary,
            AlwaysRetryOnNetworkError,
            Runtime,
            IsAsync,
            IsBatched,
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
                            "children" => Ok(GeneratedField::Children),
                            "name" => Ok(GeneratedField::Name),
                            "argNames" | "arg_names" => Ok(GeneratedField::ArgNames),
                            "argTypes" | "arg_types" => Ok(GeneratedField::ArgTypes),
                            "language" => Ok(GeneratedField::Language),
                            "link" => Ok(GeneratedField::Link),
                            "identifier" => Ok(GeneratedField::Identifier),
                            "body" => Ok(GeneratedField::Body),
                            "compressedBinary" | "compressed_binary" => Ok(GeneratedField::CompressedBinary),
                            "alwaysRetryOnNetworkError" | "always_retry_on_network_error" => Ok(GeneratedField::AlwaysRetryOnNetworkError),
                            "runtime" => Ok(GeneratedField::Runtime),
                            "isAsync" | "is_async" => Ok(GeneratedField::IsAsync),
                            "isBatched" | "is_batched" => Ok(GeneratedField::IsBatched),
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
            type Value = UserDefinedFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.UserDefinedFunction")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UserDefinedFunction, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut children__ = None;
                let mut name__ = None;
                let mut arg_names__ = None;
                let mut arg_types__ = None;
                let mut language__ = None;
                let mut link__ = None;
                let mut identifier__ = None;
                let mut body__ = None;
                let mut compressed_binary__ = None;
                let mut always_retry_on_network_error__ = None;
                let mut runtime__ = None;
                let mut is_async__ = None;
                let mut is_batched__ = None;
                let mut version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Children => {
                            if children__.is_some() {
                                return Err(serde::de::Error::duplicate_field("children"));
                            }
                            children__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ArgNames => {
                            if arg_names__.is_some() {
                                return Err(serde::de::Error::duplicate_field("argNames"));
                            }
                            arg_names__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ArgTypes => {
                            if arg_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("argTypes"));
                            }
                            arg_types__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Language => {
                            if language__.is_some() {
                                return Err(serde::de::Error::duplicate_field("language"));
                            }
                            language__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Link => {
                            if link__.is_some() {
                                return Err(serde::de::Error::duplicate_field("link"));
                            }
                            link__ = map_.next_value()?;
                        }
                        GeneratedField::Identifier => {
                            if identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("identifier"));
                            }
                            identifier__ = map_.next_value()?;
                        }
                        GeneratedField::Body => {
                            if body__.is_some() {
                                return Err(serde::de::Error::duplicate_field("body"));
                            }
                            body__ = map_.next_value()?;
                        }
                        GeneratedField::CompressedBinary => {
                            if compressed_binary__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compressedBinary"));
                            }
                            compressed_binary__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::AlwaysRetryOnNetworkError => {
                            if always_retry_on_network_error__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alwaysRetryOnNetworkError"));
                            }
                            always_retry_on_network_error__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Runtime => {
                            if runtime__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runtime"));
                            }
                            runtime__ = map_.next_value()?;
                        }
                        GeneratedField::IsAsync => {
                            if is_async__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isAsync"));
                            }
                            is_async__ = map_.next_value()?;
                        }
                        GeneratedField::IsBatched => {
                            if is_batched__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isBatched"));
                            }
                            is_batched__ = map_.next_value()?;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value::<UdfExprVersion>()? as i32);
                        }
                    }
                }
                Ok(UserDefinedFunction {
                    children: children__.unwrap_or_default(),
                    name: name__.unwrap_or_default(),
                    arg_names: arg_names__.unwrap_or_default(),
                    arg_types: arg_types__.unwrap_or_default(),
                    language: language__.unwrap_or_default(),
                    link: link__,
                    identifier: identifier__,
                    body: body__,
                    compressed_binary: compressed_binary__,
                    always_retry_on_network_error: always_retry_on_network_error__.unwrap_or_default(),
                    runtime: runtime__,
                    is_async: is_async__,
                    is_batched: is_batched__,
                    version: version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.UserDefinedFunction", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UserDefinedFunctionMetadata {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.arg_names.is_empty() {
            len += 1;
        }
        if !self.arg_types.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if !self.language.is_empty() {
            len += 1;
        }
        if self.link.is_some() {
            len += 1;
        }
        if self.identifier.is_some() {
            len += 1;
        }
        if self.body.is_some() {
            len += 1;
        }
        if self.compressed_binary.is_some() {
            len += 1;
        }
        if self.runtime.is_some() {
            len += 1;
        }
        if self.version != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.UserDefinedFunctionMetadata", len)?;
        if !self.arg_names.is_empty() {
            struct_ser.serialize_field("argNames", &self.arg_names)?;
        }
        if !self.arg_types.is_empty() {
            struct_ser.serialize_field("argTypes", &self.arg_types)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        if !self.language.is_empty() {
            struct_ser.serialize_field("language", &self.language)?;
        }
        if let Some(v) = self.link.as_ref() {
            struct_ser.serialize_field("link", v)?;
        }
        if let Some(v) = self.identifier.as_ref() {
            struct_ser.serialize_field("identifier", v)?;
        }
        if let Some(v) = self.body.as_ref() {
            struct_ser.serialize_field("body", v)?;
        }
        if let Some(v) = self.compressed_binary.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("compressedBinary", pbjson::private::base64::encode(&v).as_str())?;
        }
        if let Some(v) = self.runtime.as_ref() {
            struct_ser.serialize_field("runtime", v)?;
        }
        if self.version != 0 {
            let v = UdfExprVersion::try_from(self.version)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.version)))?;
            struct_ser.serialize_field("version", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UserDefinedFunctionMetadata {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "arg_names",
            "argNames",
            "arg_types",
            "argTypes",
            "return_type",
            "returnType",
            "language",
            "link",
            "identifier",
            "body",
            "compressed_binary",
            "compressedBinary",
            "runtime",
            "version",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ArgNames,
            ArgTypes,
            ReturnType,
            Language,
            Link,
            Identifier,
            Body,
            CompressedBinary,
            Runtime,
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
                            "argNames" | "arg_names" => Ok(GeneratedField::ArgNames),
                            "argTypes" | "arg_types" => Ok(GeneratedField::ArgTypes),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            "language" => Ok(GeneratedField::Language),
                            "link" => Ok(GeneratedField::Link),
                            "identifier" => Ok(GeneratedField::Identifier),
                            "body" => Ok(GeneratedField::Body),
                            "compressedBinary" | "compressed_binary" => Ok(GeneratedField::CompressedBinary),
                            "runtime" => Ok(GeneratedField::Runtime),
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
            type Value = UserDefinedFunctionMetadata;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.UserDefinedFunctionMetadata")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UserDefinedFunctionMetadata, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut arg_names__ = None;
                let mut arg_types__ = None;
                let mut return_type__ = None;
                let mut language__ = None;
                let mut link__ = None;
                let mut identifier__ = None;
                let mut body__ = None;
                let mut compressed_binary__ = None;
                let mut runtime__ = None;
                let mut version__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ArgNames => {
                            if arg_names__.is_some() {
                                return Err(serde::de::Error::duplicate_field("argNames"));
                            }
                            arg_names__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ArgTypes => {
                            if arg_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("argTypes"));
                            }
                            arg_types__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                        GeneratedField::Language => {
                            if language__.is_some() {
                                return Err(serde::de::Error::duplicate_field("language"));
                            }
                            language__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Link => {
                            if link__.is_some() {
                                return Err(serde::de::Error::duplicate_field("link"));
                            }
                            link__ = map_.next_value()?;
                        }
                        GeneratedField::Identifier => {
                            if identifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("identifier"));
                            }
                            identifier__ = map_.next_value()?;
                        }
                        GeneratedField::Body => {
                            if body__.is_some() {
                                return Err(serde::de::Error::duplicate_field("body"));
                            }
                            body__ = map_.next_value()?;
                        }
                        GeneratedField::CompressedBinary => {
                            if compressed_binary__.is_some() {
                                return Err(serde::de::Error::duplicate_field("compressedBinary"));
                            }
                            compressed_binary__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::Runtime => {
                            if runtime__.is_some() {
                                return Err(serde::de::Error::duplicate_field("runtime"));
                            }
                            runtime__ = map_.next_value()?;
                        }
                        GeneratedField::Version => {
                            if version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("version"));
                            }
                            version__ = Some(map_.next_value::<UdfExprVersion>()? as i32);
                        }
                    }
                }
                Ok(UserDefinedFunctionMetadata {
                    arg_names: arg_names__.unwrap_or_default(),
                    arg_types: arg_types__.unwrap_or_default(),
                    return_type: return_type__,
                    language: language__.unwrap_or_default(),
                    link: link__,
                    identifier: identifier__,
                    body: body__,
                    compressed_binary: compressed_binary__,
                    runtime: runtime__,
                    version: version__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("expr.UserDefinedFunctionMetadata", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrame {
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
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        if self.exclusion != 0 {
            len += 1;
        }
        if self.bounds.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame", len)?;
        if self.r#type != 0 {
            let v = window_frame::Type::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        if self.exclusion != 0 {
            let v = window_frame::Exclusion::try_from(self.exclusion)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.exclusion)))?;
            struct_ser.serialize_field("exclusion", &v)?;
        }
        if let Some(v) = self.bounds.as_ref() {
            match v {
                window_frame::Bounds::Rows(v) => {
                    struct_ser.serialize_field("rows", v)?;
                }
                window_frame::Bounds::Range(v) => {
                    struct_ser.serialize_field("range", v)?;
                }
                window_frame::Bounds::Session(v) => {
                    struct_ser.serialize_field("session", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrame {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "start",
            "end",
            "exclusion",
            "rows",
            "range",
            "session",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Start,
            End,
            Exclusion,
            Rows,
            Range,
            Session,
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
                            "type" => Ok(GeneratedField::Type),
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            "exclusion" => Ok(GeneratedField::Exclusion),
                            "rows" => Ok(GeneratedField::Rows),
                            "range" => Ok(GeneratedField::Range),
                            "session" => Ok(GeneratedField::Session),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrame;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowFrame, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut start__ = None;
                let mut end__ = None;
                let mut exclusion__ = None;
                let mut bounds__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<window_frame::Type>()? as i32);
                        }
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map_.next_value()?;
                        }
                        GeneratedField::Exclusion => {
                            if exclusion__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exclusion"));
                            }
                            exclusion__ = Some(map_.next_value::<window_frame::Exclusion>()? as i32);
                        }
                        GeneratedField::Rows => {
                            if bounds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rows"));
                            }
                            bounds__ = map_.next_value::<::std::option::Option<_>>()?.map(window_frame::Bounds::Rows)
;
                        }
                        GeneratedField::Range => {
                            if bounds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            bounds__ = map_.next_value::<::std::option::Option<_>>()?.map(window_frame::Bounds::Range)
;
                        }
                        GeneratedField::Session => {
                            if bounds__.is_some() {
                                return Err(serde::de::Error::duplicate_field("session"));
                            }
                            bounds__ = map_.next_value::<::std::option::Option<_>>()?.map(window_frame::Bounds::Session)
;
                        }
                    }
                }
                Ok(WindowFrame {
                    r#type: r#type__.unwrap_or_default(),
                    start: start__,
                    end: end__,
                    exclusion: exclusion__.unwrap_or_default(),
                    bounds: bounds__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::Bound {
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
        if self.offset.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.Bound", len)?;
        if self.r#type != 0 {
            let v = window_frame::BoundType::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.offset.as_ref() {
            match v {
                window_frame::bound::Offset::Integer(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("integer", ToString::to_string(&v).as_str())?;
                }
                window_frame::bound::Offset::Datum(v) => {
                    struct_ser.serialize_field("datum", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::Bound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "integer",
            "datum",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
            Integer,
            Datum,
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
                            "type" => Ok(GeneratedField::Type),
                            "integer" => Ok(GeneratedField::Integer),
                            "datum" => Ok(GeneratedField::Datum),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::Bound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.Bound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::Bound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<window_frame::BoundType>()? as i32);
                        }
                        GeneratedField::Integer => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("integer"));
                            }
                            offset__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| window_frame::bound::Offset::Integer(x.0));
                        }
                        GeneratedField::Datum => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("datum"));
                            }
                            offset__ = map_.next_value::<::std::option::Option<_>>()?.map(window_frame::bound::Offset::Datum)
;
                        }
                    }
                }
                Ok(window_frame::Bound {
                    r#type: r#type__.unwrap_or_default(),
                    offset: offset__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.Bound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::BoundType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "BOUND_TYPE_UNSPECIFIED",
            Self::UnboundedPreceding => "BOUND_TYPE_UNBOUNDED_PRECEDING",
            Self::Preceding => "BOUND_TYPE_PRECEDING",
            Self::CurrentRow => "BOUND_TYPE_CURRENT_ROW",
            Self::Following => "BOUND_TYPE_FOLLOWING",
            Self::UnboundedFollowing => "BOUND_TYPE_UNBOUNDED_FOLLOWING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::BoundType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "BOUND_TYPE_UNSPECIFIED",
            "BOUND_TYPE_UNBOUNDED_PRECEDING",
            "BOUND_TYPE_PRECEDING",
            "BOUND_TYPE_CURRENT_ROW",
            "BOUND_TYPE_FOLLOWING",
            "BOUND_TYPE_UNBOUNDED_FOLLOWING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::BoundType;

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
                    "BOUND_TYPE_UNSPECIFIED" => Ok(window_frame::BoundType::Unspecified),
                    "BOUND_TYPE_UNBOUNDED_PRECEDING" => Ok(window_frame::BoundType::UnboundedPreceding),
                    "BOUND_TYPE_PRECEDING" => Ok(window_frame::BoundType::Preceding),
                    "BOUND_TYPE_CURRENT_ROW" => Ok(window_frame::BoundType::CurrentRow),
                    "BOUND_TYPE_FOLLOWING" => Ok(window_frame::BoundType::Following),
                    "BOUND_TYPE_UNBOUNDED_FOLLOWING" => Ok(window_frame::BoundType::UnboundedFollowing),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::Exclusion {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "EXCLUSION_UNSPECIFIED",
            Self::CurrentRow => "EXCLUSION_CURRENT_ROW",
            Self::NoOthers => "EXCLUSION_NO_OTHERS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::Exclusion {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "EXCLUSION_UNSPECIFIED",
            "EXCLUSION_CURRENT_ROW",
            "EXCLUSION_NO_OTHERS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::Exclusion;

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
                    "EXCLUSION_UNSPECIFIED" => Ok(window_frame::Exclusion::Unspecified),
                    "EXCLUSION_CURRENT_ROW" => Ok(window_frame::Exclusion::CurrentRow),
                    "EXCLUSION_NO_OTHERS" => Ok(window_frame::Exclusion::NoOthers),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::RangeFrameBound {
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
        if self.offset.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.RangeFrameBound", len)?;
        if self.r#type != 0 {
            let v = window_frame::BoundType::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.offset.as_ref() {
            struct_ser.serialize_field("offset", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::RangeFrameBound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
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
            type Value = window_frame::RangeFrameBound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.RangeFrameBound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::RangeFrameBound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<window_frame::BoundType>()? as i32);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = map_.next_value()?;
                        }
                    }
                }
                Ok(window_frame::RangeFrameBound {
                    r#type: r#type__.unwrap_or_default(),
                    offset: offset__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.RangeFrameBound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::RangeFrameBounds {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        if self.order_data_type.is_some() {
            len += 1;
        }
        if self.order_type.is_some() {
            len += 1;
        }
        if self.offset_data_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.RangeFrameBounds", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        if let Some(v) = self.order_data_type.as_ref() {
            struct_ser.serialize_field("orderDataType", v)?;
        }
        if let Some(v) = self.order_type.as_ref() {
            struct_ser.serialize_field("orderType", v)?;
        }
        if let Some(v) = self.offset_data_type.as_ref() {
            struct_ser.serialize_field("offsetDataType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::RangeFrameBounds {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
            "order_data_type",
            "orderDataType",
            "order_type",
            "orderType",
            "offset_data_type",
            "offsetDataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
            OrderDataType,
            OrderType,
            OffsetDataType,
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
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            "orderDataType" | "order_data_type" => Ok(GeneratedField::OrderDataType),
                            "orderType" | "order_type" => Ok(GeneratedField::OrderType),
                            "offsetDataType" | "offset_data_type" => Ok(GeneratedField::OffsetDataType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::RangeFrameBounds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.RangeFrameBounds")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::RangeFrameBounds, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                let mut order_data_type__ = None;
                let mut order_type__ = None;
                let mut offset_data_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map_.next_value()?;
                        }
                        GeneratedField::OrderDataType => {
                            if order_data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderDataType"));
                            }
                            order_data_type__ = map_.next_value()?;
                        }
                        GeneratedField::OrderType => {
                            if order_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderType"));
                            }
                            order_type__ = map_.next_value()?;
                        }
                        GeneratedField::OffsetDataType => {
                            if offset_data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offsetDataType"));
                            }
                            offset_data_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(window_frame::RangeFrameBounds {
                    start: start__,
                    end: end__,
                    order_data_type: order_data_type__,
                    order_type: order_type__,
                    offset_data_type: offset_data_type__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.RangeFrameBounds", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::RowsFrameBound {
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
        if self.offset.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.RowsFrameBound", len)?;
        if self.r#type != 0 {
            let v = window_frame::BoundType::try_from(self.r#type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.r#type)))?;
            struct_ser.serialize_field("type", &v)?;
        }
        if let Some(v) = self.offset.as_ref() {
            #[allow(clippy::needless_borrow)]
            #[allow(clippy::needless_borrows_for_generic_args)]
            struct_ser.serialize_field("offset", ToString::to_string(&v).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::RowsFrameBound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "type",
            "offset",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Type,
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

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "type" => Ok(GeneratedField::Type),
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
            type Value = window_frame::RowsFrameBound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.RowsFrameBound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::RowsFrameBound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut r#type__ = None;
                let mut offset__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value::<window_frame::BoundType>()? as i32);
                        }
                        GeneratedField::Offset => {
                            if offset__.is_some() {
                                return Err(serde::de::Error::duplicate_field("offset"));
                            }
                            offset__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(window_frame::RowsFrameBound {
                    r#type: r#type__.unwrap_or_default(),
                    offset: offset__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.RowsFrameBound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::RowsFrameBounds {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.end.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.RowsFrameBounds", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.end.as_ref() {
            struct_ser.serialize_field("end", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::RowsFrameBounds {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
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
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::RowsFrameBounds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.RowsFrameBounds")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::RowsFrameBounds, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = map_.next_value()?;
                        }
                    }
                }
                Ok(window_frame::RowsFrameBounds {
                    start: start__,
                    end: end__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.RowsFrameBounds", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::SessionFrameBounds {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.gap.is_some() {
            len += 1;
        }
        if self.order_data_type.is_some() {
            len += 1;
        }
        if self.order_type.is_some() {
            len += 1;
        }
        if self.gap_data_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFrame.SessionFrameBounds", len)?;
        if let Some(v) = self.gap.as_ref() {
            struct_ser.serialize_field("gap", v)?;
        }
        if let Some(v) = self.order_data_type.as_ref() {
            struct_ser.serialize_field("orderDataType", v)?;
        }
        if let Some(v) = self.order_type.as_ref() {
            struct_ser.serialize_field("orderType", v)?;
        }
        if let Some(v) = self.gap_data_type.as_ref() {
            struct_ser.serialize_field("gapDataType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::SessionFrameBounds {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "gap",
            "order_data_type",
            "orderDataType",
            "order_type",
            "orderType",
            "gap_data_type",
            "gapDataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Gap,
            OrderDataType,
            OrderType,
            GapDataType,
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
                            "gap" => Ok(GeneratedField::Gap),
                            "orderDataType" | "order_data_type" => Ok(GeneratedField::OrderDataType),
                            "orderType" | "order_type" => Ok(GeneratedField::OrderType),
                            "gapDataType" | "gap_data_type" => Ok(GeneratedField::GapDataType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::SessionFrameBounds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFrame.SessionFrameBounds")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<window_frame::SessionFrameBounds, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut gap__ = None;
                let mut order_data_type__ = None;
                let mut order_type__ = None;
                let mut gap_data_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Gap => {
                            if gap__.is_some() {
                                return Err(serde::de::Error::duplicate_field("gap"));
                            }
                            gap__ = map_.next_value()?;
                        }
                        GeneratedField::OrderDataType => {
                            if order_data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderDataType"));
                            }
                            order_data_type__ = map_.next_value()?;
                        }
                        GeneratedField::OrderType => {
                            if order_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderType"));
                            }
                            order_type__ = map_.next_value()?;
                        }
                        GeneratedField::GapDataType => {
                            if gap_data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("gapDataType"));
                            }
                            gap_data_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(window_frame::SessionFrameBounds {
                    gap: gap__,
                    order_data_type: order_data_type__,
                    order_type: order_type__,
                    gap_data_type: gap_data_type__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFrame.SessionFrameBounds", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_frame::Type {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "TYPE_UNSPECIFIED",
            Self::RowsLegacy => "TYPE_ROWS_LEGACY",
            Self::Rows => "TYPE_ROWS",
            Self::Range => "TYPE_RANGE",
            Self::Session => "TYPE_SESSION",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for window_frame::Type {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "TYPE_UNSPECIFIED",
            "TYPE_ROWS_LEGACY",
            "TYPE_ROWS",
            "TYPE_RANGE",
            "TYPE_SESSION",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_frame::Type;

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
                    "TYPE_UNSPECIFIED" => Ok(window_frame::Type::Unspecified),
                    "TYPE_ROWS_LEGACY" => Ok(window_frame::Type::RowsLegacy),
                    "TYPE_ROWS" => Ok(window_frame::Type::Rows),
                    "TYPE_RANGE" => Ok(window_frame::Type::Range),
                    "TYPE_SESSION" => Ok(window_frame::Type::Session),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.args.is_empty() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        if self.frame.is_some() {
            len += 1;
        }
        if self.ignore_nulls {
            len += 1;
        }
        if self.r#type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("expr.WindowFunction", len)?;
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        if let Some(v) = self.frame.as_ref() {
            struct_ser.serialize_field("frame", v)?;
        }
        if self.ignore_nulls {
            struct_ser.serialize_field("ignoreNulls", &self.ignore_nulls)?;
        }
        if let Some(v) = self.r#type.as_ref() {
            match v {
                window_function::Type::General(v) => {
                    let v = window_function::GeneralType::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("general", &v)?;
                }
                window_function::Type::Aggregate(v) => {
                    let v = agg_call::Kind::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggregate", &v)?;
                }
                window_function::Type::Aggregate2(v) => {
                    struct_ser.serialize_field("aggregate2", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "args",
            "return_type",
            "returnType",
            "frame",
            "ignore_nulls",
            "ignoreNulls",
            "general",
            "aggregate",
            "aggregate2",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Args,
            ReturnType,
            Frame,
            IgnoreNulls,
            General,
            Aggregate,
            Aggregate2,
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
                            "args" => Ok(GeneratedField::Args),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            "frame" => Ok(GeneratedField::Frame),
                            "ignoreNulls" | "ignore_nulls" => Ok(GeneratedField::IgnoreNulls),
                            "general" => Ok(GeneratedField::General),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "aggregate2" => Ok(GeneratedField::Aggregate2),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct expr.WindowFunction")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowFunction, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut args__ = None;
                let mut return_type__ = None;
                let mut frame__ = None;
                let mut ignore_nulls__ = None;
                let mut r#type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                        GeneratedField::Frame => {
                            if frame__.is_some() {
                                return Err(serde::de::Error::duplicate_field("frame"));
                            }
                            frame__ = map_.next_value()?;
                        }
                        GeneratedField::IgnoreNulls => {
                            if ignore_nulls__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ignoreNulls"));
                            }
                            ignore_nulls__ = Some(map_.next_value()?);
                        }
                        GeneratedField::General => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("general"));
                            }
                            r#type__ = map_.next_value::<::std::option::Option<window_function::GeneralType>>()?.map(|x| window_function::Type::General(x as i32));
                        }
                        GeneratedField::Aggregate => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            r#type__ = map_.next_value::<::std::option::Option<agg_call::Kind>>()?.map(|x| window_function::Type::Aggregate(x as i32));
                        }
                        GeneratedField::Aggregate2 => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate2"));
                            }
                            r#type__ = map_.next_value::<::std::option::Option<_>>()?.map(window_function::Type::Aggregate2)
;
                        }
                    }
                }
                Ok(WindowFunction {
                    args: args__.unwrap_or_default(),
                    return_type: return_type__,
                    frame: frame__,
                    ignore_nulls: ignore_nulls__.unwrap_or_default(),
                    r#type: r#type__,
                })
            }
        }
        deserializer.deserialize_struct("expr.WindowFunction", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for window_function::GeneralType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::RowNumber => "ROW_NUMBER",
            Self::Rank => "RANK",
            Self::DenseRank => "DENSE_RANK",
            Self::Lag => "LAG",
            Self::Lead => "LEAD",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for window_function::GeneralType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "UNSPECIFIED",
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "LAG",
            "LEAD",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = window_function::GeneralType;

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
                    "UNSPECIFIED" => Ok(window_function::GeneralType::Unspecified),
                    "ROW_NUMBER" => Ok(window_function::GeneralType::RowNumber),
                    "RANK" => Ok(window_function::GeneralType::Rank),
                    "DENSE_RANK" => Ok(window_function::GeneralType::DenseRank),
                    "LAG" => Ok(window_function::GeneralType::Lag),
                    "LEAD" => Ok(window_function::GeneralType::Lead),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
