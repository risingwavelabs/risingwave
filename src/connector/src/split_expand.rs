pub mod split {
    use std::marker::PhantomData;
    use anyhow::anyhow;
    use risingwave_common::types::JsonbVal;
    use serde::{Deserialize, Serialize};
    use crate::source::cdc::external::DebeziumOffset;
    use crate::source::cdc::{CdcSourceType, CdcSourceTypeTrait};
    use crate::source::{SplitId, SplitMetaData};
    trait CdcSplitTrait: Send + Sync {
        fn split_id(&self) -> u32;
        fn start_offset(&self) -> &Option<String>;
        fn snapshot_done(&self) -> bool;
        fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()>;
    }
    /// The base states of a CDC split, which will be persisted to checkpoint.
    /// CDC source only has single split, so we use the `source_id` to identify the split.
    pub struct CdcSplitBase {
        pub split_id: u32,
        pub start_offset: Option<String>,
        pub snapshot_done: bool,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for CdcSplitBase {
        #[inline]
        fn clone(&self) -> CdcSplitBase {
            CdcSplitBase {
                split_id: ::core::clone::Clone::clone(&self.split_id),
                start_offset: ::core::clone::Clone::clone(&self.start_offset),
                snapshot_done: ::core::clone::Clone::clone(&self.snapshot_done),
            }
        }
    }
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl _serde::Serialize for CdcSplitBase {
            fn serialize<__S>(
                &self,
                __serializer: __S,
            ) -> _serde::__private::Result<__S::Ok, __S::Error>
            where
                __S: _serde::Serializer,
            {
                let mut __serde_state = _serde::Serializer::serialize_struct(
                    __serializer,
                    "CdcSplitBase",
                    false as usize + 1 + 1 + 1,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "split_id",
                    &self.split_id,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "start_offset",
                    &self.start_offset,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "snapshot_done",
                    &self.snapshot_done,
                )?;
                _serde::ser::SerializeStruct::end(__serde_state)
            }
        }
    };
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<'de> _serde::Deserialize<'de> for CdcSplitBase {
            fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, __D::Error>
            where
                __D: _serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                enum __Field {
                    __field0,
                    __field1,
                    __field2,
                    __ignore,
                }
                #[doc(hidden)]
                struct __FieldVisitor;
                impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                    type Value = __Field;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "field identifier")
                    }
                    fn visit_u64<__E>(
                        self,
                        __value: u64,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            0u64 => _serde::__private::Ok(__Field::__field0),
                            1u64 => _serde::__private::Ok(__Field::__field1),
                            2u64 => _serde::__private::Ok(__Field::__field2),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_str<__E>(
                        self,
                        __value: &str,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            "split_id" => _serde::__private::Ok(__Field::__field0),
                            "start_offset" => _serde::__private::Ok(__Field::__field1),
                            "snapshot_done" => _serde::__private::Ok(__Field::__field2),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_bytes<__E>(
                        self,
                        __value: &[u8],
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            b"split_id" => _serde::__private::Ok(__Field::__field0),
                            b"start_offset" => _serde::__private::Ok(__Field::__field1),
                            b"snapshot_done" => _serde::__private::Ok(__Field::__field2),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                }
                impl<'de> _serde::Deserialize<'de> for __Field {
                    #[inline]
                    fn deserialize<__D>(
                        __deserializer: __D,
                    ) -> _serde::__private::Result<Self, __D::Error>
                    where
                        __D: _serde::Deserializer<'de>,
                    {
                        _serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
                    }
                }
                #[doc(hidden)]
                struct __Visitor<'de> {
                    marker: _serde::__private::PhantomData<CdcSplitBase>,
                    lifetime: _serde::__private::PhantomData<&'de ()>,
                }
                impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                    type Value = CdcSplitBase;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "struct CdcSplitBase")
                    }
                    #[inline]
                    fn visit_seq<__A>(
                        self,
                        mut __seq: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::SeqAccess<'de>,
                    {
                        let __field0 = match _serde::de::SeqAccess::next_element::<u32>(&mut __seq)?
                        {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    0usize,
                                    &"struct CdcSplitBase with 3 elements",
                                ))
                            }
                        };
                        let __field1 = match _serde::de::SeqAccess::next_element::<Option<String>>(
                            &mut __seq,
                        )? {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    1usize,
                                    &"struct CdcSplitBase with 3 elements",
                                ))
                            }
                        };
                        let __field2 =
                            match _serde::de::SeqAccess::next_element::<bool>(&mut __seq)? {
                                _serde::__private::Some(__value) => __value,
                                _serde::__private::None => {
                                    return _serde::__private::Err(
                                        _serde::de::Error::invalid_length(
                                            2usize,
                                            &"struct CdcSplitBase with 3 elements",
                                        ),
                                    )
                                }
                            };
                        _serde::__private::Ok(CdcSplitBase {
                            split_id: __field0,
                            start_offset: __field1,
                            snapshot_done: __field2,
                        })
                    }
                    #[inline]
                    fn visit_map<__A>(
                        self,
                        mut __map: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::MapAccess<'de>,
                    {
                        let mut __field0: _serde::__private::Option<u32> = _serde::__private::None;
                        let mut __field1: _serde::__private::Option<Option<String>> =
                            _serde::__private::None;
                        let mut __field2: _serde::__private::Option<bool> = _serde::__private::None;
                        while let _serde::__private::Some(__key) =
                            _serde::de::MapAccess::next_key::<__Field>(&mut __map)?
                        {
                            match __key {
                                __Field::__field0 => {
                                    if _serde::__private::Option::is_some(&__field0) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "split_id",
                                            ),
                                        );
                                    }
                                    __field0 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<u32>(&mut __map)?,
                                    );
                                }
                                __Field::__field1 => {
                                    if _serde::__private::Option::is_some(&__field1) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "start_offset",
                                            ),
                                        );
                                    }
                                    __field1 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<Option<String>>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                __Field::__field2 => {
                                    if _serde::__private::Option::is_some(&__field2) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "snapshot_done",
                                            ),
                                        );
                                    }
                                    __field2 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<bool>(&mut __map)?,
                                    );
                                }
                                _ => {
                                    let _ = _serde::de::MapAccess::next_value::<
                                        _serde::de::IgnoredAny,
                                    >(&mut __map)?;
                                }
                            }
                        }
                        let __field0 = match __field0 {
                            _serde::__private::Some(__field0) => __field0,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("split_id")?
                            }
                        };
                        let __field1 = match __field1 {
                            _serde::__private::Some(__field1) => __field1,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("start_offset")?
                            }
                        };
                        let __field2 = match __field2 {
                            _serde::__private::Some(__field2) => __field2,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("snapshot_done")?
                            }
                        };
                        _serde::__private::Ok(CdcSplitBase {
                            split_id: __field0,
                            start_offset: __field1,
                            snapshot_done: __field2,
                        })
                    }
                }
                #[doc(hidden)]
                const FIELDS: &'static [&'static str] =
                    &["split_id", "start_offset", "snapshot_done"];
                _serde::Deserializer::deserialize_struct(
                    __deserializer,
                    "CdcSplitBase",
                    FIELDS,
                    __Visitor {
                        marker: _serde::__private::PhantomData::<CdcSplitBase>,
                        lifetime: _serde::__private::PhantomData,
                    },
                )
            }
        }
    };
    #[automatically_derived]
    impl ::core::fmt::Debug for CdcSplitBase {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field3_finish(
                f,
                "CdcSplitBase",
                "split_id",
                &self.split_id,
                "start_offset",
                &self.start_offset,
                "snapshot_done",
                &&self.snapshot_done,
            )
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for CdcSplitBase {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for CdcSplitBase {
        #[inline]
        fn eq(&self, other: &CdcSplitBase) -> bool {
            self.split_id == other.split_id
                && self.start_offset == other.start_offset
                && self.snapshot_done == other.snapshot_done
        }
    }
    #[automatically_derived]
    impl ::core::hash::Hash for CdcSplitBase {
        #[inline]
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            ::core::hash::Hash::hash(&self.split_id, state);
            ::core::hash::Hash::hash(&self.start_offset, state);
            ::core::hash::Hash::hash(&self.snapshot_done, state)
        }
    }
    impl CdcSplitBase {
        pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
            Self {
                split_id,
                start_offset,
                snapshot_done: false,
            }
        }
    }
    pub struct MySqlCdcSplit {
        pub inner: CdcSplitBase,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for MySqlCdcSplit {
        #[inline]
        fn clone(&self) -> MySqlCdcSplit {
            MySqlCdcSplit {
                inner: ::core::clone::Clone::clone(&self.inner),
            }
        }
    }
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl _serde::Serialize for MySqlCdcSplit {
            fn serialize<__S>(
                &self,
                __serializer: __S,
            ) -> _serde::__private::Result<__S::Ok, __S::Error>
            where
                __S: _serde::Serializer,
            {
                let mut __serde_state = _serde::Serializer::serialize_struct(
                    __serializer,
                    "MySqlCdcSplit",
                    false as usize + 1,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "inner",
                    &self.inner,
                )?;
                _serde::ser::SerializeStruct::end(__serde_state)
            }
        }
    };
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<'de> _serde::Deserialize<'de> for MySqlCdcSplit {
            fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, __D::Error>
            where
                __D: _serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                enum __Field {
                    __field0,
                    __ignore,
                }
                #[doc(hidden)]
                struct __FieldVisitor;
                impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                    type Value = __Field;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "field identifier")
                    }
                    fn visit_u64<__E>(
                        self,
                        __value: u64,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            0u64 => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_str<__E>(
                        self,
                        __value: &str,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            "inner" => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_bytes<__E>(
                        self,
                        __value: &[u8],
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            b"inner" => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                }
                impl<'de> _serde::Deserialize<'de> for __Field {
                    #[inline]
                    fn deserialize<__D>(
                        __deserializer: __D,
                    ) -> _serde::__private::Result<Self, __D::Error>
                    where
                        __D: _serde::Deserializer<'de>,
                    {
                        _serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
                    }
                }
                #[doc(hidden)]
                struct __Visitor<'de> {
                    marker: _serde::__private::PhantomData<MySqlCdcSplit>,
                    lifetime: _serde::__private::PhantomData<&'de ()>,
                }
                impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                    type Value = MySqlCdcSplit;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "struct MySqlCdcSplit")
                    }
                    #[inline]
                    fn visit_seq<__A>(
                        self,
                        mut __seq: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::SeqAccess<'de>,
                    {
                        let __field0 = match _serde::de::SeqAccess::next_element::<CdcSplitBase>(
                            &mut __seq,
                        )? {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    0usize,
                                    &"struct MySqlCdcSplit with 1 element",
                                ))
                            }
                        };
                        _serde::__private::Ok(MySqlCdcSplit { inner: __field0 })
                    }
                    #[inline]
                    fn visit_map<__A>(
                        self,
                        mut __map: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::MapAccess<'de>,
                    {
                        let mut __field0: _serde::__private::Option<CdcSplitBase> =
                            _serde::__private::None;
                        while let _serde::__private::Some(__key) =
                            _serde::de::MapAccess::next_key::<__Field>(&mut __map)?
                        {
                            match __key {
                                __Field::__field0 => {
                                    if _serde::__private::Option::is_some(&__field0) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "inner",
                                            ),
                                        );
                                    }
                                    __field0 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<CdcSplitBase>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                _ => {
                                    let _ = _serde::de::MapAccess::next_value::<
                                        _serde::de::IgnoredAny,
                                    >(&mut __map)?;
                                }
                            }
                        }
                        let __field0 = match __field0 {
                            _serde::__private::Some(__field0) => __field0,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("inner")?
                            }
                        };
                        _serde::__private::Ok(MySqlCdcSplit { inner: __field0 })
                    }
                }
                #[doc(hidden)]
                const FIELDS: &'static [&'static str] = &["inner"];
                _serde::Deserializer::deserialize_struct(
                    __deserializer,
                    "MySqlCdcSplit",
                    FIELDS,
                    __Visitor {
                        marker: _serde::__private::PhantomData::<MySqlCdcSplit>,
                        lifetime: _serde::__private::PhantomData,
                    },
                )
            }
        }
    };
    #[automatically_derived]
    impl ::core::fmt::Debug for MySqlCdcSplit {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field1_finish(
                f,
                "MySqlCdcSplit",
                "inner",
                &&self.inner,
            )
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for MySqlCdcSplit {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for MySqlCdcSplit {
        #[inline]
        fn eq(&self, other: &MySqlCdcSplit) -> bool {
            self.inner == other.inner
        }
    }
    #[automatically_derived]
    impl ::core::hash::Hash for MySqlCdcSplit {
        #[inline]
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            ::core::hash::Hash::hash(&self.inner, state)
        }
    }
    pub struct PostgresCdcSplit {
        pub inner: CdcSplitBase,
        pub server_addr: Option<String>,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for PostgresCdcSplit {
        #[inline]
        fn clone(&self) -> PostgresCdcSplit {
            PostgresCdcSplit {
                inner: ::core::clone::Clone::clone(&self.inner),
                server_addr: ::core::clone::Clone::clone(&self.server_addr),
            }
        }
    }
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl _serde::Serialize for PostgresCdcSplit {
            fn serialize<__S>(
                &self,
                __serializer: __S,
            ) -> _serde::__private::Result<__S::Ok, __S::Error>
            where
                __S: _serde::Serializer,
            {
                let mut __serde_state = _serde::Serializer::serialize_struct(
                    __serializer,
                    "PostgresCdcSplit",
                    false as usize + 1 + 1,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "inner",
                    &self.inner,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "server_addr",
                    &self.server_addr,
                )?;
                _serde::ser::SerializeStruct::end(__serde_state)
            }
        }
    };
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<'de> _serde::Deserialize<'de> for PostgresCdcSplit {
            fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, __D::Error>
            where
                __D: _serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                enum __Field {
                    __field0,
                    __field1,
                    __ignore,
                }
                #[doc(hidden)]
                struct __FieldVisitor;
                impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                    type Value = __Field;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "field identifier")
                    }
                    fn visit_u64<__E>(
                        self,
                        __value: u64,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            0u64 => _serde::__private::Ok(__Field::__field0),
                            1u64 => _serde::__private::Ok(__Field::__field1),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_str<__E>(
                        self,
                        __value: &str,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            "inner" => _serde::__private::Ok(__Field::__field0),
                            "server_addr" => _serde::__private::Ok(__Field::__field1),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_bytes<__E>(
                        self,
                        __value: &[u8],
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            b"inner" => _serde::__private::Ok(__Field::__field0),
                            b"server_addr" => _serde::__private::Ok(__Field::__field1),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                }
                impl<'de> _serde::Deserialize<'de> for __Field {
                    #[inline]
                    fn deserialize<__D>(
                        __deserializer: __D,
                    ) -> _serde::__private::Result<Self, __D::Error>
                    where
                        __D: _serde::Deserializer<'de>,
                    {
                        _serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
                    }
                }
                #[doc(hidden)]
                struct __Visitor<'de> {
                    marker: _serde::__private::PhantomData<PostgresCdcSplit>,
                    lifetime: _serde::__private::PhantomData<&'de ()>,
                }
                impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                    type Value = PostgresCdcSplit;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(
                            __formatter,
                            "struct PostgresCdcSplit",
                        )
                    }
                    #[inline]
                    fn visit_seq<__A>(
                        self,
                        mut __seq: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::SeqAccess<'de>,
                    {
                        let __field0 = match _serde::de::SeqAccess::next_element::<CdcSplitBase>(
                            &mut __seq,
                        )? {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    0usize,
                                    &"struct PostgresCdcSplit with 2 elements",
                                ))
                            }
                        };
                        let __field1 = match _serde::de::SeqAccess::next_element::<Option<String>>(
                            &mut __seq,
                        )? {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    1usize,
                                    &"struct PostgresCdcSplit with 2 elements",
                                ))
                            }
                        };
                        _serde::__private::Ok(PostgresCdcSplit {
                            inner: __field0,
                            server_addr: __field1,
                        })
                    }
                    #[inline]
                    fn visit_map<__A>(
                        self,
                        mut __map: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::MapAccess<'de>,
                    {
                        let mut __field0: _serde::__private::Option<CdcSplitBase> =
                            _serde::__private::None;
                        let mut __field1: _serde::__private::Option<Option<String>> =
                            _serde::__private::None;
                        while let _serde::__private::Some(__key) =
                            _serde::de::MapAccess::next_key::<__Field>(&mut __map)?
                        {
                            match __key {
                                __Field::__field0 => {
                                    if _serde::__private::Option::is_some(&__field0) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "inner",
                                            ),
                                        );
                                    }
                                    __field0 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<CdcSplitBase>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                __Field::__field1 => {
                                    if _serde::__private::Option::is_some(&__field1) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "server_addr",
                                            ),
                                        );
                                    }
                                    __field1 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<Option<String>>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                _ => {
                                    let _ = _serde::de::MapAccess::next_value::<
                                        _serde::de::IgnoredAny,
                                    >(&mut __map)?;
                                }
                            }
                        }
                        let __field0 = match __field0 {
                            _serde::__private::Some(__field0) => __field0,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("inner")?
                            }
                        };
                        let __field1 = match __field1 {
                            _serde::__private::Some(__field1) => __field1,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("server_addr")?
                            }
                        };
                        _serde::__private::Ok(PostgresCdcSplit {
                            inner: __field0,
                            server_addr: __field1,
                        })
                    }
                }
                #[doc(hidden)]
                const FIELDS: &'static [&'static str] = &["inner", "server_addr"];
                _serde::Deserializer::deserialize_struct(
                    __deserializer,
                    "PostgresCdcSplit",
                    FIELDS,
                    __Visitor {
                        marker: _serde::__private::PhantomData::<PostgresCdcSplit>,
                        lifetime: _serde::__private::PhantomData,
                    },
                )
            }
        }
    };
    #[automatically_derived]
    impl ::core::fmt::Debug for PostgresCdcSplit {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field2_finish(
                f,
                "PostgresCdcSplit",
                "inner",
                &self.inner,
                "server_addr",
                &&self.server_addr,
            )
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for PostgresCdcSplit {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for PostgresCdcSplit {
        #[inline]
        fn eq(&self, other: &PostgresCdcSplit) -> bool {
            self.inner == other.inner && self.server_addr == other.server_addr
        }
    }
    #[automatically_derived]
    impl ::core::hash::Hash for PostgresCdcSplit {
        #[inline]
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            ::core::hash::Hash::hash(&self.inner, state);
            ::core::hash::Hash::hash(&self.server_addr, state)
        }
    }
    pub struct MongoDbCdcSplit {
        pub inner: CdcSplitBase,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for MongoDbCdcSplit {
        #[inline]
        fn clone(&self) -> MongoDbCdcSplit {
            MongoDbCdcSplit {
                inner: ::core::clone::Clone::clone(&self.inner),
            }
        }
    }
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl _serde::Serialize for MongoDbCdcSplit {
            fn serialize<__S>(
                &self,
                __serializer: __S,
            ) -> _serde::__private::Result<__S::Ok, __S::Error>
            where
                __S: _serde::Serializer,
            {
                let mut __serde_state = _serde::Serializer::serialize_struct(
                    __serializer,
                    "MongoDbCdcSplit",
                    false as usize + 1,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "inner",
                    &self.inner,
                )?;
                _serde::ser::SerializeStruct::end(__serde_state)
            }
        }
    };
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<'de> _serde::Deserialize<'de> for MongoDbCdcSplit {
            fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, __D::Error>
            where
                __D: _serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                enum __Field {
                    __field0,
                    __ignore,
                }
                #[doc(hidden)]
                struct __FieldVisitor;
                impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                    type Value = __Field;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "field identifier")
                    }
                    fn visit_u64<__E>(
                        self,
                        __value: u64,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            0u64 => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_str<__E>(
                        self,
                        __value: &str,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            "inner" => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_bytes<__E>(
                        self,
                        __value: &[u8],
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            b"inner" => _serde::__private::Ok(__Field::__field0),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                }
                impl<'de> _serde::Deserialize<'de> for __Field {
                    #[inline]
                    fn deserialize<__D>(
                        __deserializer: __D,
                    ) -> _serde::__private::Result<Self, __D::Error>
                    where
                        __D: _serde::Deserializer<'de>,
                    {
                        _serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
                    }
                }
                #[doc(hidden)]
                struct __Visitor<'de> {
                    marker: _serde::__private::PhantomData<MongoDbCdcSplit>,
                    lifetime: _serde::__private::PhantomData<&'de ()>,
                }
                impl<'de> _serde::de::Visitor<'de> for __Visitor<'de> {
                    type Value = MongoDbCdcSplit;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(
                            __formatter,
                            "struct MongoDbCdcSplit",
                        )
                    }
                    #[inline]
                    fn visit_seq<__A>(
                        self,
                        mut __seq: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::SeqAccess<'de>,
                    {
                        let __field0 = match _serde::de::SeqAccess::next_element::<CdcSplitBase>(
                            &mut __seq,
                        )? {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    0usize,
                                    &"struct MongoDbCdcSplit with 1 element",
                                ))
                            }
                        };
                        _serde::__private::Ok(MongoDbCdcSplit { inner: __field0 })
                    }
                    #[inline]
                    fn visit_map<__A>(
                        self,
                        mut __map: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::MapAccess<'de>,
                    {
                        let mut __field0: _serde::__private::Option<CdcSplitBase> =
                            _serde::__private::None;
                        while let _serde::__private::Some(__key) =
                            _serde::de::MapAccess::next_key::<__Field>(&mut __map)?
                        {
                            match __key {
                                __Field::__field0 => {
                                    if _serde::__private::Option::is_some(&__field0) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "inner",
                                            ),
                                        );
                                    }
                                    __field0 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<CdcSplitBase>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                _ => {
                                    let _ = _serde::de::MapAccess::next_value::<
                                        _serde::de::IgnoredAny,
                                    >(&mut __map)?;
                                }
                            }
                        }
                        let __field0 = match __field0 {
                            _serde::__private::Some(__field0) => __field0,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("inner")?
                            }
                        };
                        _serde::__private::Ok(MongoDbCdcSplit { inner: __field0 })
                    }
                }
                #[doc(hidden)]
                const FIELDS: &'static [&'static str] = &["inner"];
                _serde::Deserializer::deserialize_struct(
                    __deserializer,
                    "MongoDbCdcSplit",
                    FIELDS,
                    __Visitor {
                        marker: _serde::__private::PhantomData::<MongoDbCdcSplit>,
                        lifetime: _serde::__private::PhantomData,
                    },
                )
            }
        }
    };
    #[automatically_derived]
    impl ::core::fmt::Debug for MongoDbCdcSplit {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field1_finish(
                f,
                "MongoDbCdcSplit",
                "inner",
                &&self.inner,
            )
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for MongoDbCdcSplit {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for MongoDbCdcSplit {
        #[inline]
        fn eq(&self, other: &MongoDbCdcSplit) -> bool {
            self.inner == other.inner
        }
    }
    #[automatically_derived]
    impl ::core::hash::Hash for MongoDbCdcSplit {
        #[inline]
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            ::core::hash::Hash::hash(&self.inner, state)
        }
    }
    impl MySqlCdcSplit {
        pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
            let split = CdcSplitBase {
                split_id,
                start_offset,
                snapshot_done: false,
            };
            Self { inner: split }
        }
    }
    impl CdcSplitTrait for MySqlCdcSplit {
        fn split_id(&self) -> u32 {
            self.inner.split_id
        }
        fn start_offset(&self) -> &Option<String> {
            &self.inner.start_offset
        }
        fn snapshot_done(&self) -> bool {
            self.inner.snapshot_done
        }
        fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
            let mut snapshot_done = self.inner.snapshot_done;
            if !snapshot_done {
                let dbz_offset: DebeziumOffset =
                    serde_json::from_str(&start_offset).map_err(|e| {
                        ::anyhow::Error::msg({
                            let res = ::alloc::fmt::format(format_args!(
                                "invalid mysql offset: {0}, error: {1}, split: {2}",
                                start_offset, e, self.inner.split_id
                            ));
                            res
                        })
                    })?;
                if !dbz_offset.is_heartbeat {
                    snapshot_done = match dbz_offset.source_offset.snapshot {
                        Some(val) => !val,
                        None => true,
                    };
                }
            }
            self.inner.start_offset = Some(start_offset);
            self.inner.snapshot_done = snapshot_done;
            Ok(())
        }
    }
    impl PostgresCdcSplit {
        pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
            let split = CdcSplitBase {
                split_id,
                start_offset,
                snapshot_done: false,
            };
            Self {
                inner: split,
                server_addr: None,
            }
        }
        pub fn new_with_server_addr(
            split_id: u32,
            start_offset: Option<String>,
            server_addr: Option<String>,
        ) -> Self {
            let mut result = Self::new(split_id, start_offset);
            result.server_addr = server_addr;
            result
        }
    }
    impl CdcSplitTrait for PostgresCdcSplit {
        fn split_id(&self) -> u32 {
            self.inner.split_id
        }
        fn start_offset(&self) -> &Option<String> {
            &self.inner.start_offset
        }
        fn snapshot_done(&self) -> bool {
            self.inner.snapshot_done
        }
        fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
            let mut snapshot_done = self.inner.snapshot_done;
            if !snapshot_done {
                let dbz_offset: DebeziumOffset =
                    serde_json::from_str(&start_offset).map_err(|e| {
                        ::anyhow::Error::msg({
                            let res = ::alloc::fmt::format(format_args!(
                                "invalid postgres offset: {0}, error: {1}, split: {2}",
                                start_offset, e, self.inner.split_id
                            ));
                            res
                        })
                    })?;
                if !dbz_offset.is_heartbeat {
                    snapshot_done = dbz_offset
                        .source_offset
                        .last_snapshot_record
                        .unwrap_or(false);
                }
            }
            self.inner.start_offset = Some(start_offset);
            self.inner.snapshot_done = snapshot_done;
            Ok(())
        }
    }
    impl MongoDbCdcSplit {
        pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
            let split = CdcSplitBase {
                split_id,
                start_offset,
                snapshot_done: false,
            };
            Self { inner: split }
        }
        pub fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
            self.inner.start_offset = Some(start_offset);
            Ok(())
        }
    }
    impl CdcSplitTrait for MongoDbCdcSplit {
        fn split_id(&self) -> u32 {
            self.inner.split_id
        }
        fn start_offset(&self) -> &Option<String> {
            &self.inner.start_offset
        }
        fn snapshot_done(&self) -> bool {
            self.inner.snapshot_done
        }
        fn update_with_offset(&mut self, _start_offset: String) -> anyhow::Result<()> {
            Ok(())
        }
    }
    pub struct DebeziumCdcSplit<T: CdcSourceTypeTrait> {
        pub mysql_split: Option<MySqlCdcSplit>,
        #[serde(rename = "pg_split")]
        pub postgres_split: Option<PostgresCdcSplit>,
        pub citus_split: Option<PostgresCdcSplit>,
        pub mongodb_split: Option<MongoDbCdcSplit>,
        #[serde(skip)]
        pub _phantom: PhantomData<T>,
    }
    #[automatically_derived]
    impl<T: ::core::clone::Clone + CdcSourceTypeTrait> ::core::clone::Clone for DebeziumCdcSplit<T> {
        #[inline]
        fn clone(&self) -> DebeziumCdcSplit<T> {
            DebeziumCdcSplit {
                mysql_split: ::core::clone::Clone::clone(&self.mysql_split),
                postgres_split: ::core::clone::Clone::clone(&self.postgres_split),
                citus_split: ::core::clone::Clone::clone(&self.citus_split),
                mongodb_split: ::core::clone::Clone::clone(&self.mongodb_split),
                _phantom: ::core::clone::Clone::clone(&self._phantom),
            }
        }
    }
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<T: CdcSourceTypeTrait> _serde::Serialize for DebeziumCdcSplit<T> {
            fn serialize<__S>(
                &self,
                __serializer: __S,
            ) -> _serde::__private::Result<__S::Ok, __S::Error>
            where
                __S: _serde::Serializer,
            {
                let mut __serde_state = _serde::Serializer::serialize_struct(
                    __serializer,
                    "DebeziumCdcSplit",
                    false as usize + 1 + 1 + 1 + 1,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "mysql_split",
                    &self.mysql_split,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "pg_split",
                    &self.postgres_split,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "citus_split",
                    &self.citus_split,
                )?;
                _serde::ser::SerializeStruct::serialize_field(
                    &mut __serde_state,
                    "mongodb_split",
                    &self.mongodb_split,
                )?;
                _serde::ser::SerializeStruct::end(__serde_state)
            }
        }
    };
    #[doc(hidden)]
    #[allow(non_upper_case_globals, unused_attributes, unused_qualifications)]
    const _: () = {
        #[allow(unused_extern_crates, clippy::useless_attribute)]
        extern crate serde as _serde;
        #[automatically_derived]
        impl<'de, T: CdcSourceTypeTrait> _serde::Deserialize<'de> for DebeziumCdcSplit<T> {
            fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, __D::Error>
            where
                __D: _serde::Deserializer<'de>,
            {
                #[allow(non_camel_case_types)]
                #[doc(hidden)]
                enum __Field {
                    __field0,
                    __field1,
                    __field2,
                    __field3,
                    __ignore,
                }
                #[doc(hidden)]
                struct __FieldVisitor;
                impl<'de> _serde::de::Visitor<'de> for __FieldVisitor {
                    type Value = __Field;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(__formatter, "field identifier")
                    }
                    fn visit_u64<__E>(
                        self,
                        __value: u64,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            0u64 => _serde::__private::Ok(__Field::__field0),
                            1u64 => _serde::__private::Ok(__Field::__field1),
                            2u64 => _serde::__private::Ok(__Field::__field2),
                            3u64 => _serde::__private::Ok(__Field::__field3),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_str<__E>(
                        self,
                        __value: &str,
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            "mysql_split" => _serde::__private::Ok(__Field::__field0),
                            "pg_split" => _serde::__private::Ok(__Field::__field1),
                            "citus_split" => _serde::__private::Ok(__Field::__field2),
                            "mongodb_split" => _serde::__private::Ok(__Field::__field3),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                    fn visit_bytes<__E>(
                        self,
                        __value: &[u8],
                    ) -> _serde::__private::Result<Self::Value, __E>
                    where
                        __E: _serde::de::Error,
                    {
                        match __value {
                            b"mysql_split" => _serde::__private::Ok(__Field::__field0),
                            b"pg_split" => _serde::__private::Ok(__Field::__field1),
                            b"citus_split" => _serde::__private::Ok(__Field::__field2),
                            b"mongodb_split" => _serde::__private::Ok(__Field::__field3),
                            _ => _serde::__private::Ok(__Field::__ignore),
                        }
                    }
                }
                impl<'de> _serde::Deserialize<'de> for __Field {
                    #[inline]
                    fn deserialize<__D>(
                        __deserializer: __D,
                    ) -> _serde::__private::Result<Self, __D::Error>
                    where
                        __D: _serde::Deserializer<'de>,
                    {
                        _serde::Deserializer::deserialize_identifier(__deserializer, __FieldVisitor)
                    }
                }
                #[doc(hidden)]
                struct __Visitor<'de, T: CdcSourceTypeTrait> {
                    marker: _serde::__private::PhantomData<DebeziumCdcSplit<T>>,
                    lifetime: _serde::__private::PhantomData<&'de ()>,
                }
                impl<'de, T: CdcSourceTypeTrait> _serde::de::Visitor<'de> for __Visitor<'de, T> {
                    type Value = DebeziumCdcSplit<T>;
                    fn expecting(
                        &self,
                        __formatter: &mut _serde::__private::Formatter,
                    ) -> _serde::__private::fmt::Result {
                        _serde::__private::Formatter::write_str(
                            __formatter,
                            "struct DebeziumCdcSplit",
                        )
                    }
                    #[inline]
                    fn visit_seq<__A>(
                        self,
                        mut __seq: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::SeqAccess<'de>,
                    {
                        let __field0 = match _serde::de::SeqAccess::next_element::<
                            Option<MySqlCdcSplit>,
                        >(&mut __seq)?
                        {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    0usize,
                                    &"struct DebeziumCdcSplit with 4 elements",
                                ))
                            }
                        };
                        let __field1 = match _serde::de::SeqAccess::next_element::<
                            Option<PostgresCdcSplit>,
                        >(&mut __seq)?
                        {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    1usize,
                                    &"struct DebeziumCdcSplit with 4 elements",
                                ))
                            }
                        };
                        let __field2 = match _serde::de::SeqAccess::next_element::<
                            Option<PostgresCdcSplit>,
                        >(&mut __seq)?
                        {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    2usize,
                                    &"struct DebeziumCdcSplit with 4 elements",
                                ))
                            }
                        };
                        let __field3 = match _serde::de::SeqAccess::next_element::<
                            Option<MongoDbCdcSplit>,
                        >(&mut __seq)?
                        {
                            _serde::__private::Some(__value) => __value,
                            _serde::__private::None => {
                                return _serde::__private::Err(_serde::de::Error::invalid_length(
                                    3usize,
                                    &"struct DebeziumCdcSplit with 4 elements",
                                ))
                            }
                        };
                        let __field4 = _serde::__private::Default::default();
                        _serde::__private::Ok(DebeziumCdcSplit {
                            mysql_split: __field0,
                            postgres_split: __field1,
                            citus_split: __field2,
                            mongodb_split: __field3,
                            _phantom: __field4,
                        })
                    }
                    #[inline]
                    fn visit_map<__A>(
                        self,
                        mut __map: __A,
                    ) -> _serde::__private::Result<Self::Value, __A::Error>
                    where
                        __A: _serde::de::MapAccess<'de>,
                    {
                        let mut __field0: _serde::__private::Option<Option<MySqlCdcSplit>> =
                            _serde::__private::None;
                        let mut __field1: _serde::__private::Option<Option<PostgresCdcSplit>> =
                            _serde::__private::None;
                        let mut __field2: _serde::__private::Option<Option<PostgresCdcSplit>> =
                            _serde::__private::None;
                        let mut __field3: _serde::__private::Option<Option<MongoDbCdcSplit>> =
                            _serde::__private::None;
                        while let _serde::__private::Some(__key) =
                            _serde::de::MapAccess::next_key::<__Field>(&mut __map)?
                        {
                            match __key {
                                __Field::__field0 => {
                                    if _serde::__private::Option::is_some(&__field0) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "mysql_split",
                                            ),
                                        );
                                    }
                                    __field0 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<Option<MySqlCdcSplit>>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                __Field::__field1 => {
                                    if _serde::__private::Option::is_some(&__field1) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "pg_split",
                                            ),
                                        );
                                    }
                                    __field1 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<
                                            Option<PostgresCdcSplit>,
                                        >(&mut __map)?,
                                    );
                                }
                                __Field::__field2 => {
                                    if _serde::__private::Option::is_some(&__field2) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "citus_split",
                                            ),
                                        );
                                    }
                                    __field2 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<
                                            Option<PostgresCdcSplit>,
                                        >(&mut __map)?,
                                    );
                                }
                                __Field::__field3 => {
                                    if _serde::__private::Option::is_some(&__field3) {
                                        return _serde::__private::Err(
                                            <__A::Error as _serde::de::Error>::duplicate_field(
                                                "mongodb_split",
                                            ),
                                        );
                                    }
                                    __field3 = _serde::__private::Some(
                                        _serde::de::MapAccess::next_value::<Option<MongoDbCdcSplit>>(
                                            &mut __map,
                                        )?,
                                    );
                                }
                                _ => {
                                    let _ = _serde::de::MapAccess::next_value::<
                                        _serde::de::IgnoredAny,
                                    >(&mut __map)?;
                                }
                            }
                        }
                        let __field0 = match __field0 {
                            _serde::__private::Some(__field0) => __field0,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("mysql_split")?
                            }
                        };
                        let __field1 = match __field1 {
                            _serde::__private::Some(__field1) => __field1,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("pg_split")?
                            }
                        };
                        let __field2 = match __field2 {
                            _serde::__private::Some(__field2) => __field2,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("citus_split")?
                            }
                        };
                        let __field3 = match __field3 {
                            _serde::__private::Some(__field3) => __field3,
                            _serde::__private::None => {
                                _serde::__private::de::missing_field("mongodb_split")?
                            }
                        };
                        _serde::__private::Ok(DebeziumCdcSplit {
                            mysql_split: __field0,
                            postgres_split: __field1,
                            citus_split: __field2,
                            mongodb_split: __field3,
                            _phantom: _serde::__private::Default::default(),
                        })
                    }
                }
                #[doc(hidden)]
                const FIELDS: &'static [&'static str] =
                    &["mysql_split", "pg_split", "citus_split", "mongodb_split"];
                _serde::Deserializer::deserialize_struct(
                    __deserializer,
                    "DebeziumCdcSplit",
                    FIELDS,
                    __Visitor {
                        marker: _serde::__private::PhantomData::<DebeziumCdcSplit<T>>,
                        lifetime: _serde::__private::PhantomData,
                    },
                )
            }
        }
    };
    #[automatically_derived]
    impl<T: ::core::fmt::Debug + CdcSourceTypeTrait> ::core::fmt::Debug for DebeziumCdcSplit<T> {
        #[inline]
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field5_finish(
                f,
                "DebeziumCdcSplit",
                "mysql_split",
                &self.mysql_split,
                "postgres_split",
                &self.postgres_split,
                "citus_split",
                &self.citus_split,
                "mongodb_split",
                &self.mongodb_split,
                "_phantom",
                &&self._phantom,
            )
        }
    }
    #[automatically_derived]
    impl<T: CdcSourceTypeTrait> ::core::marker::StructuralPartialEq for DebeziumCdcSplit<T> {}
    #[automatically_derived]
    impl<T: ::core::cmp::PartialEq + CdcSourceTypeTrait> ::core::cmp::PartialEq
        for DebeziumCdcSplit<T>
    {
        #[inline]
        fn eq(&self, other: &DebeziumCdcSplit<T>) -> bool {
            self.mysql_split == other.mysql_split
                && self.postgres_split == other.postgres_split
                && self.citus_split == other.citus_split
                && self.mongodb_split == other.mongodb_split
                && self._phantom == other._phantom
        }
    }
    #[automatically_derived]
    impl<T: ::core::hash::Hash + CdcSourceTypeTrait> ::core::hash::Hash for DebeziumCdcSplit<T> {
        #[inline]
        fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) -> () {
            ::core::hash::Hash::hash(&self.mysql_split, state);
            ::core::hash::Hash::hash(&self.postgres_split, state);
            ::core::hash::Hash::hash(&self.citus_split, state);
            ::core::hash::Hash::hash(&self.mongodb_split, state);
            ::core::hash::Hash::hash(&self._phantom, state)
        }
    }
    impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
        fn id(&self) -> SplitId {
            {
                let res = ::alloc::fmt::format(format_args!("{0}", self.split_id()));
                res
            }
            .into()
        }
        fn encode_to_json(&self) -> JsonbVal {
            serde_json::to_value(self.clone()).unwrap().into()
        }
        fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
            serde_json::from_value(value.take()).map_err(|e| {
                ::anyhow::__private::must_use({
                    use ::anyhow::__private::kind::*;
                    let error = match e {
                        error => (&error).anyhow_kind().new(error),
                    };
                    error
                })
            })
        }
        fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
            match T::source_type() {
                CdcSourceType::Mysql => self
                    .mysql_split
                    .as_mut()
                    .expect("mysql split must exist")
                    .update_with_offset(start_offset)?,
                CdcSourceType::Postgres => self
                    .postgres_split
                    .as_mut()
                    .expect("postgres split must exist")
                    .update_with_offset(start_offset)?,
                CdcSourceType::Citus => self
                    .citus_split
                    .as_mut()
                    .expect("citus split must exist")
                    .update_with_offset(start_offset)?,
                CdcSourceType::Mongodb => self
                    .mongodb_split
                    .as_mut()
                    .expect("mongodb split must exist")
                    .update_with_offset(start_offset)?,
                CdcSourceType::Unspecified => {
                    {
                        ::core::panicking::panic_fmt(format_args!(
                            "internal error: entered unreachable code: {0}",
                            format_args!("invalid debezium split")
                        ));
                    };
                }
            };
            Ok(())
        }
    }
    impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
        pub fn new_mysql(split_id: u32, start_offset: Option<String>) -> Self {
            if !match T::source_type() {
                CdcSourceType::Mysql => true,
                _ => false,
            } {
                ::core::panicking::panic(
                    "assertion failed: matches!(T::source_type(), CdcSourceType::Mysql)",
                )
            };
            let split = MySqlCdcSplit::new(split_id, start_offset);
            Self {
                mysql_split: Some(split),
                postgres_split: None,
                citus_split: None,
                mongodb_split: None,
                _phantom: PhantomData,
            }
        }
        pub fn new_postgres(split_id: u32, start_offset: Option<String>) -> Self {
            if !match T::source_type() {
                CdcSourceType::Postgres => true,
                _ => false,
            } {
                ::core::panicking::panic(
                    "assertion failed: matches!(T::source_type(), CdcSourceType::Postgres)",
                )
            };
            let split = PostgresCdcSplit::new(split_id, start_offset);
            Self {
                mysql_split: None,
                postgres_split: Some(split),
                citus_split: None,
                mongodb_split: None,
                _phantom: PhantomData,
            }
        }
        pub fn new_citus(
            split_id: u32,
            start_offset: Option<String>,
            server_addr: Option<String>,
        ) -> Self {
            if !match T::source_type() {
                CdcSourceType::Citus => true,
                _ => false,
            } {
                ::core::panicking::panic(
                    "assertion failed: matches!(T::source_type(), CdcSourceType::Citus)",
                )
            };
            let split = PostgresCdcSplit::new_with_server_addr(split_id, start_offset, server_addr);
            Self {
                mysql_split: None,
                postgres_split: None,
                citus_split: Some(split),
                mongodb_split: None,
                _phantom: PhantomData,
            }
        }
        pub fn new_mongodb(split_id: u32, start_offset: Option<String>) -> Self {
            if !match T::source_type() {
                CdcSourceType::Mongodb => true,
                _ => false,
            } {
                ::core::panicking::panic(
                    "assertion failed: matches!(T::source_type(), CdcSourceType::Mongodb)",
                )
            };
            let split = MongoDbCdcSplit::new(split_id, start_offset);
            Self {
                mysql_split: None,
                postgres_split: None,
                citus_split: None,
                mongodb_split: Some(split),
                _phantom: PhantomData,
            }
        }
        pub fn split_id(&self) -> u32 {
            match T::source_type() {
                CdcSourceType::Mysql => self
                    .mysql_split
                    .as_ref()
                    .expect("mysql split must exist")
                    .split_id(),
                CdcSourceType::Postgres => self
                    .postgres_split
                    .as_ref()
                    .expect("postgres split must exist")
                    .split_id(),
                CdcSourceType::Citus => self
                    .citus_split
                    .as_ref()
                    .expect("citus split must exist")
                    .split_id(),
                CdcSourceType::Mongodb => self
                    .mongodb_split
                    .as_ref()
                    .expect("mongodb split must exist")
                    .split_id(),
                CdcSourceType::Unspecified => {
                    {
                        ::core::panicking::panic_fmt(format_args!(
                            "internal error: entered unreachable code: {0}",
                            format_args!("invalid debezium split")
                        ));
                    };
                }
            }
        }
        pub fn start_offset(&self) -> &Option<String> {
            match T::source_type() {
                CdcSourceType::Mysql => &self
                    .mysql_split
                    .as_ref()
                    .expect("mysql split must exist")
                    .start_offset(),
                CdcSourceType::Postgres => &self
                    .postgres_split
                    .as_ref()
                    .expect("postgres split must exist")
                    .start_offset(),
                CdcSourceType::Citus => &self
                    .citus_split
                    .as_ref()
                    .expect("citus split must exist")
                    .start_offset(),
                CdcSourceType::Mongodb => &self
                    .mongodb_split
                    .as_ref()
                    .expect("mongodb split must exist")
                    .start_offset(),
                CdcSourceType::Unspecified => {
                    {
                        ::core::panicking::panic_fmt(format_args!(
                            "internal error: entered unreachable code: {0}",
                            format_args!("invalid debezium split")
                        ));
                    };
                }
            }
        }
        pub fn snapshot_done(&self) -> bool {
            match T::source_type() {
                CdcSourceType::Mysql => self
                    .mysql_split
                    .as_ref()
                    .expect("mysql split must exist")
                    .snapshot_done(),
                CdcSourceType::Postgres => self
                    .postgres_split
                    .as_ref()
                    .expect("postgres split must exist")
                    .snapshot_done(),
                CdcSourceType::Citus => self
                    .citus_split
                    .as_ref()
                    .expect("citus split must exist")
                    .snapshot_done(),
                CdcSourceType::Mongodb => self
                    .mongodb_split
                    .as_ref()
                    .expect("mongodb split must exist")
                    .snapshot_done(),
                CdcSourceType::Unspecified => {
                    {
                        ::core::panicking::panic_fmt(format_args!(
                            "internal error: entered unreachable code: {0}",
                            format_args!("invalid debezium split")
                        ));
                    };
                }
            }
        }
        pub fn server_addr(&self) -> Option<String> {
            match T::source_type() {
                CdcSourceType::Mysql | CdcSourceType::Postgres | CdcSourceType::Mongodb => None,
                CdcSourceType::Citus => self
                    .citus_split
                    .as_ref()
                    .expect("split must exist")
                    .server_addr
                    .clone(),
                CdcSourceType::Unspecified => {
                    {
                        ::core::panicking::panic_fmt(format_args!(
                            "internal error: entered unreachable code: {0}",
                            format_args!("invalid debezium split")
                        ));
                    };
                }
            }
        }
    }
}
