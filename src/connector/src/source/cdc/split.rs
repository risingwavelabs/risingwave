// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;

use anyhow::Context;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::cdc::external::DebeziumOffset;
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcSerializedNames, CdcSourceType, CdcSourceTypeTrait,
};
use crate::source::{SplitId, SplitMetaData};

/// The base states of a CDC split, which will be persisted to checkpoint.
/// CDC source only has single split, so we use the `source_id` to identify the split.
#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct CdcSplitBase {
    pub split_id: u32,
    pub start_offset: Option<String>,
    pub snapshot_done: bool,
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

/// The split of one CDC connector, wrapped by [`DebeziumCdcSplit`].
///
/// Every connector's split holds a [`CdcSplitBase`]; the provided methods read and write it
/// through [`Self::inner`] and [`Self::inner_mut`]. A connector overrides a method only when its
/// Debezium offset format needs special handling. Connector-specific fields beyond the base are
/// initialized by the connector's own constructor in its own module, not through this trait.
/// Reading the offset itself is the job of `CdcDebeziumOffsetInterpreter`; the wrapper
/// interprets it and hands the result to [`Self::update_offset`].
pub trait CdcSplitTrait: Send + Sync + Default {
    fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let mut split = Self::default();
        *split.inner_mut() = CdcSplitBase::new(split_id, start_offset);
        split
    }

    fn inner(&self) -> &CdcSplitBase;

    fn inner_mut(&mut self) -> &mut CdcSplitBase;

    fn split_id(&self) -> u32 {
        self.inner().split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner().start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner().snapshot_done
    }

    /// Clear the start offset so that the connector restarts from the beginning (`RESET SOURCE`).
    fn reset_start_offset(&mut self) {
        self.inner_mut().start_offset = None;
    }

    /// Stores `last_seen_offset` and the `snapshot_done` flag the wrapper derived from it.
    fn update_offset(
        &mut self,
        last_seen_offset: String,
        snapshot_done: bool,
    ) -> ConnectorResult<()> {
        let inner = self.inner_mut();
        inner.snapshot_done = snapshot_done;
        inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

/// The `snapshot_done` flag of a split after seeing `offset`. Once set it stays set; heartbeats
/// carry no snapshot information and leave it unchanged.
fn snapshot_done_after_offset<T: CdcDebeziumOffsetInterpreter>(
    split: &impl CdcSplitTrait,
    offset: &str,
) -> ConnectorResult<bool> {
    if split.is_snapshot_done() {
        return Ok(true);
    }
    let dbz_offset: DebeziumOffset<T::SourceOffset> =
        serde_json::from_str(offset).with_context(|| {
            format!(
                "invalid cdc offset: {}, split: {}",
                offset,
                split.split_id()
            )
        })?;
    if dbz_offset.is_heartbeat {
        return Ok(false);
    }
    Ok(T::snapshot_done_from_debezium_offset(&dbz_offset))
}

/// Generates [`DebeziumCdcSplit`] from the CDC block of `for_all_classified_sources!`: one
/// `Option<XxxCdcSplit>` field named `xxx_split` per connector, its constructor, and its serde
/// impls. `XxxCdcSplit` is expected at `crate::source::cdc::xxx::XxxCdcSplit`. Serde is
/// hand-written because the persisted key of each field comes from `CdcSerializedNames`,
/// which `#[serde(rename)]` cannot read.
macro_rules! impl_debezium_cdc_split {
    ({$({$cdc_source_type:ident}),*}, {$($_ignore:tt),*}) => {
        $crate::paste! {
            /// We use this struct to wrap the specific split, which act as an interface to other modules
            #[derive(Clone, Debug, PartialEq, Hash)]
            pub struct DebeziumCdcSplit<T: CdcSourceTypeTrait> {
                $(
                    pub [<$cdc_source_type:snake _split>]:
                        Option<$crate::source::cdc::[<$cdc_source_type:snake>]::[<$cdc_source_type CdcSplit>]>,
                )*
                pub _phantom: PhantomData<T>,
            }

            impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
                fn empty() -> Self {
                    Self {
                        $(
                            [<$cdc_source_type:snake _split>]: None,
                        )*
                        _phantom: PhantomData,
                    }
                }

                pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
                    let mut split = Self::empty();
                    match T::source_type() {
                        $(
                            CdcSourceType::$cdc_source_type => {
                                split.[<$cdc_source_type:snake _split>] = Some(
                                    $crate::source::cdc::[<$cdc_source_type:snake>]::[<$cdc_source_type CdcSplit>]::new(
                                        split_id,
                                        start_offset,
                                    ),
                                );
                            }
                        )*
                        CdcSourceType::Unspecified => unreachable!("invalid debezium split"),
                    }
                    split
                }
            }

            const DEBEZIUM_CDC_SPLIT_FIELDS: &[&str] = &[
                $(
                    <$crate::source::cdc::$cdc_source_type as CdcSerializedNames>::DEBEZIUM_CDC_SPLIT_SPLIT_FIELD,
                )*
            ];

            impl<T: CdcSourceTypeTrait> Serialize for DebeziumCdcSplit<T> {
                fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    use serde::ser::SerializeStruct;
                    let mut state = serializer
                        .serialize_struct("DebeziumCdcSplit", DEBEZIUM_CDC_SPLIT_FIELDS.len())?;
                    $(
                        state.serialize_field(
                            <$crate::source::cdc::$cdc_source_type as CdcSerializedNames>::DEBEZIUM_CDC_SPLIT_SPLIT_FIELD,
                            &self.[<$cdc_source_type:snake _split>],
                        )?;
                    )*
                    state.end()
                }
            }

            impl<'de, T: CdcSourceTypeTrait> Deserialize<'de> for DebeziumCdcSplit<T> {
                fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                    struct DebeziumCdcSplitVisitor<T>(PhantomData<T>);

                    impl<'de, T: CdcSourceTypeTrait> serde::de::Visitor<'de> for DebeziumCdcSplitVisitor<T> {
                        type Value = DebeziumCdcSplit<T>;

                        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            formatter.write_str("struct DebeziumCdcSplit")
                        }

                        fn visit_map<A: serde::de::MapAccess<'de>>(
                            self,
                            mut map: A,
                        ) -> Result<Self::Value, A::Error> {
                            let mut split = DebeziumCdcSplit::<T>::empty();
                            while let Some(key) = map.next_key::<String>()? {
                                match key.as_str() {
                                    $(
                                        key if key == <$crate::source::cdc::$cdc_source_type as CdcSerializedNames>::DEBEZIUM_CDC_SPLIT_SPLIT_FIELD => {
                                            split.[<$cdc_source_type:snake _split>] = map.next_value()?;
                                        }
                                    )*
                                    _ => {
                                        map.next_value::<serde::de::IgnoredAny>()?;
                                    }
                                }
                            }
                            Ok(split)
                        }
                    }

                    deserializer.deserialize_struct(
                        "DebeziumCdcSplit",
                        DEBEZIUM_CDC_SPLIT_FIELDS,
                        DebeziumCdcSplitVisitor(PhantomData),
                    )
                }
            }
        }
    };
}

crate::for_all_classified_sources!(impl_debezium_cdc_split);

macro_rules! dispatch_cdc_split_inner {
    (
        {$({$cdc_source_type:ident}),*},
        {$($_ignore:tt),*},
        $dbz_split:tt,
        $as_type:tt,
        $split:ident,
        $connector:ident,
        $body:expr
    ) => {
        $crate::paste! {
            match T::source_type() {
                $(
                    CdcSourceType::$cdc_source_type => {
                        type $connector = $crate::source::cdc::$cdc_source_type;
                        let $split = $dbz_split.[<$cdc_source_type:snake _split>]
                            .[<as_ $as_type>]()
                            .expect(concat!(stringify!([<$cdc_source_type:lower>]), " split must exist"));
                        {
                            $body
                        }
                    }
                )*
                CdcSourceType::Unspecified => {
                    unreachable!("invalid debezium split");
                }
            }
        }
    };
}

/// Runs `$body` on the split of the connector `T`, bound as `$split` (`ref` or `mut`). The form
/// with `$connector` also binds the connector marker type under that name, so the body can call
/// its trait items.
macro_rules! dispatch_cdc_split {
    ($dbz_split:expr, $as_type:tt, $split:ident, $body:expr) => {
        dispatch_cdc_split!($dbz_split, $as_type, $split, _Connector, $body)
    };
    ($dbz_split:expr, $as_type:tt, $split:ident, $connector:ident, $body:expr) => {{
        crate::for_all_classified_sources! {
            dispatch_cdc_split_inner,
            { $dbz_split },
            $as_type,
            $split,
            $connector,
            { $body }
        }
    }};
}

impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
    fn id(&self) -> SplitId {
        format!("{}", self.split_id()).into()
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.update_offset_inner(last_seen_offset)
    }
}

impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
    pub fn split_id(&self) -> u32 {
        dispatch_cdc_split!(self, ref, split, split.split_id())
    }

    pub fn start_offset(&self) -> &Option<String> {
        dispatch_cdc_split!(self, ref, split, split.start_offset())
    }

    pub fn snapshot_done(&self) -> bool {
        dispatch_cdc_split!(self, ref, split, split.is_snapshot_done())
    }

    /// Clear the start offset so that the connector restarts from the beginning (`RESET SOURCE`).
    pub fn reset_start_offset(&mut self) {
        dispatch_cdc_split!(self, mut, split, split.reset_start_offset())
    }

    pub fn update_offset_inner(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        dispatch_cdc_split!(self, mut, split, C, {
            let snapshot_done = snapshot_done_after_offset::<C>(split, &last_seen_offset)?;
            split.update_offset(last_seen_offset, snapshot_done)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::cdc::Mysql;

    #[test]
    fn test_debezium_cdc_split_persisted_field_names() {
        let mut fields = DEBEZIUM_CDC_SPLIT_FIELDS.to_vec();
        fields.sort_unstable();
        assert_eq!(
            fields,
            [
                "citus_split",
                "mongodb_split",
                "mysql_split",
                "oracle_split",
                "pg_split",
                "sql_server_split",
            ]
        );

        // Persisted before `oracle_split` existed; the missing key must restore as `None`.
        let legacy = r#"{
            "mysql_split": {"inner": {"split_id": 1001, "start_offset": "offset", "snapshot_done": true}},
            "pg_split": null,
            "citus_split": null,
            "mongodb_split": null,
            "sql_server_split": null
        }"#;
        let restored: DebeziumCdcSplit<Mysql> = serde_json::from_str(legacy).unwrap();
        assert_eq!(restored.split_id(), 1001);
        assert_eq!(restored.start_offset().as_deref(), Some("offset"));
        assert!(restored.snapshot_done());
        assert!(restored.oracle_split.is_none());
    }
}
