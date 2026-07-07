// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use anyhow::anyhow;
use bytes::{Buf, BufMut};
use itertools::Itertools;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
use risingwave_hummock_sdk::version::HummockVersion;
use serde::{Deserialize, Serialize};

use crate::meta_snapshot::{
    MetaSnapshot, Metadata, read_u32_le, read_u64_le, verify_checksum_and_strip,
};
use crate::{BackupError, BackupResult};
pub type MetaSnapshotV2 = MetaSnapshot<MetadataV2>;

impl From<serde_json::Error> for BackupError {
    fn from(value: serde_json::Error) -> Self {
        BackupError::Other(value.into())
    }
}

/// Add new item in the end. Do not change the order.
#[macro_export]
macro_rules! for_all_metadata_models_v2 {
    ($macro:ident) => {
        $macro! {
            {seaql_migrations, risingwave_meta_model::serde_seaql_migration},
            {version_stats, risingwave_meta_model::hummock_version_stats},
            {compaction_configs, risingwave_meta_model::compaction_config},
            {clusters, risingwave_meta_model::cluster},
            {fragment_relation, risingwave_meta_model::fragment_relation},
            {catalog_versions, risingwave_meta_model::catalog_version},
            {connections, risingwave_meta_model::connection},
            {databases, risingwave_meta_model::database},
            {fragments, risingwave_meta_model::fragment},
            {functions, risingwave_meta_model::function},
            {indexes, risingwave_meta_model::index},
            {objects, risingwave_meta_model::object},
            {object_dependencies, risingwave_meta_model::object_dependency},
            {schemas, risingwave_meta_model::schema},
            {sinks, risingwave_meta_model::sink},
            {sources, risingwave_meta_model::source},
            {streaming_jobs, risingwave_meta_model::streaming_job},
            {subscriptions, risingwave_meta_model::subscription},
            {system_parameters, risingwave_meta_model::system_parameter},
            {tables, risingwave_meta_model::table},
            {users, risingwave_meta_model::user},
            {user_privileges, risingwave_meta_model::user_privilege},
            {views, risingwave_meta_model::view},
            {workers, risingwave_meta_model::worker},
            {worker_properties, risingwave_meta_model::worker_property},
            {hummock_sequences, risingwave_meta_model::hummock_sequence},
            {session_parameters, risingwave_meta_model::session_parameter},
            {secrets, risingwave_meta_model::secret},
            {exactly_once_iceberg_sinks, risingwave_meta_model::exactly_once_iceberg_sink},
            {iceberg_tables, risingwave_meta_model::iceberg_tables},
            {iceberg_namespace_properties, risingwave_meta_model::iceberg_namespace_properties},
            {user_default_privilege, risingwave_meta_model::user_default_privilege},
            {fragment_splits, risingwave_meta_model::fragment_splits},
            {pending_sink_state, risingwave_meta_model::pending_sink_state},
            {refresh_jobs, risingwave_meta_model::refresh_job},
            {cdc_table_snapshot_splits, risingwave_meta_model::cdc_table_snapshot_split},
            {hummock_table_change_logs, risingwave_meta_model::hummock_table_change_log}
        }
    };
}

macro_rules! define_metadata_v2 {
    ($({ $name:ident, $mod_path:ident::$mod_name:ident }),*) => {
        #[derive(Default)]
        pub struct MetadataV2 {
            pub hummock_version: HummockVersion,
            $(
                pub $name: Vec<$mod_path::$mod_name::Model>,
            )*
        }
    };
}

for_all_metadata_models_v2!(define_metadata_v2);

macro_rules! define_encode_metadata {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        fn encode_metadata(
          metadata: &MetadataV2,
          buf: &mut Vec<u8>,
        ) -> BackupResult<()> {
            let mut _idx = 0;
            $(
                if _idx == 1 {
                    put_1(buf, &metadata.hummock_version.to_protobuf())?;
                }
                put_n(buf, &metadata.$name)?;
                _idx += 1;
            )*
            Ok(())
        }
    };
}

for_all_metadata_models_v2!(define_encode_metadata);

macro_rules! define_decode_metadata {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        fn decode_metadata(
          metadata: &mut MetadataV2,
          mut buf: &[u8],
        ) -> BackupResult<()> {
            let mut _idx = 0;
            $(
                if _idx == 1 {
                    metadata.hummock_version = HummockVersion::from_persisted_protobuf_owned(get_1(&mut buf)?);
                }
                metadata.$name = get_n(&mut buf)?;
                _idx += 1;
            )*
            Ok(())
        }
    };
}

for_all_metadata_models_v2!(define_decode_metadata);

// Metadata V2 is encoded as a positional list of sections. The actual encoded order inserts
// `hummock_version` before `version_stats`, so `hummock_sequences` is section 26:
// 0 seaql_migrations, 1 hummock_version, 2 version_stats, ..., 26 hummock_sequences.
//
// Keep this as a fixed index so partial decoding can remain compatible with future formats that
// only append sections after `hummock_sequences`.
const HUMMOCK_SEQUENCES_METADATA_SECTION_INDEX: usize = 26;

pub fn decode_hummock_sequences_from_snapshot(
    snapshot: &[u8],
) -> BackupResult<Vec<risingwave_meta_model::hummock_sequence::Model>> {
    let mut buf = verify_checksum_and_strip(snapshot)?;
    let format_version = read_u32_le(&mut buf)?;
    if format_version < 2 {
        return Err(BackupError::Other(anyhow!(
            "unsupported metadata snapshot format version for hummock sequence partial decoding: {}",
            format_version
        )));
    }
    let _snapshot_id = read_u64_le(&mut buf)?;
    for _ in 0..HUMMOCK_SEQUENCES_METADATA_SECTION_INDEX {
        skip_metadata_list(&mut buf)?;
    }
    get_n(&mut buf)
}

impl Display for MetadataV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "clusters: {:#?}", self.clusters)?;
        writeln!(
            f,
            "Hummock version: id {}, committed_epoch: {:?}",
            self.hummock_version.id,
            self.hummock_version.state_table_info.info(),
        )?;
        // optionally dump other metadata
        Ok(())
    }
}

impl Metadata for MetadataV2 {
    fn encode_to(&self, buf: &mut Vec<u8>) -> BackupResult<()> {
        encode_metadata(self, buf)
    }

    fn decode(buf: &[u8]) -> BackupResult<Self>
    where
        Self: Sized,
    {
        let mut metadata = Self::default();
        decode_metadata(&mut metadata, buf)?;
        Ok(metadata)
    }

    fn hummock_version_ref(&self) -> &HummockVersion {
        &self.hummock_version
    }

    fn hummock_version(self) -> HummockVersion {
        self.hummock_version
    }

    fn storage_url(&self) -> BackupResult<String> {
        let storage_url_from_snapshot = self
            .system_parameters
            .iter()
            .filter_map(|m| {
                if m.name == "state_store" {
                    return Some(m.value.clone());
                }
                None
            })
            .exactly_one()
            .map_err(|_| BackupError::Other(anyhow!("expect state_store")))?;
        storage_url_from_snapshot
            .strip_prefix("hummock+")
            .map(|s| s.to_owned())
            .ok_or_else(|| {
                BackupError::Other(anyhow!(
                    "invalid state_store from metadata snapshot: {}",
                    storage_url_from_snapshot
                ))
            })
    }

    fn storage_directory(&self) -> BackupResult<String> {
        self.system_parameters
            .iter()
            .filter_map(|m| {
                if m.name == "data_directory" {
                    return Some(m.value.clone());
                }
                None
            })
            .exactly_one()
            .map_err(|_| BackupError::Other(anyhow!("expect data_directory")))
    }

    fn table_change_log_object_ids(&self) -> HashSet<HummockRawObjectId> {
        self.hummock_table_change_logs
            .iter()
            .flat_map(|m| {
                // We cannot use `change_log_ssts` here because `to_table_change_log` returns an owned value, not a reference.
                let EpochNewChangeLog {
                    new_value,
                    old_value,
                    ..
                } = to_table_change_log(m);
                new_value
                    .into_iter()
                    .chain(old_value)
                    .map(|t| t.object_id.as_raw())
            })
            .collect()
    }
}

fn to_table_change_log(
    change_log: &risingwave_meta_model::hummock_table_change_log::Model,
) -> EpochNewChangeLog {
    EpochNewChangeLog {
        new_value: change_log
            .new_value_sst
            .to_protobuf()
            .into_iter()
            .map(Into::into)
            .collect(),
        old_value: change_log
            .old_value_sst
            .to_protobuf()
            .into_iter()
            .map(Into::into)
            .collect(),
        non_checkpoint_epochs: change_log
            .non_checkpoint_epochs
            .0
            .iter()
            .map(|e| *e as _)
            .collect(),
        checkpoint_epoch: change_log.checkpoint_epoch as _,
    }
}

fn put_n<T: Serialize>(buf: &mut Vec<u8>, data: &[T]) -> Result<(), serde_json::Error> {
    buf.put_u32_le(
        data.len()
            .try_into()
            .unwrap_or_else(|_| panic!("cannot convert {} into u32", data.len())),
    );
    for d in data {
        put_with_len_prefix(buf, d)?;
    }
    Ok(())
}

fn put_1<T: Serialize>(buf: &mut Vec<u8>, data: &T) -> Result<(), serde_json::Error> {
    put_n(buf, &[data])
}

fn get_n<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> BackupResult<Vec<T>> {
    let n = read_u32_le(buf)? as usize;
    let mut elements = Vec::with_capacity(n);
    for _ in 0..n {
        elements.push(get_with_len_prefix(buf)?);
    }
    Ok(elements)
}

fn get_1<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> BackupResult<T> {
    let elements = get_n(buf)?;
    assert_eq!(elements.len(), 1);
    Ok(elements.into_iter().next().unwrap())
}

fn skip_metadata_list(buf: &mut &[u8]) -> BackupResult<()> {
    let n = read_u32_le(buf)? as usize;
    for _ in 0..n {
        skip_with_len_prefix(buf)?;
    }
    Ok(())
}

fn skip_with_len_prefix(buf: &mut &[u8]) -> BackupResult<()> {
    let len = read_len_prefix(buf)?;
    if buf.remaining() < len {
        return Err(BackupError::Other(anyhow!(
            "metadata JSON payload length {} exceeds remaining buffer {}",
            len,
            buf.remaining()
        )));
    }
    buf.advance(len);
    Ok(())
}

fn read_len_prefix(buf: &mut &[u8]) -> BackupResult<usize> {
    match read_u32_le(buf)? {
        0 => read_u64_le(buf)?
            .try_into()
            .map_err(|_| BackupError::Other(anyhow!("metadata JSON payload length exceeds usize"))),
        len => Ok(len as usize),
    }
}

fn put_with_len_prefix<T: Serialize>(buf: &mut Vec<u8>, data: &T) -> Result<(), serde_json::Error> {
    let b = serde_json::to_vec(data)?;
    // Any valid JSON value serialized by serde_json is non-empty, such as `null`, `{}`, or `[]`.
    assert!(!b.is_empty());
    if let Ok(len) = u32::try_from(b.len()) {
        buf.put_u32_le(len);
    } else {
        buf.put_u32_le(0);
        buf.put_u64_le(
            b.len()
                .try_into()
                .unwrap_or_else(|_| panic!("cannot convert {} into u64", b.len())),
        );
    }
    buf.put_slice(&b);
    Ok(())
}

fn get_with_len_prefix<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> BackupResult<T> {
    let len = read_len_prefix(buf)?;
    if buf.remaining() < len {
        return Err(BackupError::Other(anyhow!(
            "metadata JSON payload length {} exceeds remaining buffer {}",
            len,
            buf.remaining()
        )));
    }
    let d = serde_json::from_slice(&buf[..len])?;
    buf.advance(len);
    Ok(d)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use risingwave_common::util::panic::rw_catch_unwind;
    use risingwave_meta_model::hummock_sequence;

    use super::*;

    fn encoded_snapshot_with_invalid_trailing_metadata_after_hummock_sequences(
        metadata: &MetadataV2,
    ) -> Vec<u8> {
        let raw = MetaSnapshotV2 {
            format_version: 2,
            id: 123,
            metadata: MetadataV2 {
                hummock_sequences: metadata.hummock_sequences.clone(),
                ..Default::default()
            },
        };
        let encoded = raw.encode().unwrap();
        let payload = &encoded[..encoded.len() - 8];
        let mut metadata_buf = &payload[12..];

        for _ in 0..=HUMMOCK_SEQUENCES_METADATA_SECTION_INDEX {
            skip_metadata_list(&mut metadata_buf).unwrap();
        }

        let mut partial_payload = payload[..payload.len() - metadata_buf.len()].to_vec();
        partial_payload.put_u32_le(1);
        partial_payload.put_u32_le(100);
        partial_payload.put_slice(br#""short""#);
        let checksum = crate::xxhash64_checksum(&partial_payload);
        partial_payload.put_u64_le(checksum);
        partial_payload
    }

    fn append_future_metadata_section(mut encoded: Vec<u8>) -> Vec<u8> {
        encoded.truncate(encoded.len() - 8);
        put_n(&mut encoded, &["future section"]).unwrap();
        let checksum = crate::xxhash64_checksum(&encoded);
        encoded.put_u64_le(checksum);
        encoded
    }

    #[test]
    fn test_len_prefix_legacy_u32_round_trip() {
        let mut buf = vec![];
        put_with_len_prefix(&mut buf, &"hello").unwrap();

        assert_ne!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 0);

        let decoded: String = get_with_len_prefix(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn test_len_prefix_extended_u64_decoding() {
        let payload = serde_json::to_vec("hello").unwrap();
        let mut buf = vec![];
        buf.put_u32_le(0);
        buf.put_u64_le(payload.len() as u64);
        buf.put_slice(&payload);

        let decoded: String = get_with_len_prefix(&mut buf.as_slice()).unwrap();
        assert_eq!(decoded, "hello");
    }

    #[test]
    fn test_len_prefix_errors_on_missing_extended_u64() {
        let mut buf = vec![];
        buf.put_u32_le(0);

        assert!(get_with_len_prefix::<String>(&mut buf.as_slice()).is_err());
    }

    #[test]
    fn test_len_prefix_errors_on_payload_shorter_than_declared_len() {
        let mut buf = vec![];
        buf.put_u32_le(100);
        buf.put_slice(br#""hello""#);

        assert!(get_with_len_prefix::<String>(&mut buf.as_slice()).is_err());
    }

    #[test]
    fn test_snapshot_v2_encoding_decoding() {
        let raw = MetaSnapshotV2 {
            format_version: 2,
            id: 123,
            metadata: MetadataV2::default(),
        };

        let encoded = raw.encode().unwrap();
        let decoded = MetaSnapshotV2::decode(&encoded).unwrap();

        assert_eq!(decoded.format_version, raw.format_version);
        assert_eq!(decoded.id, raw.id);
        assert_eq!(
            decoded.metadata.hummock_version.id,
            raw.metadata.hummock_version.id
        );
    }

    #[test]
    fn test_partial_decode_hummock_sequences() {
        let raw = MetaSnapshotV2 {
            format_version: 2,
            id: 123,
            metadata: MetadataV2 {
                hummock_sequences: vec![
                    hummock_sequence::Model {
                        name: "meta_backup".to_owned(),
                        seq: 42,
                    },
                    hummock_sequence::Model {
                        name: "sstable_object".to_owned(),
                        seq: 100,
                    },
                ],
                ..Default::default()
            },
        };

        let decoded = decode_hummock_sequences_from_snapshot(&raw.encode().unwrap()).unwrap();
        assert_eq!(decoded, raw.metadata.hummock_sequences);
    }

    #[test]
    fn test_partial_decode_hummock_sequences_ignores_trailing_metadata() {
        let metadata = MetadataV2 {
            hummock_sequences: vec![hummock_sequence::Model {
                name: "meta_backup".to_owned(),
                seq: 42,
            }],
            ..Default::default()
        };
        let encoded =
            encoded_snapshot_with_invalid_trailing_metadata_after_hummock_sequences(&metadata);

        let decoded = decode_hummock_sequences_from_snapshot(&encoded).unwrap();
        assert_eq!(decoded, metadata.hummock_sequences);
        let full_decode_result = rw_catch_unwind(|| MetaSnapshotV2::decode(&encoded));
        assert!(
            full_decode_result.is_err() || full_decode_result.unwrap().is_err(),
            "full metadata decoding should not accept the intentionally invalid trailing section"
        );
    }

    #[test]
    fn test_partial_decode_hummock_sequences_ignores_appended_future_sections() {
        let raw = MetaSnapshotV2 {
            format_version: 2,
            id: 123,
            metadata: MetadataV2 {
                hummock_sequences: vec![hummock_sequence::Model {
                    name: "meta_backup".to_owned(),
                    seq: 42,
                }],
                ..Default::default()
            },
        };

        let encoded = append_future_metadata_section(raw.encode().unwrap());
        let decoded = decode_hummock_sequences_from_snapshot(&encoded).unwrap();
        assert_eq!(decoded, raw.metadata.hummock_sequences);
    }
}
