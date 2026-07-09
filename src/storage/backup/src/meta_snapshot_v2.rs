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
use std::mem::size_of;

use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use itertools::Itertools;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectDataStreamReader};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::AsyncRead;
use tracing::info;

use crate::meta_snapshot::{MetaSnapshot, Metadata, SnapshotPayloadReader, SnapshotPayloadWriter};
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

macro_rules! define_encode_metadata_to_writer {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        async fn encode_metadata_to_writer(
            metadata: &MetadataV2,
            writer: &mut SnapshotPayloadWriter,
        ) -> BackupResult<()> {
            let mut _idx = 0;
            $(
                if _idx == 1 {
                    write_1(writer, &metadata.hummock_version.to_protobuf()).await?;
                }
                write_n(writer, &metadata.$name).await?;
                _idx += 1;
            )*
            Ok(())
        }
    };
}

for_all_metadata_models_v2!(define_encode_metadata_to_writer);

macro_rules! define_decode_metadata {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        async fn decode_metadata_from_reader<R>(
            metadata: &mut MetadataV2,
            reader: &mut SnapshotPayloadReader<R>,
        ) -> BackupResult<()>
        where
            R: AsyncRead + Send,
        {
            let mut _idx = 0;
            $(
                if _idx == 1 {
                    metadata.hummock_version = HummockVersion::from_persisted_protobuf_owned(read_1(reader).await?);
                }
                metadata.$name = read_n(reader).await?;
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

pub async fn decode_hummock_sequences_from_stream(
    stream: MonitoredStreamingReader,
) -> BackupResult<Vec<risingwave_meta_model::hummock_sequence::Model>> {
    let mut reader = SnapshotPayloadReader::new(ObjectDataStreamReader::new(stream.into_stream()));
    let (format_version, _snapshot_id) = reader.read_snapshot_header().await?;
    if format_version < 2 {
        return Err(BackupError::Other(anyhow!(
            "unsupported metadata snapshot format version for hummock sequence partial decoding: {}",
            format_version
        )));
    }
    for _ in 0..HUMMOCK_SEQUENCES_METADATA_SECTION_INDEX {
        skip_metadata_list(&mut reader).await?;
    }
    let hummock_sequences = read_n(&mut reader).await?;
    reader.finish_after_skipping_to_end().await?;
    Ok(hummock_sequences)
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
    async fn encode_to_writer(&self, writer: &mut SnapshotPayloadWriter) -> BackupResult<()> {
        encode_metadata_to_writer(self, writer).await?;
        let snapshot_size_bytes = writer.size() + size_of::<u64>();
        info!(snapshot_size_bytes, "encoded v2 meta snapshot backup file");
        Ok(())
    }

    async fn decode_from_reader<R>(mut reader: SnapshotPayloadReader<R>) -> BackupResult<Self>
    where
        Self: Sized,
        R: AsyncRead + Send,
    {
        let mut metadata = Self::default();
        decode_metadata_from_reader(&mut metadata, &mut reader).await?;
        reader.finish().await?;
        Ok(metadata)
    }

    fn hummock_version_ref(&self) -> &HummockVersion {
        &self.hummock_version
    }

    fn hummock_version(self) -> HummockVersion {
        self.hummock_version
    }

    fn storage_url(&self) -> BackupResult<String> {
        let storage_url_from_snapshot =
            Itertools::exactly_one(self.system_parameters.iter().filter_map(|m| {
                if m.name == "state_store" {
                    return Some(m.value.clone());
                }
                None
            }))
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
        Itertools::exactly_one(self.system_parameters.iter().filter_map(|m| {
            if m.name == "data_directory" {
                return Some(m.value.clone());
            }
            None
        }))
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

async fn write_len(writer: &mut SnapshotPayloadWriter, len: usize) -> BackupResult<()> {
    let mut buf = BytesMut::with_capacity(size_of::<u32>());
    buf.put_u32_le(
        len.try_into()
            .unwrap_or_else(|_| panic!("cannot convert {} into u32", len)),
    );
    writer.write_snapshot_bytes(buf.freeze()).await
}

async fn write_n<T: Serialize>(writer: &mut SnapshotPayloadWriter, data: &[T]) -> BackupResult<()> {
    write_len(writer, data.len()).await?;
    for d in data {
        write_with_len_prefix(writer, d).await?;
    }
    Ok(())
}

async fn write_1<T: Serialize>(writer: &mut SnapshotPayloadWriter, data: &T) -> BackupResult<()> {
    write_n(writer, &[data]).await
}

async fn read_n<R, T>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<Vec<T>>
where
    R: AsyncRead + Send,
    T: DeserializeOwned,
{
    let n = reader.read_u32_le().await? as usize;
    let mut elements = Vec::with_capacity(n);
    for _ in 0..n {
        elements.push(read_with_len_prefix(reader).await?);
    }
    Ok(elements)
}

async fn read_1<R, T>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<T>
where
    R: AsyncRead + Send,
    T: DeserializeOwned,
{
    let elements = read_n(reader).await?;
    assert_eq!(elements.len(), 1);
    Ok(elements.into_iter().next().unwrap())
}

async fn write_with_len_prefix<T: Serialize>(
    writer: &mut SnapshotPayloadWriter,
    data: &T,
) -> BackupResult<()> {
    let b = serde_json::to_vec(data)?;
    // Any valid JSON value serialized by serde_json is non-empty, such as `null`, `{}`, or `[]`.
    assert!(!b.is_empty());
    let len_prefix_len = if u32::try_from(b.len()).is_ok() {
        size_of::<u32>()
    } else {
        size_of::<u32>() + size_of::<u64>()
    };
    let mut buf = BytesMut::with_capacity(len_prefix_len + b.len());
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
    writer.write_snapshot_bytes(buf.freeze()).await
}

async fn read_with_len_prefix<R, T>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<T>
where
    R: AsyncRead + Send,
    T: DeserializeOwned,
{
    let len = read_len_prefix(reader).await?;
    let bytes = reader.read_exact(len).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn skip_metadata_list<R>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<()>
where
    R: AsyncRead + Send,
{
    let n = reader.read_u32_le().await? as usize;
    for _ in 0..n {
        skip_with_len_prefix(reader).await?;
    }
    Ok(())
}

async fn skip_with_len_prefix<R>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<()>
where
    R: AsyncRead + Send,
{
    let len = read_len_prefix(reader).await?;
    reader.skip_exact(len).await
}

async fn read_len_prefix<R>(reader: &mut SnapshotPayloadReader<R>) -> BackupResult<usize>
where
    R: AsyncRead + Send,
{
    match reader.read_u32_le().await? {
        0 => {
            reader.read_u64_le().await?.try_into().map_err(|_| {
                BackupError::Other(anyhow!("metadata JSON payload length exceeds usize"))
            })
        }
        len => Ok(len as usize),
    }
}
