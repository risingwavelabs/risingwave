// Copyright 2025 RisingWave Labs
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

use std::fmt::{Display, Formatter};

use bytes::{Buf, BufMut};
use risingwave_hummock_sdk::version::HummockVersion;
use serde::{Deserialize, Serialize};

use crate::meta_snapshot::{MetaSnapshot, Metadata};
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
            {actors, risingwave_meta_model::actor},
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
            {iceberg_namespace_properties, risingwave_meta_model::iceberg_namespace_properties}
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
                    metadata.hummock_version = HummockVersion::from_persisted_protobuf(&get_1(&mut buf)?);
                }
                metadata.$name = get_n(&mut buf)?;
                _idx += 1;
            )*
            Ok(())
        }
    };
}

for_all_metadata_models_v2!(define_decode_metadata);

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

fn get_n<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> Result<Vec<T>, serde_json::Error> {
    let n = buf.get_u32_le() as usize;
    let mut elements = Vec::with_capacity(n);
    for _ in 0..n {
        elements.push(get_with_len_prefix(buf)?);
    }
    Ok(elements)
}

fn get_1<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> Result<T, serde_json::Error> {
    let elements = get_n(buf)?;
    assert_eq!(elements.len(), 1);
    Ok(elements.into_iter().next().unwrap())
}

fn put_with_len_prefix<T: Serialize>(buf: &mut Vec<u8>, data: &T) -> Result<(), serde_json::Error> {
    let b = serde_json::to_vec(data)?;
    buf.put_u32_le(
        b.len()
            .try_into()
            .unwrap_or_else(|_| panic!("cannot convert {} into u32", b.len())),
    );
    buf.put_slice(&b);
    Ok(())
}

fn get_with_len_prefix<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> Result<T, serde_json::Error> {
    let len = buf.get_u32_le() as usize;
    let d = serde_json::from_slice(&buf[..len])?;
    buf.advance(len);
    Ok(d)
}
