// Copyright 2024 RisingWave Labs
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
use risingwave_meta_model_v2 as model_v2;
use serde::{Deserialize, Serialize};

use crate::meta_snapshot::{MetaSnapshot, Metadata};
use crate::{BackupError, BackupResult};
pub type MetaSnapshotV2 = MetaSnapshot<MetadataV2>;

impl From<serde_json::Error> for BackupError {
    fn from(value: serde_json::Error) -> Self {
        BackupError::Other(value.into())
    }
}

#[derive(Default)]
pub struct MetadataV2 {
    pub seaql_migrations: Vec<model_v2::serde_seaql_migration::Model>,
    pub hummock_version: HummockVersion,
    pub version_stats: Vec<model_v2::hummock_version_stats::Model>,
    pub compaction_configs: Vec<model_v2::compaction_config::Model>,
    pub actors: Vec<model_v2::actor::Model>,
    pub clusters: Vec<model_v2::cluster::Model>,
    pub actor_dispatchers: Vec<model_v2::actor_dispatcher::Model>,
    pub catalog_versions: Vec<model_v2::catalog_version::Model>,
    pub connections: Vec<model_v2::connection::Model>,
    pub databases: Vec<model_v2::database::Model>,
    pub fragments: Vec<model_v2::fragment::Model>,
    pub functions: Vec<model_v2::function::Model>,
    pub indexes: Vec<model_v2::index::Model>,
    pub objects: Vec<model_v2::object::Model>,
    pub object_dependencies: Vec<model_v2::object_dependency::Model>,
    pub schemas: Vec<model_v2::schema::Model>,
    pub sinks: Vec<model_v2::sink::Model>,
    pub sources: Vec<model_v2::source::Model>,
    pub streaming_jobs: Vec<model_v2::streaming_job::Model>,
    pub subscriptions: Vec<model_v2::subscription::Model>,
    pub system_parameters: Vec<model_v2::system_parameter::Model>,
    pub tables: Vec<model_v2::table::Model>,
    pub users: Vec<model_v2::user::Model>,
    pub user_privileges: Vec<model_v2::user_privilege::Model>,
    pub views: Vec<model_v2::view::Model>,
    pub workers: Vec<model_v2::worker::Model>,
    pub worker_properties: Vec<model_v2::worker_property::Model>,
    pub hummock_sequences: Vec<model_v2::hummock_sequence::Model>,
    pub session_parameters: Vec<model_v2::session_parameter::Model>,
}

impl Display for MetadataV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "clusters: {:#?}", self.clusters)?;
        writeln!(
            f,
            "Hummock version: id {}, max_committed_epoch: {}",
            self.hummock_version.id, self.hummock_version.max_committed_epoch
        )?;
        // optionally dump other metadata
        Ok(())
    }
}

impl Metadata for MetadataV2 {
    fn encode_to(&self, buf: &mut Vec<u8>) -> BackupResult<()> {
        put_n(buf, &self.seaql_migrations)?;
        put_1(buf, &self.hummock_version.to_protobuf())?;
        put_n(buf, &self.version_stats)?;
        put_n(buf, &self.compaction_configs)?;
        put_n(buf, &self.actors)?;
        put_n(buf, &self.clusters)?;
        put_n(buf, &self.actor_dispatchers)?;
        put_n(buf, &self.catalog_versions)?;
        put_n(buf, &self.connections)?;
        put_n(buf, &self.databases)?;
        put_n(buf, &self.fragments)?;
        put_n(buf, &self.functions)?;
        put_n(buf, &self.indexes)?;
        put_n(buf, &self.objects)?;
        put_n(buf, &self.object_dependencies)?;
        put_n(buf, &self.schemas)?;
        put_n(buf, &self.sinks)?;
        put_n(buf, &self.sources)?;
        put_n(buf, &self.streaming_jobs)?;
        put_n(buf, &self.subscriptions)?;
        put_n(buf, &self.system_parameters)?;
        put_n(buf, &self.tables)?;
        put_n(buf, &self.users)?;
        put_n(buf, &self.user_privileges)?;
        put_n(buf, &self.views)?;
        put_n(buf, &self.workers)?;
        put_n(buf, &self.worker_properties)?;
        put_n(buf, &self.hummock_sequences)?;
        put_n(buf, &self.session_parameters)?;
        Ok(())
    }

    fn decode(mut buf: &[u8]) -> BackupResult<Self>
    where
        Self: Sized,
    {
        let seaql_migrations = get_n(&mut buf)?;
        let pb_hummock_version = get_1(&mut buf)?;
        let version_stats = get_n(&mut buf)?;
        let compaction_configs = get_n(&mut buf)?;
        let actors = get_n(&mut buf)?;
        let clusters = get_n(&mut buf)?;
        let actor_dispatchers = get_n(&mut buf)?;
        let catalog_versions = get_n(&mut buf)?;
        let connections = get_n(&mut buf)?;
        let databases = get_n(&mut buf)?;
        let fragments = get_n(&mut buf)?;
        let functions = get_n(&mut buf)?;
        let indexes = get_n(&mut buf)?;
        let objects = get_n(&mut buf)?;
        let object_dependencies = get_n(&mut buf)?;
        let schemas = get_n(&mut buf)?;
        let sinks = get_n(&mut buf)?;
        let sources = get_n(&mut buf)?;
        let streaming_jobs = get_n(&mut buf)?;
        let subscriptions = get_n(&mut buf)?;
        let system_parameters = get_n(&mut buf)?;
        let tables = get_n(&mut buf)?;
        let users = get_n(&mut buf)?;
        let user_privileges = get_n(&mut buf)?;
        let views = get_n(&mut buf)?;
        let workers = get_n(&mut buf)?;
        let worker_properties = get_n(&mut buf)?;
        let hummock_sequences = get_n(&mut buf)?;
        let session_parameters = get_n(&mut buf)?;
        Ok(Self {
            seaql_migrations,
            hummock_version: HummockVersion::from_persisted_protobuf(&pb_hummock_version),
            version_stats,
            compaction_configs,
            actors,
            clusters,
            actor_dispatchers,
            catalog_versions,
            connections,
            databases,
            fragments,
            functions,
            indexes,
            objects,
            object_dependencies,
            schemas,
            sinks,
            sources,
            streaming_jobs,
            subscriptions,
            system_parameters,
            tables,
            users,
            user_privileges,
            views,
            workers,
            worker_properties,
            hummock_sequences,
            session_parameters,
        })
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
