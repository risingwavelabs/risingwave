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

use std::fmt::{Display, Formatter};

use bytes::{Buf, BufMut};
use risingwave_meta_model_v2 as model_v2;
use risingwave_pb::hummock::HummockVersion;
use serde::{Deserialize, Serialize};

use crate::meta_snapshot::{MetaSnapshot, Metadata};
use crate::BackupResult;

pub type MetaSnapshotV2 = MetaSnapshot<MetadataV2>;

#[derive(Default)]
pub struct MetadataV2 {
    pub cluster_id: String,
    pub hummock_version: HummockVersion,
    pub version_stats: model_v2::hummock_version_stats::Model,
    pub compaction_configs: Vec<model_v2::compaction_config::Model>,
    // TODO other metadata
}

impl Display for MetadataV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "cluster_id:")?;
        writeln!(f, "{:#?}", self.cluster_id)?;
        writeln!(f, "hummock_version:")?;
        writeln!(f, "{:#?}", self.hummock_version)?;
        writeln!(f, "version_stats:")?;
        writeln!(f, "{:#?}", self.version_stats)?;
        writeln!(f, "compaction_configs:")?;
        writeln!(f, "{:#?}", self.compaction_configs)?;
        // TODO: other metadata
        Ok(())
    }
}

impl Metadata for MetadataV2 {
    fn encode_to(&self, buf: &mut Vec<u8>) -> BackupResult<()> {
        put_with_len_prefix(buf, &self.cluster_id)?;
        put_with_len_prefix(buf, &self.hummock_version)?;
        put_with_len_prefix(buf, &self.version_stats)?;
        put_with_len_prefix(buf, &self.compaction_configs)?;
        // TODO: other metadata
        Ok(())
    }

    fn decode(mut buf: &[u8]) -> BackupResult<Self>
    where
        Self: Sized,
    {
        let cluster_id = get_with_len_prefix(&mut buf)?;
        let hummock_version = get_with_len_prefix(&mut buf)?;
        let version_stats = get_with_len_prefix(&mut buf)?;
        let compaction_configs = get_with_len_prefix(&mut buf)?;
        // TODO: other metadata
        Ok(Self {
            cluster_id,
            hummock_version,
            version_stats,
            compaction_configs,
        })
    }

    fn hummock_version_ref(&self) -> &HummockVersion {
        &self.hummock_version
    }

    fn hummock_version(self) -> HummockVersion {
        self.hummock_version
    }
}

fn put_with_len_prefix<T: Serialize>(buf: &mut Vec<u8>, data: &T) -> Result<(), bincode::Error> {
    let b = bincode::serialize(data)?;
    buf.put_u32_le(
        b.len()
            .try_into()
            .unwrap_or_else(|_| panic!("cannot convert {} into u32", b.len())),
    );
    buf.put_slice(&b);
    Ok(())
}

fn get_with_len_prefix<'a, T: Deserialize<'a>>(buf: &mut &'a [u8]) -> Result<T, bincode::Error> {
    let len = buf.get_u32_le() as usize;
    let d = bincode::deserialize(&buf[..len])?;
    buf.advance(len);
    Ok(d)
}
