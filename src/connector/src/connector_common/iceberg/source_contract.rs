// Copyright 2026 RisingWave Labs
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

//! Writer/reader contract for a retracting source over PK-index sink output.
//!
//! Publishing these properties is a rollout gate, not just a schema annotation. In particular,
//! every writer must support pure compaction before a table can advertise this contract.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail, ensure};
use iceberg::spec::Schema;
use serde::{Deserialize, Serialize};

pub const SOURCE_CONTRACT_VERSION: &str = "risingwave.source-contract.version";
pub const SOURCE_CONTRACT_WRITER: &str = "risingwave.source-contract.writer";
pub const SOURCE_CONTRACT_KEY_FIELD_IDS: &str = "risingwave.source-contract.key-field-ids";
pub const COMMIT_KIND: &str = "risingwave.commit.kind";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum ContractVersion {
    #[serde(rename = "1")]
    V1,
}

/// Field IDs are the complete, ordered sink stream key, not Iceberg identifier fields.
/// Nullable and physically stored hidden keys are intentional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergSourceContract {
    version: ContractVersion,
    key_field_ids: Vec<i32>,
}

impl IcebergSourceContract {
    pub fn new(schema: &Schema, key_field_ids: Vec<i32>) -> Result<Self> {
        let contract = Self {
            version: ContractVersion::V1,
            key_field_ids,
        };
        contract.validate_schema(schema)?;
        Ok(contract)
    }

    pub fn key_field_ids(&self) -> &[i32] {
        &self.key_field_ids
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        ensure!(
            !self.key_field_ids.is_empty(),
            "Iceberg source contract has an empty key"
        );
        let mut seen = HashSet::new();
        for id in &self.key_field_ids {
            ensure!(
                *id > 0 && seen.insert(*id),
                "invalid or duplicate Iceberg source key field ID"
            );
            ensure!(
                schema
                    .as_struct()
                    .fields()
                    .iter()
                    .any(|field| field.id == *id),
                "Iceberg source key must reference stored top-level fields"
            );
        }
        Ok(())
    }

    /// Call after sink/table column-order validation. Do not use RisingWave column IDs here.
    pub fn from_key_indices(schema: &Schema, key_indices: &[usize]) -> Result<Self> {
        let fields = schema.as_struct().fields();
        let ids = key_indices
            .iter()
            .map(|index| {
                fields
                    .get(*index)
                    .map(|field| field.id)
                    .context("Iceberg sink key index is outside the table schema")
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(schema, ids)
    }

    pub fn validate_sink_key(
        &self,
        schema: &Schema,
        enable_pk_index: bool,
        key_indices: Option<&[usize]>,
    ) -> Result<()> {
        ensure!(
            enable_pk_index,
            "an Iceberg source-contract table requires an enable_pk_index sink"
        );
        let key_indices =
            key_indices.context("Iceberg source-contract sink is missing its actual key")?;
        ensure!(
            Self::from_key_indices(schema, key_indices)? == *self,
            "Iceberg sink key does not match the table source contract"
        );
        Ok(())
    }

    /// Absence is legacy. A partially advertised or unknown contract is never legacy.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        schema: &Schema,
    ) -> Result<Option<Self>> {
        if [
            SOURCE_CONTRACT_VERSION,
            SOURCE_CONTRACT_WRITER,
            SOURCE_CONTRACT_KEY_FIELD_IDS,
        ]
        .iter()
        .all(|key| !properties.contains_key(*key))
        {
            return Ok(None);
        }
        validate_version(properties)?;
        ensure!(
            properties.get(SOURCE_CONTRACT_WRITER).map(String::as_str) == Some("pk-index"),
            "missing or unsupported Iceberg source contract writer"
        );
        let ids = properties
            .get(SOURCE_CONTRACT_KEY_FIELD_IDS)
            .context("missing Iceberg source contract key field IDs")?;
        let ids =
            serde_json::from_str(ids).context("invalid Iceberg source contract key field IDs")?;
        Self::new(schema, ids).map(Some)
    }

    pub fn to_properties(&self) -> HashMap<String, String> {
        HashMap::from([
            (SOURCE_CONTRACT_VERSION.to_owned(), "1".to_owned()),
            (SOURCE_CONTRACT_WRITER.to_owned(), "pk-index".to_owned()),
            (
                SOURCE_CONTRACT_KEY_FIELD_IDS.to_owned(),
                serde_json::to_string(&self.key_field_ids).expect("field IDs are serializable"),
            ),
        ])
    }
}

fn validate_version(properties: &HashMap<String, String>) -> Result<()> {
    ensure!(
        properties.get(SOURCE_CONTRACT_VERSION).map(String::as_str) == Some("1"),
        "missing or unsupported Iceberg source contract version"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IcebergCommitKind {
    Data,
    Compaction,
}

impl IcebergCommitKind {
    /// Only snapshot summaries, not table properties, may determine commit kind.
    pub fn from_properties(properties: &HashMap<String, String>) -> Result<Option<Self>> {
        if !properties.contains_key(SOURCE_CONTRACT_VERSION)
            && !properties.contains_key(COMMIT_KIND)
        {
            return Ok(None);
        }
        validate_version(properties)?;
        match properties.get(COMMIT_KIND).map(String::as_str) {
            Some("data") => Ok(Some(Self::Data)),
            Some("compaction") => Ok(Some(Self::Compaction)),
            _ => bail!("missing or unsupported Iceberg commit kind"),
        }
    }

    pub fn to_properties(self) -> HashMap<String, String> {
        HashMap::from([
            (SOURCE_CONTRACT_VERSION.to_owned(), "1".to_owned()),
            (
                COMMIT_KIND.to_owned(),
                match self {
                    Self::Data => "data",
                    Self::Compaction => "compaction",
                }
                .to_owned(),
            ),
        ])
    }
}

/// Persisted with the files and snapshot ID. Recovery must not infer this from current metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergSourceCommit {
    pub table_uuid: uuid::Uuid,
    pub contract: IcebergSourceContract,
    pub kind: IcebergCommitKind,
}

impl IcebergSourceCommit {
    pub fn validate_snapshot(&self, properties: &HashMap<String, String>) -> Result<()> {
        ensure!(
            IcebergCommitKind::from_properties(properties)? == Some(self.kind),
            "Iceberg snapshot commit marker does not match durable pre-commit state"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{NestedField, PrimitiveType, StructType, Type};

    use super::*;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(7, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(42, "_row_id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(
                    50,
                    "nested",
                    Type::Struct(StructType::new(vec![
                        NestedField::optional(51, "key", Type::Primitive(PrimitiveType::Long))
                            .into(),
                    ])),
                )
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn key_uses_actual_ids_and_keeps_nullable_stored_hidden_columns() -> Result<()> {
        let schema = schema();
        let contract = IcebergSourceContract::from_key_indices(&schema, &[1, 0])?;
        assert_eq!(contract.key_field_ids(), &[42, 7]);
        assert_eq!(
            IcebergSourceContract::from_properties(&contract.to_properties(), &schema)?,
            Some(contract)
        );
        assert!(IcebergSourceContract::from_key_indices(&schema, &[3]).is_err());
        Ok(())
    }

    #[test]
    fn invalid_keys_are_rejected() {
        for ids in [vec![], vec![7, 7], vec![-1], vec![0], vec![99], vec![51]] {
            assert!(IcebergSourceContract::new(&schema(), ids).is_err());
        }
    }

    #[test]
    fn sink_must_use_the_same_complete_ordered_key() -> Result<()> {
        let schema = schema();
        let contract = IcebergSourceContract::new(&schema, vec![42, 7])?;
        contract.validate_sink_key(&schema, true, Some(&[1, 0]))?;
        assert!(
            contract
                .validate_sink_key(&schema, false, Some(&[1, 0]))
                .is_err()
        );
        assert!(contract.validate_sink_key(&schema, true, None).is_err());
        for indices in [vec![], vec![0], vec![0, 1], vec![1, 1], vec![3]] {
            assert!(
                contract
                    .validate_sink_key(&schema, true, Some(&indices))
                    .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn missing_partial_unknown_and_malformed_profiles() -> Result<()> {
        let schema = schema();
        assert_eq!(
            IcebergSourceContract::from_properties(&HashMap::new(), &schema)?,
            None
        );
        let properties = IcebergSourceContract::new(&schema, vec![7])?.to_properties();
        for key in [
            SOURCE_CONTRACT_VERSION,
            SOURCE_CONTRACT_WRITER,
            SOURCE_CONTRACT_KEY_FIELD_IDS,
        ] {
            let mut partial = properties.clone();
            partial.remove(key);
            assert!(IcebergSourceContract::from_properties(&partial, &schema).is_err());
        }
        for (key, value) in [
            (SOURCE_CONTRACT_VERSION, "2"),
            (SOURCE_CONTRACT_WRITER, "external"),
            (SOURCE_CONTRACT_KEY_FIELD_IDS, "not-json"),
            (SOURCE_CONTRACT_KEY_FIELD_IDS, "[7,7]"),
        ] {
            let mut invalid = properties.clone();
            invalid.insert(key.to_owned(), value.to_owned());
            assert!(IcebergSourceContract::from_properties(&invalid, &schema).is_err());
        }
        Ok(())
    }

    #[test]
    fn markers_are_explicit_and_versioned() -> Result<()> {
        assert_eq!(IcebergCommitKind::from_properties(&HashMap::new())?, None);
        for kind in [IcebergCommitKind::Data, IcebergCommitKind::Compaction] {
            let properties = kind.to_properties();
            assert_eq!(IcebergCommitKind::from_properties(&properties)?, Some(kind));
            for key in [SOURCE_CONTRACT_VERSION, COMMIT_KIND] {
                let mut partial = properties.clone();
                partial.remove(key);
                assert!(IcebergCommitKind::from_properties(&partial).is_err());
            }
        }
        for (version, kind) in [("2", "data"), ("1", "replace"), ("1", "mixed")] {
            let properties = HashMap::from([
                (SOURCE_CONTRACT_VERSION.to_owned(), version.to_owned()),
                (COMMIT_KIND.to_owned(), kind.to_owned()),
            ]);
            assert!(IcebergCommitKind::from_properties(&properties).is_err());
        }
        Ok(())
    }

    #[test]
    fn durable_commit_rejects_missing_or_conflicting_snapshot_marker() -> Result<()> {
        let commit = IcebergSourceCommit {
            table_uuid: uuid::Uuid::nil(),
            contract: IcebergSourceContract::new(&schema(), vec![7, 42])?,
            kind: IcebergCommitKind::Compaction,
        };
        let recovered: IcebergSourceCommit = serde_json::from_slice(&serde_json::to_vec(&commit)?)?;
        assert_eq!(recovered, commit);
        recovered.validate_snapshot(&IcebergCommitKind::Compaction.to_properties())?;
        assert!(
            recovered
                .validate_snapshot(&IcebergCommitKind::Data.to_properties())
                .is_err()
        );
        assert!(recovered.validate_snapshot(&HashMap::new()).is_err());
        let mut invalid = serde_json::to_value(commit)?;
        invalid["contract"]["version"] = serde_json::json!("2");
        assert!(serde_json::from_value::<IcebergSourceCommit>(invalid).is_err());
        Ok(())
    }
}
