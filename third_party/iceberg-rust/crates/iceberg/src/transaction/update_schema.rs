// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::spec::{NestedFieldRef, Schema};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{TableRequirement, TableUpdate};

/// Transaction action for updating table schema.
///
/// This action allows adding and dropping top-level fields in a table's schema during a
/// transaction.
pub struct UpdateSchemaAction {
    fields_to_add: Vec<NestedFieldRef>,
    field_names_to_drop: HashSet<String>,
}

impl UpdateSchemaAction {
    /// Creates a new [`UpdateSchemaAction`] with no fields to add.
    pub fn new() -> Self {
        UpdateSchemaAction {
            fields_to_add: vec![],
            field_names_to_drop: HashSet::new(),
        }
    }

    /// Adds a single field to the schema.
    ///
    /// # Arguments
    ///
    /// * `field` - The field to add to the schema.
    ///
    /// # Returns
    ///
    /// The updated [`UpdateSchemaAction`] with the field added.
    pub fn add_field(mut self, field: NestedFieldRef) -> Self {
        self.fields_to_add.push(field);
        self
    }

    /// Adds multiple fields to the schema.
    ///
    /// # Arguments
    ///
    /// * `fields` - An iterable of fields to add to the schema.
    ///
    /// # Returns
    ///
    /// The updated [`UpdateSchemaAction`] with the fields added.
    pub fn add_fields(mut self, fields: impl IntoIterator<Item = NestedFieldRef>) -> Self {
        self.fields_to_add.extend(fields);
        self
    }

    /// Drops a single top-level field from the schema by name.
    pub fn drop_field(mut self, field_name: impl Into<String>) -> Self {
        self.field_names_to_drop.insert(field_name.into());
        self
    }

    /// Drops multiple top-level fields from the schema by name.
    pub fn drop_fields(mut self, field_names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.field_names_to_drop
            .extend(field_names.into_iter().map(Into::into));
        self
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let current_schema = table.metadata().current_schema();
        let metadata = table.metadata();

        for field_name in &self.field_names_to_drop {
            if current_schema.field_by_name(field_name).is_none() {
                return Err(crate::Error::new(
                    crate::ErrorKind::DataInvalid,
                    format!("Cannot drop unknown field '{field_name}' from schema"),
                ));
            }
        }

        // Build new schema with retained fields + new fields.
        let mut all_fields = current_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| !self.field_names_to_drop.contains(&field.name))
            .cloned()
            .collect::<Vec<_>>();
        all_fields.extend(self.fields_to_add.iter().cloned());

        let identifier_field_ids = current_schema
            .identifier_field_ids()
            .filter(|field_id| {
                current_schema
                    .name_by_field_id(*field_id)
                    .is_some_and(|name| !self.field_names_to_drop.contains(name))
            })
            .collect::<Vec<_>>();

        // Create new schema with incremented schema ID
        // Find the max schema ID and increment it
        let new_schema_id = metadata
            .schemas_iter()
            .map(|s| s.schema_id())
            .max()
            .unwrap_or(0)
            + 1;
        let new_schema = Schema::builder()
            .with_schema_id(new_schema_id)
            .with_fields(all_fields)
            .with_identifier_field_ids(identifier_field_ids)
            .build()?;

        let updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];

        let requirements = vec![
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id: metadata.last_column_id(),
            },
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: current_schema.schema_id(),
            },
        ];

        Ok(ActionCommit::new(updates, requirements))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use as_any::Downcast;

    use crate::spec::{NestedField, PrimitiveType, Type};
    use crate::transaction::tests::make_v2_table;
    use crate::transaction::update_schema::UpdateSchemaAction;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    #[test]
    fn test_add_single_field() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let new_field = Arc::new(NestedField::optional(
            100,
            "new_field",
            Type::Primitive(PrimitiveType::String),
        ));

        let tx = tx
            .update_schema()
            .add_field(new_field.clone())
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .unwrap();

        assert_eq!(action.fields_to_add.len(), 1);
        assert_eq!(action.fields_to_add[0].name, "new_field");
    }

    #[test]
    fn test_add_multiple_fields() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let field1 = Arc::new(NestedField::optional(
            100,
            "field1",
            Type::Primitive(PrimitiveType::String),
        ));
        let field2 = Arc::new(NestedField::required(
            101,
            "field2",
            Type::Primitive(PrimitiveType::Int),
        ));

        let tx = tx
            .update_schema()
            .add_fields(vec![field1.clone(), field2.clone()])
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .unwrap();

        assert_eq!(action.fields_to_add.len(), 2);
        assert_eq!(action.fields_to_add[0].name, "field1");
        assert_eq!(action.fields_to_add[1].name, "field2");
    }

    #[test]
    fn test_chained_add_field() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let field1 = Arc::new(NestedField::optional(
            100,
            "field1",
            Type::Primitive(PrimitiveType::String),
        ));
        let field2 = Arc::new(NestedField::required(
            101,
            "field2",
            Type::Primitive(PrimitiveType::Int),
        ));

        let tx = tx
            .update_schema()
            .add_field(field1.clone())
            .add_field(field2.clone())
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .unwrap();

        assert_eq!(action.fields_to_add.len(), 2);
        assert_eq!(action.fields_to_add[0].name, "field1");
        assert_eq!(action.fields_to_add[1].name, "field2");
    }

    #[test]
    fn test_drop_multiple_fields() {
        let table = make_v2_table();
        let tx = Transaction::new(&table);

        let tx = tx
            .update_schema()
            .drop_field("x")
            .drop_fields(["z"])
            .apply(tx)
            .unwrap();

        assert_eq!(tx.actions.len(), 1);

        let action = (*tx.actions[0])
            .downcast_ref::<UpdateSchemaAction>()
            .unwrap();

        assert!(action.field_names_to_drop.contains("x"));
        assert!(action.field_names_to_drop.contains("z"));
    }

    #[tokio::test]
    async fn test_commit_action() {
        use crate::transaction::TransactionAction;
        use crate::{TableRequirement, TableUpdate};

        let table = make_v2_table();
        let original_schema = table.metadata().current_schema();
        let original_schema_id = original_schema.schema_id();
        let original_field_count = original_schema.as_struct().fields().len();
        let last_column_id = table.metadata().last_column_id();

        let new_field = Arc::new(NestedField::optional(
            100,
            "new_column",
            Type::Primitive(PrimitiveType::String),
        ));

        let action = Arc::new(UpdateSchemaAction::new().add_field(new_field));

        let action_commit = action.commit(&table).await.unwrap();
        let mut action_commit = action_commit;

        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        // Verify we have the correct updates
        assert_eq!(updates.len(), 2);

        // First update should be AddSchema
        match &updates[0] {
            TableUpdate::AddSchema { schema } => {
                // New schema should have incremented ID
                assert_eq!(schema.schema_id(), original_schema_id + 1);
                // New schema should have one more field
                assert_eq!(schema.as_struct().fields().len(), original_field_count + 1);
                // New field should be present
                assert!(schema.field_by_name("new_column").is_some());
                // Old fields should still be present
                for field in original_schema.as_struct().fields() {
                    assert!(
                        schema.field_by_name(&field.name).is_some(),
                        "Field {} should be present in new schema",
                        field.name
                    );
                }
            }
            _ => panic!("Expected AddSchema update"),
        }

        // Second update should be SetCurrentSchema with -1
        match &updates[1] {
            TableUpdate::SetCurrentSchema { schema_id } => {
                assert_eq!(schema_id, &-1);
            }
            _ => panic!("Expected SetCurrentSchema update"),
        }

        // Verify requirements
        assert_eq!(requirements.len(), 2);

        // Should have LastAssignedFieldIdMatch requirement
        assert!(requirements.iter().any(|r| matches!(
            r,
            TableRequirement::LastAssignedFieldIdMatch {
                last_assigned_field_id
            } if last_assigned_field_id == &last_column_id
        )));

        // Should have CurrentSchemaIdMatch requirement
        assert!(requirements.iter().any(
            |r| matches!(r, TableRequirement::CurrentSchemaIdMatch { current_schema_id } if current_schema_id == &original_schema_id)
        ));
    }

    #[tokio::test]
    async fn test_commit_drop_action() {
        use crate::TableUpdate;
        use crate::transaction::TransactionAction;

        let table = make_v2_table();
        let original_schema = table.metadata().current_schema();
        let original_schema_id = original_schema.schema_id();
        let action = Arc::new(UpdateSchemaAction::new().drop_field("x").drop_field("z"));
        let action_commit = action.commit(&table).await.unwrap();
        let mut action_commit = action_commit;

        let updates = action_commit.take_updates();

        match &updates[0] {
            TableUpdate::AddSchema { schema } => {
                assert_eq!(schema.schema_id(), original_schema_id + 1);
                assert!(schema.field_by_name("x").is_none());
                assert!(schema.field_by_name("z").is_none());
                assert!(schema.field_by_name("y").is_some());
                assert_eq!(schema.identifier_field_ids().collect::<Vec<_>>(), vec![2]);
                assert_eq!(schema.highest_field_id(), 2);
            }
            _ => panic!("Expected AddSchema update"),
        }
    }
}
