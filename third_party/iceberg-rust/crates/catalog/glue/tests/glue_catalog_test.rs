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

//! Integration tests for glue catalog.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;

use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result, TableCreation, TableIdent,
};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalog, GlueCatalogBuilder,
};
use iceberg_test_utils::{
    cleanup_namespace, get_glue_endpoint, get_minio_endpoint, normalize_test_name_with_parts,
    set_up,
};
use tokio::time::sleep;
use tracing::info;

async fn get_catalog() -> GlueCatalog {
    set_up();

    let glue_endpoint = get_glue_endpoint();
    let minio_endpoint = get_minio_endpoint();

    let props = HashMap::from([
        (AWS_ACCESS_KEY_ID.to_string(), "my_access_id".to_string()),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            "my_secret_key".to_string(),
        ),
        (AWS_REGION_NAME.to_string(), "us-east-1".to_string()),
        (S3_ENDPOINT.to_string(), minio_endpoint),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    // Wait for bucket to actually exist
    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            info!("S3 bucket 'warehouse' is ready");
            break;
        }
        info!("Waiting for bucket creation... (attempt {})", retries + 1);
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    let mut glue_props = HashMap::from([
        (GLUE_CATALOG_PROP_URI.to_string(), glue_endpoint),
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
    ]);
    glue_props.extend(props.clone());

    GlueCatalogBuilder::default()
        .load("glue", glue_props)
        .await
        .unwrap()
}

async fn set_test_namespace(catalog: &GlueCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();
    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: Option<String>, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let builder = TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .location_opt(location)
        .schema(schema);

    Ok(builder.build())
}

#[tokio::test]
async fn test_rename_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    // Use unique namespace to avoid conflicts
    let namespace = Namespace::new(NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_rename_table"
    )));
    cleanup_namespace(&catalog, namespace.name()).await;

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    catalog.rename_table(table.identifier(), &dest).await?;

    let table = catalog.load_table(&dest).await?;
    assert_eq!(table.identifier(), &dest);

    let src = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let src_table_exists = catalog.table_exists(&src).await?;
    assert!(!src_table_exists);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    // Use unique namespace to avoid conflicts
    let namespace = Namespace::new(NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_table_exists"
    )));
    cleanup_namespace(&catalog, namespace.name()).await;

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let ident = TableIdent::new(namespace.name().clone(), "my_table".to_string());

    let exists = catalog.table_exists(&ident).await?;
    assert!(!exists);

    let table = catalog.create_table(namespace.name(), creation).await?;

    let exists = catalog.table_exists(table.identifier()).await?;

    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    // Use unique namespace to avoid conflicts
    let namespace = Namespace::new(NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_drop_table"
    )));
    cleanup_namespace(&catalog, namespace.name()).await;

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    catalog.drop_table(table.identifier()).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    // Use unique namespace to avoid conflicts
    let namespace = Namespace::new(NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_load_table"
    )));
    cleanup_namespace(&catalog, namespace.name()).await;

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(result.identifier(), expected.identifier());
    assert_eq!(result.metadata_location(), expected.metadata_location());
    assert_eq!(result.metadata(), expected.metadata());

    Ok(())
}

#[tokio::test]
async fn test_create_table() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_create_table"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;
    // inject custom location, ignore the namespace prefix
    let creation = set_table_creation(Some("s3a://warehouse/hive".into()), "my_table")?;
    let result = catalog.create_table(&namespace, creation).await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(
        result
            .metadata_location()
            .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-"))
    );
    assert!(
        catalog
            .file_io()
            .exists("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_list_tables"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let expected = vec![];
    let result = catalog.list_tables(&namespace).await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_drop_namespace"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    catalog.drop_namespace(&namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_update_namespace"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let before_update = catalog.get_namespace(&namespace).await?;
    let before_update = before_update.properties().get("description");

    assert_eq!(before_update, None);

    let properties = HashMap::from([("description".to_string(), "my_update".to_string())]);

    catalog.update_namespace(&namespace, properties).await?;

    let after_update = catalog.get_namespace(&namespace).await?;
    let after_update = after_update.properties().get("description");

    assert_eq!(after_update, Some("my_update".to_string()).as_ref());

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_namespace_exists"));
    cleanup_namespace(&catalog, &namespace).await;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(!exists);

    set_test_namespace(&catalog, &namespace).await?;

    let exists = catalog.namespace_exists(&namespace).await?;
    assert!(exists);

    Ok(())
}

#[tokio::test]
async fn test_get_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_get_namespace"));
    cleanup_namespace(&catalog, &namespace).await;

    let does_not_exist = catalog.get_namespace(&namespace).await;
    assert!(does_not_exist.is_err());

    set_test_namespace(&catalog, &namespace).await?;

    let result = catalog.get_namespace(&namespace).await?;
    let expected = Namespace::new(namespace);

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let properties = HashMap::new();
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_create_namespace"));
    cleanup_namespace(&catalog, &namespace).await;

    let expected = Namespace::new(namespace.clone());

    let result = catalog.create_namespace(&namespace, properties).await?;

    assert_eq!(result, expected);

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_list_namespace"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let result = catalog.list_namespaces(None).await?;
    assert!(result.contains(&namespace));

    let empty_result = catalog.list_namespaces(Some(&namespace)).await?;
    assert!(empty_result.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_update_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    // Use unique namespace to avoid conflicts
    let namespace = Namespace::new(NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_update_table"
    )));
    cleanup_namespace(&catalog, namespace.name()).await;

    catalog
        .create_namespace(namespace.name(), HashMap::new())
        .await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let table = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(table.identifier(), expected.identifier());
    assert_eq!(table.metadata_location(), expected.metadata_location());
    assert_eq!(table.metadata(), expected.metadata());

    // Store the original metadata location for comparison
    let original_metadata_location = table.metadata_location();

    // Update table properties using the transaction
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("test_property".to_string(), "test_value".to_string())
        .apply(tx)?;

    // Commit the transaction to the catalog
    let updated_table = tx.commit(&catalog).await?;

    // Verify the update was successful
    assert_eq!(
        updated_table.metadata().properties().get("test_property"),
        Some(&"test_value".to_string())
    );

    // Verify the metadata location has been updated
    assert_ne!(
        updated_table.metadata_location(),
        original_metadata_location,
        "Metadata location should be updated after commit"
    );

    // Load the table again from the catalog to verify changes were persisted
    let reloaded_table = catalog.load_table(table.identifier()).await?;

    // Verify the reloaded table matches the updated table
    assert_eq!(
        reloaded_table.metadata().properties().get("test_property"),
        Some(&"test_value".to_string())
    );
    assert_eq!(
        reloaded_table.metadata_location(),
        updated_table.metadata_location(),
        "Reloaded table should have the same metadata location as the updated table"
    );

    Ok(())
}

#[tokio::test]
async fn test_register_table() -> Result<()> {
    let catalog = get_catalog().await;
    // Use unique namespace to avoid conflicts
    let namespace = NamespaceIdent::new(normalize_test_name_with_parts!("test_register_table"));
    cleanup_namespace(&catalog, &namespace).await;
    set_test_namespace(&catalog, &namespace).await?;

    let location = format!("s3a://warehouse/hive/{namespace}");
    let creation = set_table_creation(Some(location), "my_table")?;
    let table = catalog.create_table(&namespace, creation).await?;
    let metadata_location = table
        .metadata_location()
        .expect("Expected metadata location to be set")
        .to_string();

    catalog.drop_table(table.identifier()).await?;
    let ident = TableIdent::new(namespace.clone(), "my_table".to_string());

    let registered = catalog
        .register_table(&ident, metadata_location.clone())
        .await?;

    assert_eq!(registered.identifier(), &ident);
    assert_eq!(
        registered.metadata_location(),
        Some(metadata_location.as_str())
    );

    Ok(())
}
