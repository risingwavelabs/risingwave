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

//! Integration tests for rest catalog.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique namespaces based on module path to avoid conflicts.

use std::collections::HashMap;

use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
use iceberg_test_utils::{
    cleanup_namespace, get_rest_catalog_endpoint, normalize_test_name_with_parts, set_up,
};
use tokio::time::sleep;
use tracing::info;

async fn get_catalog() -> RestCatalog {
    set_up();

    let rest_endpoint = get_rest_catalog_endpoint();

    // Wait for catalog to be ready
    let client = reqwest::Client::new();
    let mut retries = 0;
    while retries < 30 {
        match client
            .get(format!("{rest_endpoint}/v1/config"))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("REST catalog is ready at {}", rest_endpoint);
                break;
            }
            _ => {
                info!(
                    "Waiting for REST catalog to be ready... (attempt {})",
                    retries + 1
                );
                sleep(std::time::Duration::from_millis(1000)).await;
                retries += 1;
            }
        }
    }

    RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), rest_endpoint)]),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_get_non_exist_namespace() {
    let catalog = get_catalog().await;

    // Use unique namespace name to ensure it doesn't exist
    let ns_ident = NamespaceIdent::new(normalize_test_name_with_parts!(
        "test_get_non_exist_namespace"
    ));
    // Clean up from any previous test runs
    cleanup_namespace(&catalog, &ns_ident).await;

    let result = catalog.get_namespace(&ns_ident).await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));
}

#[tokio::test]
async fn test_get_namespace() {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts with other tests
    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs([
            "apple",
            "ios",
            &normalize_test_name_with_parts!("test_get_namespace"),
        ])
        .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns.name()).await;

    // Verify that namespace doesn't exist
    assert!(catalog.get_namespace(ns.name()).await.is_err());

    // Create this namespace
    let created_ns = catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    assert_eq!(ns.name(), created_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());

    // Check that this namespace already exists
    let get_ns = catalog.get_namespace(ns.name()).await.unwrap();
    assert_eq!(ns.name(), get_ns.name());
    assert_map_contains(ns.properties(), created_ns.properties());
}

#[tokio::test]
async fn test_list_namespace() {
    let catalog = get_catalog().await;

    // Use unique parent namespace to avoid conflicts
    let parent_ns_name = normalize_test_name_with_parts!("test_list_namespace");
    let parent_ident = NamespaceIdent::from_strs([&parent_ns_name]).unwrap();

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs([&parent_ns_name, "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs([&parent_ns_name, "macos"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns1.name()).await;
    cleanup_namespace(&catalog, ns2.name()).await;
    cleanup_namespace(&catalog, &parent_ident).await;

    // Currently this namespace doesn't exist, so it should return error.
    assert!(catalog.list_namespaces(Some(&parent_ident)).await.is_err());

    // Create namespaces
    catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog.list_namespaces(Some(&parent_ident)).await.unwrap();

    assert!(nss.contains(ns1.name()));
    assert!(nss.contains(ns2.name()));
}

#[tokio::test]
async fn test_list_empty_namespace() {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let ns_apple = Namespace::with_properties(
        NamespaceIdent::from_strs([
            "list_empty",
            "apple",
            &normalize_test_name_with_parts!("test_list_empty_namespace"),
        ])
        .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns_apple.name()).await;

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(ns_apple.name()))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns_apple.name(), ns_apple.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog
        .list_namespaces(Some(ns_apple.name()))
        .await
        .unwrap();
    assert!(nss.is_empty());
}

#[tokio::test]
async fn test_list_root_namespace() {
    let catalog = get_catalog().await;

    // Use unique root namespace to avoid conflicts
    let root_ns_name = normalize_test_name_with_parts!("test_list_root_namespace");
    let root_ident = NamespaceIdent::from_strs([&root_ns_name]).unwrap();

    let ns1 = Namespace::with_properties(
        NamespaceIdent::from_strs([&root_ns_name, "apple", "ios"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    let ns2 = Namespace::with_properties(
        NamespaceIdent::from_strs([&root_ns_name, "google", "android"]).unwrap(),
        HashMap::from([
            ("owner".to_string(), "xuanwo".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns1.name()).await;
    cleanup_namespace(&catalog, ns2.name()).await;
    cleanup_namespace(&catalog, &root_ident).await;

    // Currently this namespace doesn't exist, so it should return error.
    assert!(catalog.list_namespaces(Some(&root_ident)).await.is_err());

    // Create namespaces
    catalog
        .create_namespace(ns1.name(), ns1.properties().clone())
        .await
        .unwrap();
    catalog
        .create_namespace(ns2.name(), ns1.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog.list_namespaces(None).await.unwrap();
    assert!(nss.contains(&root_ident));
}

#[tokio::test]
async fn test_create_table() {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs([
            "create_table",
            "apple",
            "ios",
            &normalize_test_name_with_parts!("test_create_table"),
        ])
        .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns.name()).await;

    // Create namespaces
    catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    assert_eq!(
        table.identifier(),
        &TableIdent::new(ns.name().clone(), "t1".to_string())
    );

    assert_eq!(
        table.metadata().current_schema().as_struct(),
        schema.as_struct()
    );
    assert_eq!(table.metadata().format_version(), FormatVersion::V2);
    assert!(table.metadata().current_snapshot().is_none());
    assert!(table.metadata().history().is_empty());
    assert!(table.metadata().default_sort_order().is_unsorted());
    assert!(table.metadata().default_partition_spec().is_unpartitioned());
}

#[tokio::test]
async fn test_update_table() {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let ns = Namespace::with_properties(
        NamespaceIdent::from_strs([
            "update_table",
            "apple",
            "ios",
            &normalize_test_name_with_parts!("test_update_table"),
        ])
        .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns.name()).await;

    // Create namespaces
    catalog
        .create_namespace(ns.name(), ns.properties().clone())
        .await
        .unwrap();

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .build()
        .unwrap();

    // Now we create a table
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .build();

    let table = catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    assert_eq!(
        table.identifier(),
        &TableIdent::new(ns.name().clone(), "t1".to_string())
    );

    let tx = Transaction::new(&table);
    // Update table by committing transaction
    let table2 = tx
        .update_table_properties()
        .set("prop1".to_string(), "v1".to_string())
        .apply(tx)
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();

    assert_map_contains(
        &HashMap::from([("prop1".to_string(), "v1".to_string())]),
        table2.metadata().properties(),
    );
}

fn assert_map_contains(map1: &HashMap<String, String>, map2: &HashMap<String, String>) {
    for (k, v) in map1 {
        assert!(map2.contains_key(k));
        assert_eq!(map2.get(k).unwrap(), v);
    }
}

#[tokio::test]
async fn test_list_empty_multi_level_namespace() {
    let catalog = get_catalog().await;

    // Use unique namespace to avoid conflicts
    let ns_apple = Namespace::with_properties(
        NamespaceIdent::from_strs([
            "multi_level",
            "a_a",
            "apple",
            &normalize_test_name_with_parts!("test_list_empty_multi_level_namespace"),
        ])
        .unwrap(),
        HashMap::from([
            ("owner".to_string(), "ray".to_string()),
            ("community".to_string(), "apache".to_string()),
        ]),
    );

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, ns_apple.name()).await;

    // Currently this namespace doesn't exist, so it should return error.
    assert!(
        catalog
            .list_namespaces(Some(ns_apple.name()))
            .await
            .is_err()
    );

    // Create namespaces
    catalog
        .create_namespace(ns_apple.name(), ns_apple.properties().clone())
        .await
        .unwrap();

    // List namespace
    let nss = catalog
        .list_namespaces(Some(ns_apple.name()))
        .await
        .unwrap();
    assert!(nss.is_empty());
}

#[tokio::test]
async fn test_register_table() {
    let catalog = get_catalog().await;

    // Create unique namespace to avoid conflicts
    let ns = NamespaceIdent::new(normalize_test_name_with_parts!("test_register_table"));

    // Clean up from any previous test runs
    cleanup_namespace(&catalog, &ns).await;

    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();

    // Create the table, store the metadata location, drop the table
    let empty_schema = Schema::builder().build().unwrap();
    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(empty_schema)
        .build();

    let table = catalog.create_table(&ns, table_creation).await.unwrap();

    let metadata_location = table.metadata_location().unwrap();
    catalog.drop_table(table.identifier()).await.unwrap();

    let new_table_identifier = TableIdent::new(ns.clone(), "t2".to_string());
    let table_registered = catalog
        .register_table(&new_table_identifier, metadata_location.to_string())
        .await
        .unwrap();

    assert_eq!(
        table.metadata_location(),
        table_registered.metadata_location()
    );
    assert_ne!(
        table.identifier().to_string(),
        table_registered.identifier().to_string()
    );
}
