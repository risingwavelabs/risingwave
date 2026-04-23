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

//! Integration tests for FileIO S3.
//!
//! These tests assume Docker containers are started externally via `make docker-up`.
//! Each test uses unique file paths based on module path to avoid conflicts.
#[cfg(all(test, feature = "storage-s3"))]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::TryStreamExt;
    use iceberg::io::{
        CustomAwsCredentialLoader, FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };
    use iceberg_test_utils::{get_minio_endpoint, normalize_test_name_with_parts, set_up};
    use reqsign::{AwsCredential, AwsCredentialLoad};
    use reqwest::Client;

    async fn get_file_io() -> FileIO {
        set_up();

        let minio_endpoint = get_minio_endpoint();

        FileIOBuilder::new("s3")
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_ACCESS_KEY_ID, "admin".to_string()),
                (S3_SECRET_ACCESS_KEY, "password".to_string()),
                (S3_REGION, "us-east-1".to_string()),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_file_io_s3_exists() {
        let file_io = get_file_io().await;
        assert!(!file_io.exists("s3://bucket2/any").await.unwrap());
        assert!(file_io.exists("s3://bucket1/").await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_output() {
        let file_io = get_file_io().await;
        // Use unique file path based on module path to avoid conflicts
        let output_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_output")
        );
        // Clean up from any previous test runs
        let _ = file_io.delete(&output_path).await;
        assert!(!file_io.exists(&output_path).await.unwrap());
        let output_file = file_io.new_output(&output_path).unwrap();
        {
            output_file.write("123".into()).await.unwrap();
        }
        assert!(file_io.exists(&output_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_io_s3_input() {
        let file_io = get_file_io().await;
        // Use unique file path based on module path to avoid conflicts
        let file_path = format!(
            "s3://bucket1/{}",
            normalize_test_name_with_parts!("test_file_io_s3_input")
        );
        let output_file = file_io.new_output(&file_path).unwrap();
        {
            output_file.write("test_input".into()).await.unwrap();
        }

        let input_file = file_io.new_input(&file_path).unwrap();

        {
            let buffer = input_file.read().await.unwrap();
            assert_eq!(buffer, "test_input".as_bytes());
        }
    }

    // Mock credential loader for testing
    struct MockCredentialLoader {
        credential: Option<AwsCredential>,
    }

    impl MockCredentialLoader {
        fn new(credential: Option<AwsCredential>) -> Self {
            Self { credential }
        }

        fn new_minio() -> Self {
            Self::new(Some(AwsCredential {
                access_key_id: "admin".to_string(),
                secret_access_key: "password".to_string(),
                session_token: None,
                expires_in: None,
            }))
        }
    }

    #[async_trait]
    impl AwsCredentialLoad for MockCredentialLoader {
        async fn load_credential(&self, _client: Client) -> anyhow::Result<Option<AwsCredential>> {
            Ok(self.credential.clone())
        }
    }

    #[test]
    fn test_file_io_builder_extension_system() {
        // Test adding and retrieving extensions
        let test_string = "test_extension_value".to_string();
        let builder = FileIOBuilder::new_fs_io().with_extension(test_string.clone());

        // Test retrieving the extension
        let extension: Option<Arc<String>> = builder.extension();
        assert!(extension.is_some());
        assert_eq!(*extension.unwrap(), test_string);

        // Test that non-existent extension returns None
        let non_existent: Option<Arc<i32>> = builder.extension();
        assert!(non_existent.is_none());
    }

    #[test]
    fn test_file_io_builder_multiple_extensions() {
        // Test adding multiple different types of extensions
        let test_string = "test_value".to_string();
        let test_number = 42i32;

        let builder = FileIOBuilder::new_fs_io()
            .with_extension(test_string.clone())
            .with_extension(test_number);

        // Retrieve both extensions
        let string_ext: Option<Arc<String>> = builder.extension();
        let number_ext: Option<Arc<i32>> = builder.extension();

        assert!(string_ext.is_some());
        assert!(number_ext.is_some());
        assert_eq!(*string_ext.unwrap(), test_string);
        assert_eq!(*number_ext.unwrap(), test_number);
    }

    #[test]
    fn test_custom_aws_credential_loader_instantiation() {
        // Test creating CustomAwsCredentialLoader with mock loader
        let mock_loader = MockCredentialLoader::new_minio();
        let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));

        // Test that the loader can be used in FileIOBuilder
        let builder = FileIOBuilder::new("s3")
            .with_extension(custom_loader.clone())
            .with_props(vec![
                (S3_ENDPOINT, "http://localhost:9000".to_string()),
                ("bucket", "test-bucket".to_string()),
                (S3_REGION, "us-east-1".to_string()),
            ]);

        // Verify the extension was stored
        let retrieved_loader: Option<Arc<CustomAwsCredentialLoader>> = builder.extension();
        assert!(retrieved_loader.is_some());
    }

    #[tokio::test]
    async fn test_s3_with_custom_credential_loader_integration() {
        let _file_io = get_file_io().await;

        // Create a mock credential loader
        let mock_loader = MockCredentialLoader::new_minio();
        let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));

        let minio_endpoint = get_minio_endpoint();

        // Build FileIO with custom credential loader
        let file_io_with_custom_creds = FileIOBuilder::new("s3")
            .with_extension(custom_loader)
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_REGION, "us-east-1".to_string()),
            ])
            .build()
            .unwrap();

        // Test that the FileIO was built successfully with the custom loader
        match file_io_with_custom_creds.exists("s3://bucket1/any").await {
            Ok(_) => {}
            Err(e) => panic!("Failed to check existence of bucket: {e}"),
        }
    }

    #[tokio::test]
    async fn test_s3_with_custom_credential_loader_integration_failure() {
        let _file_io = get_file_io().await;

        // Create a mock credential loader with no credentials
        let mock_loader = MockCredentialLoader::new(None);
        let custom_loader = CustomAwsCredentialLoader::new(Arc::new(mock_loader));

        let minio_endpoint = get_minio_endpoint();

        // Build FileIO with custom credential loader
        let file_io_with_custom_creds = FileIOBuilder::new("s3")
            .with_extension(custom_loader)
            .with_props(vec![
                (S3_ENDPOINT, minio_endpoint),
                (S3_REGION, "us-east-1".to_string()),
            ])
            .build()
            .unwrap();

        // Test that the FileIO was built successfully with the custom loader
        match file_io_with_custom_creds.exists("s3://bucket1/any").await {
            Ok(_) => panic!(
                "Expected error, but got Ok - the credential loader should fail to provide valid credentials"
            ),
            Err(e) => {
                assert!(
                    e.to_string()
                        .contains("no valid credential found and anonymous access is not allowed")
                );
            }
        }
    }

    #[tokio::test]
    async fn test_list_recursive_s3() {
        let file_io = get_file_io().await;

        // Create a comprehensive directory structure mimicking real Iceberg table
        let base_path = "s3://bucket1/test_list_recursive";

        // Metadata files (version-hint, metadata json, manifest lists, manifests)
        let version_hint = format!("{base_path}/metadata/version-hint.text");
        let metadata_v1 = format!("{base_path}/metadata/v1.metadata.json");
        let metadata_v2 = format!("{base_path}/metadata/v2.metadata.json");
        let manifest_list = format!("{base_path}/metadata/snap-123456789-1-abc.avro");
        let manifest1 = format!("{base_path}/metadata/abc-m0.avro");
        let manifest2 = format!("{base_path}/metadata/def-m1.avro");

        // Data files in various partition structures
        let data_unpartitioned = format!("{base_path}/data/00000-0-abc.parquet");
        let data_partition1 = format!("{base_path}/data/dt=2024-01-01/00001-0-def.parquet");
        let data_partition2 = format!("{base_path}/data/dt=2024-01-01/00002-0-ghi.parquet");
        let data_partition3 = format!("{base_path}/data/dt=2024-01-02/00003-0-jkl.parquet");

        // Multi-level nested partitions (e.g., dt/hour/region)
        let data_nested1 =
            format!("{base_path}/data/dt=2024-01-01/hour=10/region=us/00004-0-mno.parquet");
        let data_nested2 =
            format!("{base_path}/data/dt=2024-01-01/hour=10/region=eu/00005-0-pqr.parquet");
        let data_nested3 =
            format!("{base_path}/data/dt=2024-01-01/hour=11/region=us/00006-0-stu.parquet");

        // Delete files
        let delete_file = format!("{base_path}/data/dt=2024-01-01/00007-0-del.parquet");

        // Statistics files
        let stats_file = format!("{base_path}/metadata/stats-123.puffin");

        // Collect all files for verification
        let all_files = vec![
            &version_hint,
            &metadata_v1,
            &metadata_v2,
            &manifest_list,
            &manifest1,
            &manifest2,
            &data_unpartitioned,
            &data_partition1,
            &data_partition2,
            &data_partition3,
            &data_nested1,
            &data_nested2,
            &data_nested3,
            &delete_file,
            &stats_file,
        ];

        // Write all test files
        for (i, file_path) in all_files.iter().enumerate() {
            file_io
                .new_output(file_path.as_str())
                .unwrap()
                .write(format!("content_{i}").into())
                .await
                .unwrap();
        }

        // List recursively from base path
        let entries: Vec<_> = file_io
            .list(base_path, true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let listed_paths: std::collections::HashSet<String> =
            entries.iter().map(|e| e.path.clone()).collect();

        // Verify ALL files are found with EXACT path match
        for expected_path in &all_files {
            assert!(
                listed_paths.contains(expected_path.as_str()),
                "Missing file: '{expected_path}'\nListed paths: {listed_paths:?}",
            );
        }

        // Verify count matches
        assert_eq!(
            listed_paths.len(),
            all_files.len(),
            "Expected {} files but found {}.\nExpected: {:?}\nGot: {:?}",
            all_files.len(),
            listed_paths.len(),
            all_files,
            listed_paths
        );

        // Verify metadata for each entry
        for entry in &entries {
            assert!(
                !entry.metadata.is_dir,
                "Files should not be directories: {}",
                entry.path
            );
            assert!(
                entry.metadata.size > 0,
                "Files should have size > 0: {}",
                entry.path
            );
            assert!(
                entry.metadata.last_modified_ms.is_some(),
                "S3/minio should provide last_modified_ms: {}",
                entry.path
            );
        }

        // Cleanup
        for file_path in &all_files {
            file_io.delete(file_path.as_str()).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_list_recursive_path_format_consistency_s3() {
        // This test verifies that the paths returned by list_recursive
        // match exactly the paths that would be stored in manifest entries.
        // This is critical for delete_orphan_files to work correctly.

        let file_io = get_file_io().await;

        let table_location = "s3://bucket1/test_path_consistency";
        let data_file_path = format!("{table_location}/data/00000-0-abc.parquet");
        let manifest_path = format!("{table_location}/metadata/snap-123-0-abc.avro");

        // Simulate writing files like Iceberg would
        file_io
            .new_output(&data_file_path)
            .unwrap()
            .write("data".into())
            .await
            .unwrap();
        file_io
            .new_output(&manifest_path)
            .unwrap()
            .write("manifest".into())
            .await
            .unwrap();

        // List files like delete_orphan_files would
        let entries: Vec<_> = file_io
            .list(table_location, true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let listed_paths: std::collections::HashSet<String> =
            entries.into_iter().map(|e| e.path).collect();

        // The paths from list_recursive should match exactly what we wrote
        // This is the key assertion for delete_orphan_files correctness
        assert!(
            listed_paths.contains(&data_file_path),
            "list_recursive should return exact path '{data_file_path}' for HashSet.contains() to work.\nGot: {listed_paths:?}",
        );
        assert!(
            listed_paths.contains(&manifest_path),
            "list_recursive should return exact path '{manifest_path}' for HashSet.contains() to work.\nGot: {listed_paths:?}",
        );

        // Cleanup
        file_io.delete(&data_file_path).await.unwrap();
        file_io.delete(&manifest_path).await.unwrap();
    }
}
