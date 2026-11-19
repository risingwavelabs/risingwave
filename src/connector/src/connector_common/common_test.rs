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

#[cfg(test)]
mod tests {
    use std::io::Write;

    #[cfg(not(madsim))]
    use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
    use tempfile::NamedTempFile;
    use tokio;
    use url::Url;

    use crate::connector_common::common::*;

    fn create_test_pulsar_common() -> PulsarCommon {
        PulsarCommon {
            topic: "test-topic".to_owned(),
            service_url: "pulsar://localhost:6650".to_owned(),
            auth_token: None,
        }
    }

    fn create_test_aws_auth_props() -> AwsAuthProps {
        AwsAuthProps {
            region: Some("us-east-1".to_owned()),
            endpoint: None,
            access_key: None,
            secret_key: None,
            session_token: None,
            arn: None,
            external_id: None,
            profile: None,
            msk_signer_timeout_sec: None,
        }
    }

    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_file_scheme() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        // Create a temporary file for testing
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test credentials").unwrap();
        let temp_path = temp_file.path().to_str().unwrap();

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: format!("file://{}", temp_path),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_ok());
        let (url, temp_file_opt) = result.unwrap();
        assert_eq!(url, format!("file://{}", temp_path));
        assert!(temp_file_opt.is_none());
    }

    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_invalid_scheme() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: "http://example.com/credentials".to_owned(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid credentials_url scheme 'http'"));
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_absolute_path_existing_file() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        // Create a temporary file for testing
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"test credentials").unwrap();
        let temp_path = temp_file.path().to_str().unwrap().to_owned();

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: temp_path.clone(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_ok());
        let (url, temp_file_opt) = result.unwrap();
        assert_eq!(url, format!("file://{}", temp_path));
        assert!(temp_file_opt.is_none());
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_special_characters() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();
        let directory = tempfile::tempdir().unwrap();
        for name in [
            "oauth fixture.json",
            "oauth#fixture.json",
            "oauth?fixture.json",
            "oauth凭据.json",
            "oauth%20fixture.json",
            "directory with #?凭据%/oauth.json",
        ] {
            let credentials = serde_json::to_vec(&serde_json::json!({
                "client_id": name,
                "client_secret": "test-secret",
            }))
            .unwrap();
            let path = directory.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, &credentials).unwrap();

            for credentials_url in [
                path.to_str().unwrap().to_owned(),
                Url::from_file_path(&path).unwrap().to_string(),
            ] {
                let oauth = PulsarOauthCommon {
                    // Credential loading precedes issuer discovery. An invalid issuer makes
                    // initialization fail at discovery without making any network requests.
                    issuer_url: "invalid URL".to_owned(),
                    credentials_url,
                    audience: "test-audience".to_owned(),
                    scope: None,
                };
                let (resolved_url, temp_file) = pulsar_common
                    .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
                    .await
                    .unwrap();
                let url = Url::parse(&resolved_url).unwrap();

                let mut auth = OAuth2Authentication::client_credentials(OAuth2Params {
                    issuer_url: oauth.issuer_url,
                    credentials_url: resolved_url,
                    audience: Some(oauth.audience),
                    scope: oauth.scope,
                });
                let error = auth.initialize().await.unwrap_err().to_string();
                assert!(
                    error.contains("failed to discover OAuth2 token endpoint"),
                    "credential loading failed for {}: {error}",
                    oauth.credentials_url
                );

                let temp_file = temp_file.expect("encoded paths need a temporary file");
                let temp_path = temp_file.path().to_owned();
                assert_eq!(std::path::Path::new(url.path()), temp_path);
                assert_eq!(std::fs::read(url.path()).unwrap(), credentials);
                drop(temp_file);
                assert!(!temp_path.exists());
                assert_eq!(std::fs::read(&path).unwrap(), credentials);
            }
        }
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_missing_encoded_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing #?凭据.json");
        let oauth = PulsarOauthCommon {
            issuer_url: "invalid URL".to_owned(),
            credentials_url: Url::from_file_path(path).unwrap().to_string(),
            audience: "test-audience".to_owned(),
            scope: None,
        };
        let error = create_test_pulsar_common()
            .resolve_pulsar_credentials_url(&oauth, &create_test_aws_auth_props())
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to read pulsar credentials file")
        );
    }

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_absolute_path_nonexistent_file() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let nonexistent_path = "/tmp/nonexistent_credentials_file";

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: nonexistent_path.to_owned(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("credentials file does not exist"));
    }

    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_relative_path() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: "relative/path/to/credentials".to_owned(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains(
            "credentials_url must be a valid URL (s3://, file://) or an absolute file path"
        ));
    }

    #[tokio::test]
    async fn test_handle_pulsar_credentials_url_with_file_scheme() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let test_url = "file:///tmp/test_credentials";
        let url = Url::parse(test_url).unwrap();

        let result = pulsar_common
            .handle_pulsar_credentials_url(&url, &aws_auth_props)
            .await;

        assert!(result.is_ok());
        let (returned_url, temp_file_opt) = result.unwrap();
        assert_eq!(returned_url, test_url);
        assert!(temp_file_opt.is_none());
    }

    #[tokio::test]
    async fn test_handle_pulsar_credentials_url_with_invalid_scheme() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let test_url = "ftp://example.com/credentials";
        let url = Url::parse(test_url).unwrap();

        let result = pulsar_common
            .handle_pulsar_credentials_url(&url, &aws_auth_props)
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid credentials_url scheme 'ftp'"));
    }

    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_empty_string() {
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: "".to_owned(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains(
            "credentials_url must be a valid URL (s3://, file://) or an absolute file path"
        ));
    }

    #[tokio::test]
    async fn test_resolve_pulsar_credentials_url_with_windows_absolute_path() {
        // This test simulates a Windows absolute path (though running on Linux)
        let pulsar_common = create_test_pulsar_common();
        let aws_auth_props = create_test_aws_auth_props();

        // On Unix systems, this will be treated as relative, so it should fail
        let oauth = PulsarOauthCommon {
            issuer_url: "https://example.com".to_owned(),
            credentials_url: "C:\\credentials\\file.json".to_owned(),
            audience: "test-audience".to_owned(),
            scope: None,
        };

        let result = pulsar_common
            .resolve_pulsar_credentials_url(&oauth, &aws_auth_props)
            .await;

        // On Unix systems, Windows-style path is parsed as URL with scheme 'c', so it should fail
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        // Windows-style path is parsed as URL with scheme 'c' (lowercase)
        assert!(error_msg.contains("invalid credentials_url scheme 'c'"));
    }
}
