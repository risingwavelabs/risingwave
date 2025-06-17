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
