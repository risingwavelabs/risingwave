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

use std::fmt;

use anyhow::anyhow;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use serde_derive::Deserialize;
use thiserror_ext::AsReport;
use tokio_postgres::{Client as PgClient, NoTls};

use super::maybe_tls_connector::MaybeMakeTlsConnector;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    #[serde(alias = "disable")]
    Disabled,
    #[serde(alias = "prefer")]
    Preferred,
    #[serde(alias = "require")]
    Required,
    /// verify that the server is trustworthy by checking the certificate chain
    /// up to the root certificate stored on the client.
    #[serde(alias = "verify-ca")]
    VerifyCa,
    /// Besides verify the certificate, will also verify that the serverhost name
    /// matches the name stored in the server certificate.
    #[serde(alias = "verify-full")]
    VerifyFull,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::Preferred
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SslMode::Disabled => "disabled",
            SslMode::Preferred => "preferred",
            SslMode::Required => "required",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        })
    }
}

pub async fn create_pg_client(
    user: &str,
    password: &str,
    host: &str,
    port: &str,
    database: &str,
    ssl_mode: &SslMode,
    ssl_root_cert: &Option<String>,
) -> anyhow::Result<PgClient> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .user(user)
        .password(password)
        .host(host)
        .port(port.parse::<u16>().unwrap())
        .dbname(database);

    let (_verify_ca, verify_hostname) = match ssl_mode {
        SslMode::VerifyCa => (true, false),
        SslMode::VerifyFull => (true, true),
        _ => (false, false),
    };

    #[cfg(not(madsim))]
    let connector = match ssl_mode {
        SslMode::Disabled => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            MaybeMakeTlsConnector::NoTls(NoTls)
        }
        SslMode::Preferred => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
            match SslConnector::builder(SslMethod::tls()) {
                Ok(mut builder) => {
                    // disable certificate verification for `prefer`
                    builder.set_verify(SslVerifyMode::NONE);
                    MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "SSL connector error");
                    MaybeMakeTlsConnector::NoTls(NoTls)
                }
            }
        }
        SslMode::Required => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            // disable certificate verification for `require`
            builder.set_verify(SslVerifyMode::NONE);
            MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
        }

        SslMode::VerifyCa | SslMode::VerifyFull => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            if let Some(ssl_root_cert) = ssl_root_cert {
                builder.set_ca_file(ssl_root_cert).map_err(|e| {
                    anyhow!(format!("bad ssl root cert error: {}", e.to_report_string()))
                })?;
            }
            let mut connector = MakeTlsConnector::new(builder.build());
            if !verify_hostname {
                connector.set_callback(|config, _| {
                    config.set_verify_hostname(false);
                    Ok(())
                });
            }
            MaybeMakeTlsConnector::Tls(connector)
        }
    };
    #[cfg(madsim)]
    let connector = NoTls;

    let (client, connection) = pg_config.connect(connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e.as_report(), "postgres connection error");
        }
    });

    Ok(client)
}
