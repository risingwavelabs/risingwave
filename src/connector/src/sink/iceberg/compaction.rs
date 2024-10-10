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

use std::collections::HashMap;
use std::pin::pin;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_emrserverless::types::builders::SparkSubmitBuilder;
use aws_sdk_emrserverless::types::{JobDriver, JobRunState};
use aws_sdk_emrserverless::Client;
use aws_types::region::Region;
use futures::future::select;
use itertools::Itertools;
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{error, info, warn};

use crate::sink::iceberg::IcebergConfig;

pub struct NimtableCompactionConfig {
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    execution_role_arn: String,
    application_id: String,
    entrypoint: String,
    compact_frequency: usize,
    min_compact_gap_duration_sec: Duration,
}

impl NimtableCompactionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        use std::env::var;
        Ok(NimtableCompactionConfig {
            region: var("NIMTABLE_COMPACTION_REGION").ok(),
            access_key: var("NIMTABLE_COMPACTION_ACCESS_KEY").ok(),
            secret_key: var("NIMTABLE_COMPACTION_SECRET_KEY").ok(),
            execution_role_arn: var("NIMTABLE_COMPACTION_EXECUTION_ROLE_ARN").map_err(|_| {
                anyhow!("NIMTABLE_COMPACTION_EXECUTION_ROLE_ARN not set in env var")
            })?,
            application_id: var("NIMTABLE_COMPACTION_APPLICATION_ID")
                .map_err(|_| anyhow!("NIMTABLE_COMPACTION_APPLICATION_ID not set in env var"))?,
            entrypoint: var("NIMTABLE_COMPACTION_ENTRYPOINT")
                .map_err(|_| anyhow!("NIMTABLE_COMPACTION_ENTRYPOINT not set in env var"))?,
            compact_frequency: var("NIMTABLE_COMPACTION_FREQUENCY")
                .map_err(|_| anyhow!("NIMTABLE_COMPACTION_FREQUENCY not set in env var"))?
                .parse::<usize>()
                .map_err(|e| {
                    anyhow!("invalid NIMTABLE_COMPACTION_FREQUENCY: {:?}", e.as_report())
                })?,
            min_compact_gap_duration_sec: Duration::from_secs(
                var("NIMTABLE_MIN_COMPACTION_GAP_DURATION_SEC")
                    .map_err(|_| {
                        anyhow!("NIMTABLE_MIN_COMPACTION_GAP_DURATION_SEC not set in env var")
                    })?
                    .parse::<u64>()
                    .map_err(|e| {
                        anyhow!(
                            "invalid NIMTABLE_MIN_COMPACTION_GAP_DURATION_SEC: {:?}",
                            e.as_report()
                        )
                    })?,
            ),
        })
    }
}

async fn run_compact(
    database: String,
    table: String,
    catalog_config: HashMap<String, String>,
    mut commit_rx: mpsc::UnboundedReceiver<()>,
) {
    let config = match NimtableCompactionConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            error!(database, table, e = ?e.as_report(), "failed to start compact worker");
            return;
        }
    };
    let compact_frequency = config.compact_frequency;
    let min_compact_gap_duration_sec = config.min_compact_gap_duration_sec;
    let mut pending_commit_num = 0;
    let client = NimtableCompactionClient::new(config).await;
    let new_backoff = || {
        ExponentialBackoff::from_millis(1000)
            .factor(2)
            .max_delay(Duration::from_secs(60))
    };

    let mut prev_compact_success_time = Instant::now();
    let mut error_backoff = new_backoff();
    let mut error_count = 0;

    loop {
        if pending_commit_num >= compact_frequency
            && Instant::now().duration_since(prev_compact_success_time)
                > min_compact_gap_duration_sec
        {
            let compact_start_time = Instant::now();
            if let Err(e) = client
                .compact(database.clone(), table.clone(), &catalog_config)
                .await
            {
                let backoff_duration = error_backoff.next().expect("should exist");
                error_count += 1;
                error!(
                    err = ?e.as_report(),
                    ?backoff_duration,
                    error_count,
                    database, table, "failed to compact"
                );
                sleep(backoff_duration).await;
            } else {
                info!(database, table, elapsed = ?compact_start_time.elapsed(),  "compact success");
                pending_commit_num = 0;
                error_backoff = new_backoff();
                error_count = 0;
                prev_compact_success_time = Instant::now();
            }
        }
        if commit_rx.recv().await.is_some() {
            pending_commit_num += 1;
        } else {
            break;
        }
    }
}

pub fn spawn_compaction_client(
    config: &IcebergConfig,
) -> anyhow::Result<(mpsc::UnboundedSender<()>, oneshot::Sender<()>)> {
    let warehouse_path = config.common.warehouse_path.clone();
    let database = config
        .common
        .database_name
        .clone()
        .ok_or_else(|| anyhow!("should specify database"))?;
    let table = config.common.table_name.clone();
    let (commit_tx, commit_rx) = mpsc::unbounded_channel();
    let (finish_tx, finish_rx) = oneshot::channel();

    let _join_handle = tokio::spawn(async move {
        let catalog_config = HashMap::from_iter([
            ("type".to_string(), "hadoop".to_string()),
            ("warehouse".to_string(), warehouse_path),
        ]);
        select(
            finish_rx,
            pin!(run_compact(
                database.clone(),
                table.clone(),
                catalog_config,
                commit_rx,
            )),
        )
        .await;
        warn!(database, table, "compact worker exits");
    });
    Ok((commit_tx, finish_tx))
}

pub struct NimtableCompactionClient {
    client: Client,
    config: NimtableCompactionConfig,
}

impl NimtableCompactionClient {
    pub async fn new(config: NimtableCompactionConfig) -> Self {
        let config_loader = aws_config::from_env();
        let config_loader = if let Some(region) = &config.region {
            config_loader.region(Region::new(region.clone()))
        } else {
            config_loader
        };
        let config_loader = if let (Some(access_key), Some(secret_key)) =
            (&config.access_key, &config.secret_key)
        {
            config_loader.credentials_provider(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    access_key.clone(),
                    secret_key.clone(),
                    None,
                ),
            ))
        } else {
            config_loader
        };
        let sdk_config = config_loader.load().await;
        Self {
            client: Client::new(&sdk_config),
            config,
        }
    }

    async fn wait_job_finish(&self, job_run_id: String) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let success_job_run = loop {
            let output = self
                .client
                .get_job_run()
                .job_run_id(&job_run_id)
                .application_id(self.config.application_id.clone())
                .send()
                .await?;
            let job_run = output.job_run.ok_or_else(|| anyhow!("empty job run"))?;
            match &job_run.state {
                JobRunState::Cancelled | JobRunState::Cancelling | JobRunState::Failed => {
                    return Err(anyhow!(
                        "fail state: {}. Detailed: {}",
                        job_run.state,
                        job_run.state_details
                    ));
                }
                JobRunState::Pending
                | JobRunState::Queued
                | JobRunState::Running
                | JobRunState::Scheduled
                | JobRunState::Submitted => {
                    info!(
                        elapsed = ?start_time.elapsed(),
                        job_status = ?job_run.state,
                        "waiting job."
                    );
                    sleep(Duration::from_secs(5)).await;
                }
                JobRunState::Success => {
                    break job_run;
                }
                state => {
                    return Err(anyhow!("unhandled state: {:?}", state));
                }
            };
        };
        info!(
            job_run_id,
            details = success_job_run.state_details,
            elapsed = ?start_time.elapsed(),
            "job run finish"
        );
        Ok(())
    }

    pub async fn compact(
        &self,
        db: String,
        table: String,
        catalog_config: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let start_result = self
            .client
            .start_job_run()
            .name(format!(
                "job-run-{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ))
            .application_id(self.config.application_id.clone())
            .execution_role_arn(self.config.execution_role_arn.clone())
            .job_driver(JobDriver::SparkSubmit(
                SparkSubmitBuilder::default()
                    .entry_point(self.config.entrypoint.clone())
                    .entry_point_arguments(db)
                    .entry_point_arguments(table)
                    .spark_submit_parameters(
                        catalog_config
                            .iter()
                            .map(|(key, value)| {
                                format!("--conf spark.sql.catalog.nimtable.{}={}", key, value)
                            })
                            .join(" "),
                    )
                    .build()
                    .unwrap(),
            ))
            .send()
            .await
            .context("start job")?;
        info!(job_run_id = start_result.job_run_id, "job started");
        self.wait_job_finish(start_result.job_run_id).await
    }
}

#[tokio::test]
#[ignore]
async fn trigger_compaction() {
    tracing_subscriber::fmt().init();
    let warehouse = "s3://nimtable-spark/iceberg/";
    let db = "db";
    let table = "table";

    let config = NimtableCompactionConfig::from_env().unwrap();
    let client = NimtableCompactionClient::new(config).await;
    let result = client
        .compact(
            db.to_owned(),
            table.to_owned(),
            &HashMap::from_iter([
                ("type".to_string(), "hadoop".to_string()),
                ("warehouse".to_string(), warehouse.to_string()),
            ]),
        )
        .await;

    info!(?result, "job result");
}
