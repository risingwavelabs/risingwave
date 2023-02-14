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

use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::anyhow;
use axum::{Extension, Json};
use prometheus_http_query::response::{RangeVector, Sample};
use serde::Serialize;

use super::handlers::{err, DashboardError};
use super::Service;
use crate::storage::MetaStore;

#[derive(Serialize, Debug)]
pub struct PrometheusSample {
    pub timestamp: f64,
    pub value: f64,
}

impl From<&Sample> for PrometheusSample {
    fn from(value: &Sample) -> Self {
        PrometheusSample {
            timestamp: value.timestamp(),
            value: value.value(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct PrometheusVector {
    metric: HashMap<String, String>,
    sample: Vec<PrometheusSample>,
}

impl From<&RangeVector> for PrometheusVector {
    fn from(value: &RangeVector) -> Self {
        PrometheusVector {
            metric: value.metric().clone(),
            sample: value.samples().iter().map(PrometheusSample::from).collect(),
        }
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ClusterMetrics {
    cpu_data: Vec<PrometheusVector>,
    memory_data: Vec<PrometheusVector>,
}

pub type Result<T> = std::result::Result<T, DashboardError>;

pub async fn list_prometheus_cluster<S: MetaStore>(
    Extension(srv): Extension<Service<S>>,
) -> Result<Json<ClusterMetrics>> {
    if let Some(ref client) = srv.prometheus_client {
        // assume job_name is one of compute, meta, frontend
        let now = SystemTime::now();
        let cpu_query = "sum(rate(process_cpu_seconds_total{job=~\"compute|meta|frontend\"}[60s])) by (job,instance)";
        let result = client
            .query_range(
                cpu_query,
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    - 3600,
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                15.0,
            )
            .get()
            .await
            .map_err(err)?;
        let cpu_data = result
            .data()
            .as_matrix()
            .unwrap()
            .iter()
            .map(PrometheusVector::from)
            .collect();
        let memory_query =
            "avg(process_resident_memory_bytes{job=~\"compute|meta|frontend\"}) by (job,instance)";
        let result = client
            .query_range(
                memory_query,
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64
                    - 3600,
                now.duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as i64,
                60.0,
            )
            .get()
            .await
            .map_err(err)?;
        let memory_data = result
            .data()
            .as_matrix()
            .unwrap()
            .iter()
            .map(PrometheusVector::from)
            .collect();
        Ok(Json(ClusterMetrics {
            cpu_data,
            memory_data,
        }))
    } else {
        Err(err(anyhow!("Prometheus endpoint is not set")))
    }
}
