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

use prometheus_parse::Scrape;

use crate::cluster::Cluster;

impl Cluster {
    async fn get_compute_node_metrics(&self, prometheus_listen_addr: &str) -> Scrape {
        let body = reqwest::get(format!("https://{prometheus_listen_addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let lines: Vec<_> = body.lines().map(|s| Ok(s.to_owned())).collect();

        Scrape::parse(lines.into_iter()).unwrap()
    }

    async fn check_no_batch_task(&self, compute_node_prometheus_listen_addr: &str) {
        let metrics = self
            .get_compute_node_metrics(compute_node_prometheus_listen_addr)
            .await;
        assert_eq!(
            prometheus_parse::Value::Gauge(0f64),
            metrics
                .samples
                .iter()
                .find(|s| s.metric == "batch_task_num")
                .unwrap()
                .value
        );
    }

    pub async fn check_no_batch_task_in_all_compute_nodes(&self, compute_node_count: usize) {
        for i in 1..=compute_node_count {
            self.check_no_batch_task(format!("192.168.3.{i}:1222").as_str())
                .await;
        }
    }
}
