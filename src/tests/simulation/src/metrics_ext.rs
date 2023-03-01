use prometheus_parse::Scrape;

use crate::cluster::Cluster;

impl Cluster {
    fn get_compute_node_metrics(&self, prometheus_listen_addr: &str) -> Scrape {
        let body = reqwest::get(format!("https://{}/metrics", prometheus_listen_addr))
            .unwrap()
            .text()
            .unwrap();
        let lines: Vec<_> = body.lines().map(|s| Ok(s.to_owned)).collect();

        prometheus_parse::Scrape::parse(lines.into_iter()).unwrap()
    }

    fn check_no_batch_task(&self, compute_node_prometheus_listen_addr: &str) {
        let metrics = self.get_compute_node_metrics(compute_node_prometheus_listen_addr);
        assert_eq!(
            prometheus_parse::Value::Gauge(0f64),
            metrics
                .samples
                .first(|s| s.metric == "batch_task_num")
                .unwrap()
        );
    }

    pub fn check_no_batch_task_in_all_compute_nodes(&self, compute_node_count: usize) {
        for i in 1..=compute_node_count {
            self.check_no_batch_task(format!("192.168.3.{i}:1222", i))
        }
    }
}
