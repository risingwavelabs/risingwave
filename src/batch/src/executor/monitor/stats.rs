// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_vec_with_registry, Histogram, Registry,
};
use tokio::sync::mpsc::Sender;

pub struct BatchTaskMetricsManager {
    registry: Registry,
    delete_queue_sender: Sender<DeleteRecord>,
    exchange_recv_row_number: GenericCounterVec<AtomicU64>,
}

impl BatchTaskMetricsManager {
    pub fn new(registry: Registry) -> Self {
        let exchange_recv_row_number = register_int_counter_vec_with_registry!(
            "batch_exchange_recv_row_number",
            "Total number of row that have been received from upstream source",
            &[
                "query_id",
                "source_stage_id",
                "target_stage_id",
                "source_task_id",
                "target_task_id"
            ],
            registry
        )
        .unwrap();

        // Spawn a deletor.
        // TaskMetricsManager will create task metrics for each BatchExecution and task metrics will
        // record all the label it used. When the BatchExecution is finished, the task metrics will
        // send all record to the delete queue. deletor is responsible for reading
        // unused record from delete queue periodically and remove them from metrics.
        // The deletor will remove the record after several minutes. Because it's important not to
        // delete it immediately since promethues pull data periodically.
        let (delete_queue_sender, mut delete_queue_receiver) =
            tokio::sync::mpsc::channel::<DeleteRecord>(1024);
        let exchange_recv_row_number_clone = exchange_recv_row_number.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            let mut delete_cache: Vec<DeleteRecord> = Vec::new();
            let mut connect = true;
            while connect {
                // run every minute.
                let _ = interval.tick().await;

                // delete all record in delete_cache .
                while let Some(delete_record) = delete_cache.pop() {
                    let query_id = delete_record.query_id;
                    for (metric_name, vec) in &delete_record.records {
                        match metric_name.as_str() {
                            "exchange_recv_row_number" => {
                                for [source_stage_id, target_stage_id, source_task_id, target_task_id] in
                                    vec
                                {
                                    if exchange_recv_row_number_clone
                                        .remove_label_values(&[
                                            &query_id,
                                            &source_stage_id.to_string(),
                                            &target_stage_id.to_string(),
                                            &source_task_id.to_string(),
                                            &target_task_id.to_string(),
                                        ])
                                        .is_err()
                                    {
                                        // Already remove, just skip it.
                                    }
                                }
                            }
                            _ => {
                                unimplemented!("Never reach here");
                            }
                        }
                    }
                }

                // read from delete queue and push into delete_cache.
                loop {
                    match delete_queue_receiver.try_recv() {
                        Ok(delete_record) => {
                            delete_cache.push(delete_record);
                        }
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                            break;
                        }
                        Err(_) => {
                            // Error handle need modify later.
                            error!("delete_queue_receiver is closed");
                            connect = false;
                            break;
                        }
                    }
                }
            }
        });

        Self {
            registry,
            delete_queue_sender,
            exchange_recv_row_number,
        }
    }

    pub fn create_task_metrics(&self, query_id: String) -> BatchTaskMetrics {
        BatchTaskMetrics {
            query_id,
            record: RwLock::new(HashMap::new()),
            exchange_recv_row_number: self.exchange_recv_row_number.clone(),
            delete_queue_sender: self.delete_queue_sender.clone(),
        }
    }

    /// Create a new `BatchTaskMetricsManager` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}

pub struct BatchTaskMetrics {
    pub query_id: String,

    /// Used to delete the label haved been recorded.
    /// record definition:
    /// HashMap { Key: metrics_name ,
    ///           Value: the list of labels [ source_stage_id,
    /// target_stage_id,source_task_id,target_task_id]         }
    pub record: RwLock<HashMap<String, Vec<[u32; 4]>>>,
    pub exchange_recv_row_number: GenericCounterVec<AtomicU64>,
    /// Used to send DeleteRecor to delete_queue.
    delete_queue_sender: Sender<DeleteRecord>,
}

#[derive(Debug)]
pub struct DeleteRecord {
    query_id: String,
    records: HashMap<String, Vec<[u32; 4]>>,
}

impl BatchTaskMetrics {
    /// After we create a new label for a metric, we should record it first.
    pub fn add_record(
        &self,
        metric_name: String,
        source_stage_id: u32,
        target_stage_id: u32,
        source_task_id: u32,
        target_task_id: u32,
    ) {
        let mut record = self.record.write().unwrap();
        let vec = record.entry(metric_name).or_insert(Vec::new());
        vec.push([
            source_stage_id,
            target_stage_id,
            source_task_id,
            target_task_id,
        ]);
    }

    /// This function execute after the exucution done.
    /// Send all the record to the delete queue.
    pub async fn clear_record(&self) {
        if self.record.read().unwrap().is_empty() {
            return;
        }

        let delete_record = DeleteRecord {
            query_id: self.query_id.clone(),
            records: self.record.read().unwrap().clone(),
        };
        if self.delete_queue_sender.send(delete_record).await.is_err() {
            // The error handle may need to modify.
            error!("Failed to send delete record to delete queue");
        };
    }
}

pub struct BatchMetrics {
    pub row_seq_scan_next_duration: Histogram,
}

impl BatchMetrics {
    pub fn new(registry: Registry) -> Self {
        let opts = histogram_opts!(
            "batch_row_seq_scan_next_duration",
            "Time spent deserializing into a row in cell based table.",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let row_seq_scan_next_duration = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            row_seq_scan_next_duration,
        }
    }

    /// Create a new `BatchMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
