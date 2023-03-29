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

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc, Describer};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily, MetricType};
use prometheus::{proto, Opts, Registry};
use risingwave_common::catalog::TableId;

use crate::task::{ActorId, FragmentId};

/// Monitors actor info.
pub fn monitor_actor_info(registry: &Registry, collector: Arc<ActorInfoCollector>) -> Result<()> {
    let ac = ActorInfoCollectorWrapper { inner: collector };
    registry
        .register(Box::new(ac))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}

const ACTOR_ID_LABEL: &str = "actor_id";
const FRAGMENT_ID_LABEL: &str = "fragment_id";
const TABLE_ID_LABEL: &str = "table_id";
const TABLE_NAME_LABEL: &str = "table_name";
const DUMMY_GAUGE_VALUE: f64 = 1.0;

/// A collector to collect two types of actor info
/// - Mapping from actor id to fragment id
/// - Mapping from table id to actor id and table name
///
/// The collector emits one dummy gauge value labelled by (actor id, fragment id)
/// and one dummay gauge value labelled by (table id, actor id, table name). Instead
/// of emitting metrics continuously as a time-series, the collector will only emit
/// these metrics on actor creation and state table creation to save the storage cost.
///
/// These labels will then be used by the grafana dashboard to plot a table to display
/// the mapping info.
pub struct ActorInfoCollector {
    desc: Vec<Desc>,
    actor_info_to_collect: Mutex<Vec<MetricFamily>>,
}

impl Default for ActorInfoCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl ActorInfoCollector {
    pub fn new() -> Self {
        Self {
            desc: vec![
                Opts::new("actor_id_info", "Mapping from actor id to fragment id")
                    .variable_labels(vec![
                        ACTOR_ID_LABEL.to_owned(),
                        FRAGMENT_ID_LABEL.to_owned(),
                    ])
                    .describe()
                    .unwrap(),
                Opts::new(
                    "state_table_id_info",
                    "Mapping from table id to table name and actor id",
                )
                .variable_labels(vec![
                    TABLE_ID_LABEL.to_owned(),
                    ACTOR_ID_LABEL.to_owned(),
                    TABLE_NAME_LABEL.to_owned(),
                ])
                .describe()
                .unwrap(),
            ],
            actor_info_to_collect: Mutex::new(vec![]),
        }
    }

    pub fn add_actor(&self, actor_id: ActorId, fragment_id: FragmentId) {
        // Fake a metric family with the corresponding desc
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc[0].fq_name.clone());
        mf.set_help(self.desc[0].help.clone());
        mf.set_field_type(MetricType::GAUGE);

        // Fake a gauge metric with the corresponding label
        let mut m = Metric::default();
        let mut gauge = Gauge::default();
        gauge.set_value(DUMMY_GAUGE_VALUE);
        m.set_gauge(gauge);
        let mut labels = Vec::with_capacity(2);
        let mut label = LabelPair::new();
        label.set_name(ACTOR_ID_LABEL.to_owned());
        label.set_value(actor_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(FRAGMENT_ID_LABEL.to_owned());
        label.set_value(fragment_id.to_string());
        labels.push(label);
        m.set_label(labels.into());
        mf.set_metric(vec![m].into());

        // Store the fake metric family for later collection
        self.actor_info_to_collect.lock().push(mf);
    }

    pub fn add_table(&self, table_id: TableId, actor_id: ActorId, table_name: &str) {
        // Fake a metric family with the corresponding desc
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc[1].fq_name.clone());
        mf.set_help(self.desc[1].help.clone());
        mf.set_field_type(MetricType::GAUGE);

        // Fake a gauge metric with the corresponding label
        let mut m = Metric::default();
        let mut gauge = Gauge::default();
        gauge.set_value(DUMMY_GAUGE_VALUE);
        m.set_gauge(gauge);
        let mut labels = Vec::with_capacity(3);
        let mut label = LabelPair::new();
        label.set_name(TABLE_ID_LABEL.to_owned());
        label.set_value(table_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(ACTOR_ID_LABEL.to_owned());
        label.set_value(actor_id.to_string());
        labels.push(label);
        let mut label = LabelPair::new();
        label.set_name(TABLE_NAME_LABEL.to_owned());
        label.set_value(table_name.to_string());
        labels.push(label);
        m.set_label(labels.into());
        mf.set_metric(vec![m].into());

        // Store the fake metric family for later collection
        self.actor_info_to_collect.lock().push(mf);
    }

    fn desc(&self) -> Vec<&Desc> {
        self.desc.iter().collect_vec()
    }

    // The metrics will be cleared after collection to avoid emitting metrics for the same
    // actor/table continuously
    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.actor_info_to_collect.lock().split_off(0)
    }
}

pub struct ActorInfoCollectorWrapper {
    inner: Arc<ActorInfoCollector>,
}

impl Collector for ActorInfoCollectorWrapper {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.inner.collect()
    }
}
