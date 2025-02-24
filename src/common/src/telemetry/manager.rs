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

use std::sync::Arc;

use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use super::report::{TelemetryInfoFetcher, TelemetryReportCreator, start_telemetry_reporting};

pub struct TelemetryManager<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    info_fetcher: Arc<I>,
    report_creator: Arc<F>,
}

impl<F, I> TelemetryManager<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    pub fn new(info_fetcher: Arc<I>, report_creator: Arc<F>) -> Self {
        Self {
            info_fetcher,
            report_creator,
        }
    }

    pub async fn start(&self) -> (JoinHandle<()>, Sender<()>) {
        start_telemetry_reporting(self.info_fetcher.clone(), self.report_creator.clone()).await
    }
}
