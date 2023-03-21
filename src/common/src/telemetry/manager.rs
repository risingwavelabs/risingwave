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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::select;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;

use super::report::{start_telemetry_reporting, TelemetryInfoFetcher, TelemetryReportCreator};
use crate::system_param::local_manager::SystemParamsReaderRef;
use crate::telemetry::telemetry_env_enabled;

pub struct TelemetryManager<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    core: Arc<RwLock<TelemetryManagerCore<F, I>>>,
    sys_params_change_rx: Receiver<SystemParamsReaderRef>,
}

impl<F, I> TelemetryManager<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    pub fn new(
        sys_params_change_rx: Receiver<SystemParamsReaderRef>,
        info_fetcher: Arc<I>,
        report_creator: Arc<F>,
    ) -> Self {
        Self {
            core: Arc::new(RwLock::new(TelemetryManagerCore::new(
                info_fetcher,
                report_creator,
            ))),
            sys_params_change_rx,
        }
    }

    pub fn start_telemetry_reporting(&self) {
        self.core.write().start();
    }

    pub fn watch_params_change(mut self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            loop {
                select! {
                    Ok(_) = self.sys_params_change_rx.changed() => {
                                let telemetry_enabled = {
                                    let params = self.sys_params_change_rx.borrow().load();
                                    // check both environment variable and system params
                                    // if either is false, then stop telemetry
                                    params.telemetry_enabled() && telemetry_env_enabled()
                                };

                                let telemetry_running = {
                                    let core = self.core.read();
                                    core.telemetry_running()
                                };

                                match (telemetry_running, telemetry_enabled) {
                                    (false, true) => {
                                        tracing::info!("telemetry config changed to true, start reporting");
                                        self.core.write().start();
                                    }
                                    (true, false) => {
                                        tracing::info!("telemetry config changed to false, stop reporting");
                                        self.core.write().stop();
                                    }
                                    _ => {}
                                };
                        }
                    ,
                    _ = &mut shutdown_rx =>{
                        tracing::info!("Telemetry exit");
                        return;
                    }
                }
            }
        });
        (handle, shutdown_tx)
    }
}

struct TelemetryManagerCore<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    telemetry_handle: Option<JoinHandle<()>>,
    telemetry_shutdown_tx: Option<Sender<()>>,
    telemetry_running: Arc<AtomicBool>,
    info_fetcher: Arc<I>,
    report_creator: Arc<F>,
}

impl<F, I> TelemetryManagerCore<F, I>
where
    F: TelemetryReportCreator + Send + Sync + 'static,
    I: TelemetryInfoFetcher + Send + Sync + 'static,
{
    fn new(info_fetcher: Arc<I>, report_creator: Arc<F>) -> Self {
        Self {
            telemetry_handle: None,
            telemetry_shutdown_tx: None,
            telemetry_running: Arc::new(AtomicBool::new(false)),
            info_fetcher,
            report_creator,
        }
    }

    fn telemetry_running(&self) -> bool {
        self.telemetry_running.load(Ordering::Relaxed)
    }

    fn start(&mut self) {
        if self.telemetry_running() {
            return;
        }

        let (handle, tx) =
            start_telemetry_reporting(self.info_fetcher.clone(), self.report_creator.clone());
        self.telemetry_handle = Some(handle);
        self.telemetry_shutdown_tx = Some(tx);
        self.telemetry_running.store(true, Ordering::Relaxed);
    }

    fn stop(&mut self) {
        match (
            self.telemetry_running.load(Ordering::Relaxed),
            self.telemetry_shutdown_tx.take(),
            self.telemetry_handle.take(),
        ) {
            (true, Some(shutdown_rx), Some(_)) => {
                if let Err(()) = shutdown_rx.send(()) {
                    tracing::error!("telemetry mgr failed to send stop signal");
                } else {
                    self.telemetry_running.store(false, Ordering::Relaxed)
                }
            }
            // do nothing if telemetry is not running
            (false, None, None) => {}
            _ => unreachable!("impossible telemetry handler"),
        }
    }
}
