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

use risingwave_connector::source::AnySplitEnumerator;

use super::*;

const MAX_FAIL_CNT: u32 = 10;
const DEFAULT_SOURCE_TICK_TIMEOUT: Duration = Duration::from_secs(10);

pub struct SharedSplitMap {
    pub splits: Option<BTreeMap<SplitId, SplitImpl>>,
}

type SharedSplitMapRef = Arc<Mutex<SharedSplitMap>>;

/// `ConnectorSourceWorker` keeps fetching the latest split metadata from the external source service ([`Self::tick`]),
/// and maintains it in `current_splits`.
pub struct ConnectorSourceWorker {
    source_id: SourceId,
    source_name: String,
    current_splits: SharedSplitMapRef,
    // XXX: box or arc?
    enumerator: Box<dyn AnySplitEnumerator>,
    period: Duration,
    metrics: Arc<MetaMetrics>,
    connector_properties: ConnectorProperties,
    fail_cnt: u32,
    source_is_up: LabelGuardedIntGauge<2>,
}

fn extract_prop_from_existing_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let options_with_secret =
        WithOptionsSecResolved::new(source.with_properties.clone(), source.secret_refs.clone());
    let mut properties = ConnectorProperties::extract(options_with_secret, false)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}
fn extract_prop_from_new_source(source: &Source) -> ConnectorResult<ConnectorProperties> {
    let options_with_secret =
        WithOptionsSecResolved::new(source.with_properties.clone(), source.secret_refs.clone());
    let mut properties = ConnectorProperties::extract(options_with_secret, true)?;
    properties.init_from_pb_source(source);
    Ok(properties)
}

/// Used to create a new [`ConnectorSourceWorkerHandle`] for a new source.
///
/// It will call [`ConnectorSourceWorker::tick()`] to fetch split metadata once before returning.
pub async fn create_source_worker(
    source: &Source,
    metrics: Arc<MetaMetrics>,
) -> MetaResult<ConnectorSourceWorkerHandle> {
    tracing::info!("spawning new watcher for source {}", source.id);

    let splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
    let current_splits_ref = splits.clone();

    let connector_properties = extract_prop_from_new_source(source)?;
    let enable_scale_in = connector_properties.enable_drop_split();
    let enable_adaptive_splits = connector_properties.enable_adaptive_splits();
    let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = {
        let mut worker = ConnectorSourceWorker::create(
            source,
            connector_properties,
            DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
            current_splits_ref.clone(),
            metrics,
        )
        .await?;

        // if fail to fetch meta info, will refuse to create source

        // todo: make the timeout configurable, longer than `properties.sync.call.timeout`
        // in kafka
        tokio::time::timeout(DEFAULT_SOURCE_TICK_TIMEOUT, worker.tick())
            .await
            .ok()
            .with_context(|| {
                format!(
                    "failed to fetch meta info for source {}, timeout {:?}",
                    source.id, DEFAULT_SOURCE_TICK_TIMEOUT
                )
            })??;

        tokio::spawn(async move { worker.run(sync_call_rx).await })
    };
    Ok(ConnectorSourceWorkerHandle {
        handle,
        sync_call_tx,
        splits,
        enable_drop_split: enable_scale_in,
        enable_adaptive_splits,
    })
}

/// Used on startup ([`SourceManager::new`]). Failed sources will not block meta startup.
pub fn create_source_worker_async(
    source: Source,
    managed_sources: &mut HashMap<SourceId, ConnectorSourceWorkerHandle>,
    metrics: Arc<MetaMetrics>,
) -> MetaResult<()> {
    tracing::info!("spawning new watcher for source {}", source.id);

    let splits = Arc::new(Mutex::new(SharedSplitMap { splits: None }));
    let current_splits_ref = splits.clone();
    let source_id = source.id;

    let connector_properties = extract_prop_from_existing_source(&source)?;

    let enable_drop_split = connector_properties.enable_drop_split();
    let enable_adaptive_splits = connector_properties.enable_adaptive_splits();
    let (sync_call_tx, sync_call_rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        let mut ticker = time::interval(DEFAULT_SOURCE_WORKER_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut worker = loop {
            ticker.tick().await;

            match ConnectorSourceWorker::create(
                &source,
                connector_properties.clone(),
                DEFAULT_SOURCE_WORKER_TICK_INTERVAL,
                current_splits_ref.clone(),
                metrics.clone(),
            )
            .await
            {
                Ok(worker) => {
                    break worker;
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "failed to create source worker");
                }
            }
        };

        worker.run(sync_call_rx).await
    });

    managed_sources.insert(
        source_id as SourceId,
        ConnectorSourceWorkerHandle {
            handle,
            sync_call_tx,
            splits,
            enable_drop_split,
            enable_adaptive_splits,
        },
    );
    Ok(())
}

const DEFAULT_SOURCE_WORKER_TICK_INTERVAL: Duration = Duration::from_secs(30);

impl ConnectorSourceWorker {
    /// Recreate the `SplitEnumerator` to establish a new connection to the external source service.
    async fn refresh(&mut self) -> MetaResult<()> {
        let enumerator = self
            .connector_properties
            .clone()
            .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                metrics: self.metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: self.source_id as u32,
                },
            }))
            .await
            .context("failed to create SplitEnumerator")?;
        self.enumerator = enumerator;
        self.fail_cnt = 0;
        tracing::info!("refreshed source enumerator: {}", self.source_name);
        Ok(())
    }

    /// On creation, connection to the external source service will be established, but `splits`
    /// will not be updated until `tick` is called.
    pub async fn create(
        source: &Source,
        connector_properties: ConnectorProperties,
        period: Duration,
        splits: Arc<Mutex<SharedSplitMap>>,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let enumerator = connector_properties
            .clone()
            .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                metrics: metrics.source_enumerator_metrics.clone(),
                info: SourceEnumeratorInfo {
                    source_id: source.id,
                },
            }))
            .await
            .context("failed to create SplitEnumerator")?;

        let source_is_up = metrics
            .source_is_up
            .with_guarded_label_values(&[source.id.to_string().as_str(), &source.name]);

        Ok(Self {
            source_id: source.id as SourceId,
            source_name: source.name.clone(),
            current_splits: splits,
            enumerator,
            period,
            metrics,
            connector_properties,
            fail_cnt: 0,
            source_is_up,
        })
    }

    pub async fn run(
        &mut self,
        mut sync_call_rx: UnboundedReceiver<oneshot::Sender<MetaResult<()>>>,
    ) {
        let mut interval = time::interval(self.period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            select! {
                biased;
                tx = sync_call_rx.borrow_mut().recv() => {
                    if let Some(tx) = tx {
                        let _ = tx.send(self.tick().await);
                    }
                }
                _ = interval.tick() => {
                    if self.fail_cnt > MAX_FAIL_CNT {
                        if let Err(e) = self.refresh().await {
                            tracing::error!(error = %e.as_report(), "error happened when refresh from connector source worker");
                        }
                    }
                    if let Err(e) = self.tick().await {
                        tracing::error!(error = %e.as_report(), "error happened when tick from connector source worker");
                    }
                }
            }
        }
    }

    /// Uses [`risingwave_connector::source::SplitEnumerator`] to fetch the latest split metadata from the external source service.
    async fn tick(&mut self) -> MetaResult<()> {
        let source_is_up = |res: i64| {
            self.source_is_up.set(res);
        };
        let splits = self.enumerator.list_splits().await.inspect_err(|_| {
            source_is_up(0);
            self.fail_cnt += 1;
        })?;
        source_is_up(1);
        self.fail_cnt = 0;
        let mut current_splits = self.current_splits.lock().await;
        current_splits.splits.replace(
            splits
                .into_iter()
                .map(|split| (split.id(), split))
                .collect(),
        );

        Ok(())
    }
}

/// Handle for a running [`ConnectorSourceWorker`].
pub struct ConnectorSourceWorkerHandle {
    pub handle: JoinHandle<()>,
    pub sync_call_tx: UnboundedSender<oneshot::Sender<MetaResult<()>>>,
    pub splits: SharedSplitMapRef,
    pub enable_drop_split: bool,
    pub enable_adaptive_splits: bool,
}

impl ConnectorSourceWorkerHandle {
    pub fn get_enable_adaptive_splits(&self) -> bool {
        self.enable_adaptive_splits
    }

    pub async fn discovered_splits(
        &self,
        source_id: SourceId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<BTreeMap<Arc<str>, SplitImpl>> {
        let Some(mut discovered_splits) = self.splits.lock().await.splits.clone() else {
            tracing::info!(
                "The discover loop for source {} is not ready yet; we'll wait for the next run",
                source_id
            );
            return Ok(BTreeMap::new());
        };
        if discovered_splits.is_empty() {
            tracing::warn!("No splits discovered for source {}", source_id);
        }

        if self.enable_adaptive_splits {
            // Connector supporting adaptive splits returns just one split, and we need to make the number of splits equal to the number of actors in this fragment.
            // Because we Risingwave consume the splits statelessly and we do not need to keep the id internally, we always use actor_id as split_id.
            // And prev splits record should be dropped via CN.

            debug_assert!(self.enable_drop_split);
            debug_assert!(discovered_splits.len() == 1);
            discovered_splits =
                fill_adaptive_split(discovered_splits.values().next().unwrap(), actors)?;
        }

        Ok(discovered_splits)
    }
}
