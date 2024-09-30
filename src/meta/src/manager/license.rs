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

use anyhow::Context;
use notify::Watcher;
use risingwave_common::system_param::LICENSE_KEY_KEY;
use thiserror_ext::AsReport;
use tokio::sync::watch;

use super::{MetaSrvEnv, SystemParamsManagerImpl};
use crate::MetaResult;

impl MetaSrvEnv {
    /// Spawn background tasks to watch the license key file and update the system parameter,
    /// if configured.
    pub fn may_start_watch_license_key_file(&self) -> MetaResult<()> {
        let Some(path) = self.opts.license_key_path.as_ref() else {
            return Ok(());
        };

        let (changed_tx, mut changed_rx) = watch::channel(());

        let mut watcher =
            notify::recommended_watcher(move |event: Result<notify::Event, notify::Error>| {
                if let Err(e) = event {
                    tracing::warn!(
                        error = %e.as_report(),
                        "error occurred while watching license key file"
                    );
                    return;
                }
                // We don't check the event type but always notify the updater for simplicity.
                let _ = changed_tx.send(());
            })
            .context("failed to create license key file watcher")?;

        // This will spawn a new thread to watch the file, so no need to be concerned about blocking.
        watcher
            .watch(path, notify::RecursiveMode::NonRecursive)
            .context("failed to watch license key file")?;

        let updater = {
            let mgr = self.system_params_manager_impl_ref();
            let path = path.to_path_buf();
            async move {
                // Let the watcher live until the end of the updater to prevent dropping (then stopping).
                let _watcher = watcher;

                while changed_rx.changed().await.is_ok() {
                    tracing::info!(path = %path.display(), "license key file changed, reloading...");

                    let content = match tokio::fs::read_to_string(&path).await {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %e.as_report(),
                                "failed to read license key file"
                            );
                            continue;
                        }
                    };
                    let content = content.trim().to_owned();

                    // An empty license key file is considered as setting it to the default value,
                    // i.e., `None` when calling `set_param`.
                    let value = (!content.is_empty()).then_some(content);

                    let result = match &mgr {
                        SystemParamsManagerImpl::Kv(mgr) => {
                            mgr.set_param(LICENSE_KEY_KEY, value).await
                        }
                        SystemParamsManagerImpl::Sql(mgr) => {
                            mgr.set_param(LICENSE_KEY_KEY, value).await
                        }
                    };

                    if let Err(e) = result {
                        tracing::error!(
                            error = %e.as_report(),
                            "failed to set license key from file"
                        );
                    }
                }
            }
        };
        let _handle = tokio::spawn(updater);

        Ok(())
    }
}
