// Copyright 2026 RisingWave Labs
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

use anyhow::{Context, Result, anyhow};
use risingwave_meta_model::SourceId;

use crate::CtlContext;

/// Alter source connector properties with pause/resume orchestration.
///
/// This is a safe way to update source properties that:
/// 1. Optionally flushes for checkpoint
/// 2. Pauses the source
/// 3. Updates catalog and propagates changes
/// 4. Optionally resets split assignments
/// 5. Resumes the source
pub async fn alter_source_properties_safe(
    context: &CtlContext,
    source_id: u32,
    props_json: String,
    flush: bool,
    reset_splits: bool,
) -> Result<()> {
    let meta_client = context.meta_client().await?;

    let props: HashMap<String, String> =
        serde_json::from_str(&props_json).context("Failed to parse props as JSON object")?;

    if props.is_empty() {
        return Err(anyhow!("No properties provided to update"));
    }

    println!("=== ALTER SOURCE PROPERTIES (SAFE) ===");
    println!("Source ID: {}", source_id);
    println!("Properties to update: {:?}", props);
    println!("Flush before pause: {}", flush);
    println!("Reset splits: {}", reset_splits);
    println!();

    println!("WARNING: This operation will pause the source during the update.");
    if reset_splits {
        println!("WARNING: Split reset requested - this may cause data duplication or loss!");
    }
    println!();

    meta_client
        .alter_source_properties_safe(
            SourceId::from(source_id),
            props.into_iter().collect(),
            Default::default(), // No secret refs for now
            flush,
            reset_splits,
        )
        .await?;

    println!("Source properties updated successfully!");
    Ok(())
}

/// Reset source split assignments.
///
/// WARNING: This is an UNSAFE operation that may cause data duplication or loss!
/// It clears cached split state and triggers re-discovery from upstream.
pub async fn reset_source_splits(context: &CtlContext, source_id: u32) -> Result<()> {
    let meta_client = context.meta_client().await?;

    println!("=== RESET SOURCE SPLITS (UNSAFE) ===");
    println!("Source ID: {}", source_id);
    println!();
    println!("WARNING: This is an UNSAFE operation!");
    println!("It will clear all cached split state and trigger re-discovery from upstream.");
    println!("This may cause data duplication or loss depending on the connector!");
    println!();

    meta_client
        .reset_source_splits(SourceId::from(source_id))
        .await?;

    println!("Source splits reset successfully!");
    println!("New splits will be assigned on the next tick.");
    Ok(())
}

/// Inject specific offsets into source splits.
///
/// WARNING: This is an UNSAFE operation that may cause data duplication or loss!
/// Use with extreme caution and only when you know the exact offsets needed.
pub async fn inject_source_offsets(
    context: &CtlContext,
    source_id: u32,
    offsets_json: String,
) -> Result<()> {
    let meta_client = context.meta_client().await?;

    let offsets: HashMap<String, String> =
        serde_json::from_str(&offsets_json).context("Failed to parse offsets as JSON object")?;

    if offsets.is_empty() {
        return Err(anyhow!("No offsets provided to inject"));
    }

    println!("=== INJECT SOURCE OFFSETS (UNSAFE) ===");
    println!("Source ID: {}", source_id);
    println!("Offsets to inject:");
    for (split_id, offset) in &offsets {
        println!("  {} -> {}", split_id, offset);
    }
    println!();
    println!("WARNING: This is an UNSAFE operation!");
    println!("Injecting incorrect offsets WILL cause data duplication or loss!");
    println!("Make sure you know the exact offsets needed before proceeding.");
    println!();

    meta_client
        .inject_source_offsets(SourceId::from(source_id), offsets)
        .await?;

    println!("Source offsets injected successfully!");
    Ok(())
}
