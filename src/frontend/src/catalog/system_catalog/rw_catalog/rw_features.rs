use std::collections::HashSet;

use risingwave_common::license::{Feature, LicenseManager};
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// Check all defined premium features and their availability.
#[derive(Fields)]
struct RwFeature {
    #[primary_key]
    name: &'static str,
    description: &'static str,
    available: bool,
}

#[system_catalog(table, "rw_catalog.rw_features")]
fn read_rw_features(_reader: &SysCatalogReaderImpl) -> Result<Vec<RwFeature>> {
    let available_features: HashSet<_> = (LicenseManager::get().license())
        .map(|l| l.tier.available_features().collect())
        .unwrap_or_default();

    Ok(Feature::all()
        .iter()
        .filter(|f| **f != Feature::TestDummy) // hide test feature
        .map(|f| RwFeature {
            name: f.name(),
            description: f.description(),
            available: available_features.contains(f),
        })
        .collect())
}
