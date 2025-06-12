use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogBackfillType {
    Source,
    SnapshotBackfill,
    ArrangementOrNoShuffle,
}

impl Display for CatalogBackfillType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogBackfillType::Source => write!(f, "SOURCE"),
            CatalogBackfillType::SnapshotBackfill => write!(f, "SNAPSHOT_BACKFILL"),
            CatalogBackfillType::ArrangementOrNoShuffle => write!(f, "ARRANGEMENT_OR_NO_SHUFFLE"),
        }
    }
}
