use core::fmt::Display;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::ObjectName;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AnalyzeTarget {
    Id(u32),
    Table(ObjectName),
    MaterializedView(ObjectName),
    Index(ObjectName),
    Sink(ObjectName),
}

impl Display for AnalyzeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyzeTarget::Id(id) => write!(f, "ID {}", id),
            AnalyzeTarget::Table(name) => write!(f, "TABLE {}", name),
            AnalyzeTarget::MaterializedView(name) => write!(f, "MATERIALIZED VIEW {}", name),
            AnalyzeTarget::Index(name) => write!(f, "INDEX {}", name),
            AnalyzeTarget::Sink(name) => write!(f, "SINK {}", name),
        }
    }
}
