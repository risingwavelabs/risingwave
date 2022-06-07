use risingwave_pb::batch_plan::{scan_range, ScanRange as ScanRangeProst};

use crate::expr::{Expr, Literal};

/// See also [`ScanRangeProst`]
#[derive(Debug, Clone)]
pub struct ScanRange {
    pub eq_conds: Vec<Literal>,
    /// ((lb, inclusive), (ub, inclusive))
    #[allow(clippy::type_complexity)]
    pub range: Option<(Option<(Literal, bool)>, Option<(Literal, bool)>)>,
}

impl ScanRange {
    pub fn to_protobuf(&self) -> ScanRangeProst {
        ScanRangeProst {
            eq_conds: self
                .eq_conds
                .iter()
                .map(|lit| lit.to_expr_proto())
                .collect(),
            range: self.range.as_ref().map(|(lb, ub)| scan_range::Range {
                lower_bound: lb
                    .as_ref()
                    .map(|(lit, inclusive)| scan_range::range::Bound {
                        value: Some(lit.to_expr_proto()),
                        inclusive: *inclusive,
                    }),
                upper_bound: ub
                    .as_ref()
                    .map(|(lit, inclusive)| scan_range::range::Bound {
                        value: Some(lit.to_expr_proto()),
                        inclusive: *inclusive,
                    }),
            }),
        }
    }

    pub fn is_full_table_scan(&self) -> bool {
        self.eq_conds.is_empty() && self.range.is_none()
    }

    pub fn full_table_scan() -> Self {
        Self {
            eq_conds: vec![],
            range: None,
        }
    }
}
