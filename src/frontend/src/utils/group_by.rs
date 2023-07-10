use crate::expr::ExprImpl;

#[derive(Debug, Clone)]
pub enum GroupBy {
    GroupKey(Vec<ExprImpl>),
    GroupingSets(Vec<Vec<ExprImpl>>),
}

impl GroupBy {
    pub fn is_empty(&self) -> bool {
        match self {
            GroupBy::GroupKey(group_key) => group_key.is_empty(),
            GroupBy::GroupingSets(grouping_sets) => grouping_sets.is_empty(),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &ExprImpl> + '_> {
        match self {
            GroupBy::GroupKey(group_key) => Box::new(group_key.iter()),
            GroupBy::GroupingSets(grouping_sets) => {
                Box::new(grouping_sets.iter().flat_map(|v| v.iter()))
            }
        }
    }

    pub fn iter_mut(&mut self) -> Box<dyn Iterator<Item = &mut ExprImpl> + '_> {
        match self {
            GroupBy::GroupKey(group_key) => Box::new(group_key.iter_mut()),
            GroupBy::GroupingSets(grouping_sets) => {
                Box::new(grouping_sets.iter_mut().flat_map(|v| v.iter_mut()))
            }
        }
    }
}
