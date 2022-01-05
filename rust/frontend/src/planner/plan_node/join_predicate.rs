#[derive(Debug, Clone)]
pub struct JoinPredicate {
    // TODO(expr)
    keys: Vec<(usize, usize)>,
}
#[allow(dead_code)]
impl JoinPredicate {
    // TODO(expr): should derive the equal columns and the
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        // TODO(expr)
        todo!()
    }
    pub fn is_equal_cond(&self) -> bool {
        todo!()
    }
    pub fn euqal_keys(&self) -> Vec<(usize, usize)> {
        self.keys.clone()
    }
    pub fn left_keys(&self) -> Vec<usize> {
        self.keys.iter().map(|(left, _)| *left).collect()
    }
    pub fn right_keys(&self) -> Vec<usize> {
        self.keys.iter().map(|(_, right)| *right).collect()
    }

    // TODO: our backend not support some equal columns and sonm NonEqual condition
    //   fn has_equal_cond(&self) ->bool{
    //     todo!()
    //   }
}
