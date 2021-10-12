use crate::executor::join::JoinType::Inner;

mod hash_join;
mod hash_join_state;
mod nested_loop_join;

#[derive(Copy, Clone, Debug)]
pub(super) enum JoinType {
    Inner,
    LeftOuter,
    /// Semi join when probe side should output when matched
    LeftSemi,
    /// Anti join when probe side should not output when matched
    LeftAnti,
    RightOuter,
    /// Semi join when build side should output when matched
    RightSemi,
    /// Anti join when build side should output when matched
    RightAnti,
    FullOuter,
}

impl JoinType {
    #[inline(always)]
    pub(super) fn need_join_remaining(self) -> bool {
        matches!(
            self,
            JoinType::RightOuter | JoinType::RightAnti | JoinType::FullOuter
        )
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}
