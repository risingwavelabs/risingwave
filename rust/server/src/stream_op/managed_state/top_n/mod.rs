mod top_n_bottom_n_state;
mod top_n_state;

pub use top_n_bottom_n_state::ManagedTopNBottomNState;
pub use top_n_state::ManagedTopNState;

pub mod variants {
    pub const TOP_N_MIN: usize = 0;
    pub const TOP_N_MAX: usize = 1;
}
