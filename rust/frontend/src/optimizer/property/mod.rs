//! Define all property of plan tree node, which actually represent property of the node's result.
//!
//! We have physical property [`Order`] and [`Distribution`] which is on batch or stream operator,
//! also, we have logical property which all PlanNode has.
//!
//! We have not give any common abstract trait for the property yet. They are not so much and we
//! don't need get a common behaviour now. we can treat them as different traits of the
//! [`PlanNode`](super::plan_node::PlanNode) now and refactor them when our optimizer need more
//! (such as an optimizer based on the Volcano/Cascades model).
mod convention;
pub use convention::*;
mod order;
pub use order::*;
mod distribution;
pub use distribution::*;
mod schema;
pub use schema::*;
