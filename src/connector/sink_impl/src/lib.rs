#![expect(dead_code)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(generators)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(box_patterns)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(lint_reasons)]
#![feature(lazy_cell)]
#![feature(result_option_inspect)]
#![feature(let_chains)]
#![feature(box_into_inner)]
#![feature(type_alias_impl_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![feature(async_fn_in_trait)]
#![feature(associated_type_defaults)]
#![feature(impl_trait_in_assoc_type)]
#![feature(iter_from_generator)]
#![feature(if_let_guard)]
#![feature(iterator_try_collect)]

pub mod sink;

use futures::future::BoxFuture;
use futures::FutureExt;
use risingwave_connector_common::sink::boxed::BoxCoordinator;
use risingwave_connector_common::sink::catalog::SinkCatalog;
use risingwave_connector_common::sink::{Sink, SinkError, SinkParam};
pub(crate) use risingwave_connector_common::*;
use risingwave_pb::catalog::PbSink;

use crate::sink::build_sink;

#[export_name = "__exported_validate_sink"]
pub fn validate_sink(
    prost_sink_catalog: &PbSink,
) -> BoxFuture<'_, std::result::Result<(), SinkError>> {
    async move {
        let sink_catalog = SinkCatalog::from(prost_sink_catalog);
        let param = SinkParam::from(sink_catalog);

        let sink = build_sink(param)?;

        dispatch_sink!(sink, sink, Ok(sink.validate().await?))
    }
    .boxed()
}

#[export_name = "__exported_build_box_coordinator"]
pub fn build_box_coordinator(
    param: SinkParam,
) -> BoxFuture<'static, std::result::Result<BoxCoordinator, SinkError>> {
    async move {
        let sink = build_sink(param)?;
        dispatch_sink!(sink, sink, {
            let coordinator = sink.new_coordinator().await?;
            Ok(Box::new(coordinator) as BoxCoordinator)
        })
    }
    .boxed()
}
