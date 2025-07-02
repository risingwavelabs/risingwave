use std::error::Error;

use tracing::{info, info_span};

macro_rules! my_info {
    ($($tt:tt)*) => {
        info!($($tt)*);
    };
}

fn main() {
    let err = "foo".parse::<i32>().unwrap_err();

    let _ = format!("{err}");
    let _ = format!("{err}");
    let _ = format!("{err:#}");
    let _ = format!("{err:?}");
    let _ = format!("{err}");
    let _ = format!("{err}");

    let _ = format!("{}", &err);
    let _ = format!("{}", &&err);
    let _ = format!("{}", err.source().unwrap());

    let _ = format!("{}", &err as &dyn Error);
    let _ = format!("{}", &err as &(dyn Error + Send));
    let _ = format!("{}", Box::new(&err));
    let _ = format!("{}", Box::new(err.clone()));

    println!("{err}");
    info!("{}", err);
    my_info!("{}", err);

    tracing::field::display(&err);
    tracing::field::debug(err.clone());

    info!(%err, "233");
    info!(?err, "233");
    info!(%err, "{}", err);
    let _ = info_span!("span", %err);

    let _ = format!(
        "this is a really long message, test lint span: {err} {err} {err} "
    );

    let _ = err.to_string();
    let _ = (err.clone()).to_string();
    let _ = err.to_string().to_string();
    let _ = (&&err).to_string();

    use anyhow::anyhow;

    let _ = anyhow!("{}", err);
    let _ = anyhow!("{:?}", err);
    let _ = anyhow!("some error occurred: {}", err);
    let _ = anyhow!("some error occurred: {:?}", err);

    // `anyhow::Error` does not implement `Error` trait, test the special path here.
    let make_anyhow_err = || anyhow!("foobar");
    let anyhow_err = make_anyhow_err();

    let _ = format!("{anyhow_err}");
    let _ = format!("{}", &anyhow_err);
    let _ = format!("{}", &&anyhow_err);
    let _ = format!("{}", Box::new(&anyhow_err)); // TODO: fail to lint

    tracing::field::display(&anyhow_err);
    tracing::field::debug(make_anyhow_err());

    let _ = anyhow_err.to_string();
    let _ = (&&anyhow_err).to_string();

    let _ = anyhow!("{}", anyhow_err);
    let _ = anyhow!("some error occurred: {:?}", anyhow_err);

    use thiserror_ext::AsReport;

    let _ = anyhow!("{}", err.as_report());
    let _ = anyhow!("some error occurred: {}", err.as_report());
    let _ = anyhow!("{:?}", anyhow_err.as_report());
    let _ = anyhow!("some error occurred: {:?}", anyhow_err.as_report());

    let box_dyn_err_1: Box<dyn Error> = Box::new(err.clone());
    let box_dyn_err_2: Box<dyn Error + Send> = Box::new(err.clone());
    let box_dyn_err_3: Box<dyn Error + Send + Sync> = Box::new(err.clone());

    // TODO: fail to lint
    let _ = format!("{box_dyn_err_1}");
    info!("{}", box_dyn_err_2);
    let _ = box_dyn_err_3.to_string();

    // We tolerate formatting error in `#[error("...")]`. Should not emit lint.
    // TODO: shall we lint this as well?
    #[derive(thiserror::Error, Debug)]
    #[error("my io error: {0}")]
    struct _MyIoError(std::io::Error);
}
