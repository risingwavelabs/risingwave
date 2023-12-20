use jni::objects::JString;
use jni::sys::jint;
use tracing::Level;

use crate::EnvParam;

#[no_mangle]
pub(crate) extern "system" fn Java_com_risingwave_java_binding_Binding_tracingSlf4jEvent(
    mut env: EnvParam<'_>,
    class_name: JString<'_>,
    level: jint,
    message: JString<'_>,
) {
    let Ok(class_name) = env.get_string(&class_name) else {
        return;
    };
    let class_name: String = class_name.into();

    let Ok(message) = env.get_string(&message) else {
        return;
    };
    let message: String = message.into();

    macro_rules! event {
        ($lvl:expr) => {
            tracing::event!(
                target: "risingwave_connector_node",
                $lvl,
                class = class_name,
                "{message}",
            )
        };
    }

    // See `com.risingwave.connector.api.tracing.TracingSlf4jImpl`.
    match level {
        0 => event!(Level::ERROR),
        1 => event!(Level::WARN),
        2 => event!(Level::INFO),
        3 => event!(Level::DEBUG),
        4 => event!(Level::TRACE),
        _ => unreachable!(),
    }
}
