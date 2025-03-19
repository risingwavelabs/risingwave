// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use jni::objects::JString;
use jni::sys::{jboolean, jint};
use tracing::Level;

use crate::{EnvParam, execute_and_catch};

const TARGET: &str = "risingwave_connector_node";

#[no_mangle]
pub(crate) extern "system" fn Java_com_risingwave_java_binding_Binding_tracingSlf4jEvent(
    env: EnvParam<'_>,
    thread_name: JString<'_>,
    class_name: JString<'_>,
    level: jint,
    message: JString<'_>,
    stack_trace: JString<'_>,
) {
    execute_and_catch(env, move |env| {
        let thread_name = env.get_string(&thread_name)?;
        let thread_name: Cow<'_, str> = (&thread_name).into();

        let class_name = env.get_string(&class_name)?;
        let class_name: Cow<'_, str> = (&class_name).into();

        let message = env.get_string(&message)?;
        let message: Cow<'_, str> = (&message).into();

        let stack_trace = if stack_trace.is_null() {
            None
        } else {
            Some(env.get_string(&stack_trace)?)
        };
        let stack_trace: Option<Cow<'_, str>> = stack_trace.as_ref().map(|c| c.into());
        let stack_trace: Option<&str> = stack_trace.as_deref();

        macro_rules! event {
            ($lvl:expr) => {
                tracing::event!(
                    target: TARGET,
                    $lvl,
                    thread = &*thread_name,
                    class = &*class_name,
                    error = stack_trace,
                    "{message}",
                )
            };
        }

        // See `com.risingwave.tracing.TracingSlf4jImpl`.
        match level {
            0 => event!(Level::ERROR),
            1 => event!(Level::WARN),
            2 => event!(Level::INFO),
            3 => event!(Level::DEBUG),
            4 => event!(Level::TRACE),
            _ => unreachable!(),
        }

        Ok(())
    })
}

#[no_mangle]
pub(crate) extern "system" fn Java_com_risingwave_java_binding_Binding_tracingSlf4jEventEnabled(
    env: EnvParam<'_>,
    level: jint,
) -> jboolean {
    execute_and_catch(env, move |_env| {
        // Currently we only support `StaticDirective` in `tracing-subscriber`, so
        // filtering by fields like `thread` and `class` is not supported.
        // TODO: should we support dynamic `Directive`?
        macro_rules! event_enabled {
            ($lvl:expr) => {
                tracing::event_enabled!(
                    target: TARGET,
                    $lvl,
                )
            };
        }

        // See `com.risingwave.tracing.TracingSlf4jImpl`.
        let enabled = match level {
            0 => event_enabled!(Level::ERROR),
            1 => event_enabled!(Level::WARN),
            2 => event_enabled!(Level::INFO),
            3 => event_enabled!(Level::DEBUG),
            4 => event_enabled!(Level::TRACE),
            _ => unreachable!(),
        };

        Ok(enabled)
    })
    .into()
}
