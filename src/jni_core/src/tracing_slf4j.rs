// Copyright 2023 RisingWave Labs
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
use jni::sys::jint;
use tracing::Level;

use crate::{execute_and_catch, EnvParam};

#[no_mangle]
pub(crate) extern "system" fn Java_com_risingwave_java_binding_Binding_tracingSlf4jEvent(
    env: EnvParam<'_>,
    class_name: JString<'_>,
    level: jint,
    message: JString<'_>,
) {
    execute_and_catch(env, move |env| {
        let class_name = env.get_string(&class_name)?;
        let class_name: Cow<'_, str> = (&class_name).into();

        let message = env.get_string(&message)?;
        let message: Cow<'_, str> = (&message).into();

        macro_rules! event {
            ($lvl:expr) => {
                tracing::event!(
                    target: "risingwave_connector_node",
                    $lvl,
                    class = class_name.as_ref(),
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
