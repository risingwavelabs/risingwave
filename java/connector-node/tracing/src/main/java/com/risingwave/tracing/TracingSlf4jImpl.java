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

// Ported from https://github.com/MrFriendly-B-V/tracing-slf4j,
// which is licensed under the Apache License, Version 2.0.

package com.risingwave.tracing;

import com.risingwave.java.binding.Binding;
import org.slf4j.event.Level;

public class TracingSlf4jImpl {
    private static final int BINDING_ERROR = 0;
    private static final int BINDING_WARN = 1;
    private static final int BINDING_INFO = 2;
    private static final int BINDING_DEBUG = 3;
    private static final int BINDING_TRACE = 4;

    // TODO: We may support changing the log level at runtime in the future.
    private static final boolean isErrorEnabled = Binding.tracingSlf4jEventEnabled(BINDING_ERROR);
    private static final boolean isWarnEnabled = Binding.tracingSlf4jEventEnabled(BINDING_WARN);
    private static final boolean isInfoEnabled = Binding.tracingSlf4jEventEnabled(BINDING_INFO);
    private static final boolean isDebugEnabled = Binding.tracingSlf4jEventEnabled(BINDING_DEBUG);
    private static final boolean isTraceEnabled = Binding.tracingSlf4jEventEnabled(BINDING_TRACE);

    private static int levelToBinding(Level level) {
        switch (level) {
            case ERROR:
                return BINDING_ERROR;
            case WARN:
                return BINDING_WARN;
            case INFO:
                return BINDING_INFO;
            case DEBUG:
                return BINDING_DEBUG;
            case TRACE:
                return BINDING_TRACE;
            default:
                return -1;
        }
    }

    public static void event(String name, Level level, String message, String stackTrace) {
        Binding.tracingSlf4jEvent(
                Thread.currentThread().getName(), name, levelToBinding(level), message, stackTrace);
    }

    public static boolean isEnabled(Level level) {
        switch (level) {
            case ERROR:
                return isErrorEnabled;
            case WARN:
                return isWarnEnabled;
            case INFO:
                return isInfoEnabled;
            case DEBUG:
                return isDebugEnabled;
            case TRACE:
                return isTraceEnabled;
            default:
                return false;
        }
    }
}
