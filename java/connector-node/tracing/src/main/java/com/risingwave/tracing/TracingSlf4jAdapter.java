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

// Import log4j's ParameterizedMessage, so that we can format the messages
// with the same interpolation as log4j (i.e. "{}" instead of "%s").
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.LegacyAbstractLogger;

public class TracingSlf4jAdapter extends LegacyAbstractLogger {

    private final String name;

    public TracingSlf4jAdapter(String name) {
        this.name = name;
    }

    @Override
    public boolean isTraceEnabled() {
        return TracingSlf4jImpl.isEnabled(Level.TRACE);
    }

    @Override
    public boolean isDebugEnabled() {
        return TracingSlf4jImpl.isEnabled(Level.DEBUG);
    }

    @Override
    public boolean isInfoEnabled() {
        return TracingSlf4jImpl.isEnabled(Level.INFO);
    }

    @Override
    public boolean isWarnEnabled() {
        return TracingSlf4jImpl.isEnabled(Level.WARN);
    }

    @Override
    public boolean isErrorEnabled() {
        return TracingSlf4jImpl.isEnabled(Level.ERROR);
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return null;
    }

    @Override
    protected void handleNormalizedLoggingCall(
            Level level,
            Marker marker,
            String messagePattern,
            Object[] arguments,
            Throwable throwable) {
        var pm = new ParameterizedMessage(messagePattern, arguments, throwable);
        var message = pm.getFormattedMessage();

        String stackTrace = null;
        if (throwable != null) {
            stackTrace = ExceptionUtils.getStackTrace(throwable);
        }

        TracingSlf4jImpl.event(name, level, message, stackTrace);
    }
}
