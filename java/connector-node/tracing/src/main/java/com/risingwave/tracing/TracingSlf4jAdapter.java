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

// Ported from https://github.com/MrFriendly-B-V/tracing-slf4j,
// which is licensed under the Apache License, Version 2.0.

package com.risingwave.tracing;

import org.slf4j.Logger;
import org.slf4j.Marker;

public class TracingSlf4jAdapter implements Logger {

    private final String name;

    public TracingSlf4jAdapter(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean isTraceEnabled() {
        return true;
    }

    @Override
    public void trace(String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, arg));
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, arg1, arg2));
    }

    @Override
    public void trace(String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, arguments));
    }

    @Override
    public void trace(String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.TRACE, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return true;
    }

    @Override
    public void trace(Marker marker, String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, msg);
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, arg));
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, arg1, arg2));
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.TRACE, String.format(format, argArray));
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.TRACE, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public void debug(String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arg));
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arg1, arg2));
    }

    @Override
    public void debug(String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arguments));
    }

    @Override
    public void debug(String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.DEBUG, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return true;
    }

    @Override
    public void debug(Marker marker, String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, msg);
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arg));
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arg1, arg2));
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.DEBUG, String.format(format, arguments));
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.DEBUG, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public void info(String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arg));
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arg1, arg2));
    }

    @Override
    public void info(String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arguments));
    }

    @Override
    public void info(String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.INFO, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return true;
    }

    @Override
    public void info(Marker marker, String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, msg);
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arg));
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arg1, arg2));
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.INFO, String.format(format, arguments));
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.INFO, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void warn(String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arg));
    }

    @Override
    public void warn(String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arguments));
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arg1, arg2));
    }

    @Override
    public void warn(String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.WARN, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return true;
    }

    @Override
    public void warn(Marker marker, String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, msg);
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arg));
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arg1, arg2));
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.WARN, String.format(format, arguments));
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.WARN, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public void error(String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, msg);
    }

    @Override
    public void error(String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arg));
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arg1, arg2));
    }

    @Override
    public void error(String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arguments));
    }

    @Override
    public void error(String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.ERROR, String.format("%s: %s", msg, t.toString()));
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return true;
    }

    @Override
    public void error(Marker marker, String msg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, msg);
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arg));
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arg1, arg2));
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
        TracingSlf4jImpl.event(name, TracingSlf4jImpl.ERROR, String.format(format, arguments));
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
        TracingSlf4jImpl.event(
                name, TracingSlf4jImpl.ERROR, String.format("%s: %s", msg, t.toString()));
    }
}
