/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Ported from https://github.com/MrFriendly-B-V/tracing-slf4j,
// which is licensed under the Apache License, Version 2.0.

package com.risingwave.tracing;

import com.risingwave.java.binding.Binding;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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

    /**
     * Optional Java-side per-logger level overrides.
     *
     * <p>This is intentionally implemented on the Java side because the Rust-side JNI bridge
     * currently rewrites all Java logs to the same tracing target ("risingwave_connector_node"),
     * which makes it hard to control log levels for individual Java loggers via {@code RUST_LOG}.
     *
     * <p>Env var format: {@code RW_JAVA_LOG="info,io.debezium=error,org.apache.kafka=warn"}
     *
     * <ul>
     *   <li>Comma-separated directives.
     *   <li>Directive can be {@code <prefix>=<level>} (prefix match via {@code startsWith}).
     *   <li>A bare {@code <level>} sets the default level for all loggers without explicit rules.
     *   <li>Supported levels: {@code off,error,warn,info,debug,trace} (case-insensitive).
     * </ul>
     */
    private static final JavaLoggerLevelOverrides javaLoggerLevelOverrides =
            JavaLoggerLevelOverrides.fromEnv("RW_JAVA_LOG");

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

    /**
     * Check whether a log event is enabled for the given Java logger name.
     *
     * <p>This applies both:
     *
     * <ul>
     *   <li>Rust-side filtering (via {@link Binding#tracingSlf4jEventEnabled(int)}).
     *   <li>Java-side per-logger overrides (via {@code RW_JAVA_LOG}).
     * </ul>
     */
    public static boolean isEnabled(String loggerName, Level level) {
        int bindingLevel = levelToBinding(level);
        if (bindingLevel < 0) {
            return false;
        }

        // Rust-side filter (target-level only).
        if (!Binding.tracingSlf4jEventEnabled(bindingLevel)) {
            return false;
        }

        // Java-side per-logger overrides.
        return javaLoggerLevelOverrides.isEnabled(loggerName, bindingLevel);
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

    private static class JavaLoggerLevelOverrides {
        private final int defaultMaxBindingLevel;
        private final List<Rule> rules;

        private JavaLoggerLevelOverrides(int defaultMaxBindingLevel, List<Rule> rules) {
            this.defaultMaxBindingLevel = defaultMaxBindingLevel;
            this.rules = rules;
        }

        static JavaLoggerLevelOverrides fromEnv(String envKey) {
            String raw = System.getenv(envKey);
            if (raw == null || raw.trim().isEmpty()) {
                // Default: allow all levels, letting Rust-side filtering decide.
                return new JavaLoggerLevelOverrides(BINDING_TRACE, List.of());
            }
            return parse(raw);
        }

        boolean isEnabled(String loggerName, int bindingLevel) {
            if (bindingLevel < 0) {
                return false;
            }
            int maxAllowed = defaultMaxBindingLevel;
            if (loggerName != null && !loggerName.isEmpty()) {
                for (Rule r : rules) {
                    if (loggerName.startsWith(r.prefix)) {
                        maxAllowed = r.maxBindingLevel;
                        break;
                    }
                }
            }
            return bindingLevel <= maxAllowed;
        }

        private static JavaLoggerLevelOverrides parse(String raw) {
            int defaultMax = BINDING_TRACE;
            List<Rule> rules = new ArrayList<>();

            for (String part : raw.split(",")) {
                String token = part.trim();
                if (token.isEmpty()) {
                    continue;
                }

                int eq = token.indexOf('=');
                if (eq < 0) {
                    // Bare level => default.
                    Optional<Integer> lvl = parseMaxBindingLevel(token);
                    if (lvl.isPresent()) {
                        defaultMax = lvl.get();
                    }
                    continue;
                }

                String prefix = token.substring(0, eq).trim();
                String levelStr = token.substring(eq + 1).trim();
                if (prefix.isEmpty()) {
                    continue;
                }

                // Special prefixes for default.
                if (prefix.equals("*") || prefix.equalsIgnoreCase("root")) {
                    Optional<Integer> lvl = parseMaxBindingLevel(levelStr);
                    if (lvl.isPresent()) {
                        defaultMax = lvl.get();
                    }
                    continue;
                }

                Optional<Integer> lvl = parseMaxBindingLevel(levelStr);
                if (lvl.isPresent()) {
                    rules.add(new Rule(prefix, lvl.get()));
                }
            }

            // Longest prefix match wins: sort by descending prefix length.
            rules.sort(Comparator.comparingInt((Rule r) -> r.prefix.length()).reversed());
            return new JavaLoggerLevelOverrides(defaultMax, List.copyOf(rules));
        }

        private static Optional<Integer> parseMaxBindingLevel(String rawLevel) {
            if (rawLevel == null) {
                return Optional.empty();
            }
            String s = rawLevel.trim().toLowerCase(Locale.ROOT);
            switch (s) {
                case "off":
                    return Optional.of(-1);
                case "error":
                    return Optional.of(BINDING_ERROR);
                case "warn":
                case "warning":
                    return Optional.of(BINDING_WARN);
                case "info":
                    return Optional.of(BINDING_INFO);
                case "debug":
                    return Optional.of(BINDING_DEBUG);
                case "trace":
                    return Optional.of(BINDING_TRACE);
                default:
                    return Optional.empty();
            }
        }

        private static class Rule {
            private final String prefix;
            private final int maxBindingLevel;

            private Rule(String prefix, int maxBindingLevel) {
                this.prefix = prefix;
                this.maxBindingLevel = maxBindingLevel;
            }
        }
    }
}
