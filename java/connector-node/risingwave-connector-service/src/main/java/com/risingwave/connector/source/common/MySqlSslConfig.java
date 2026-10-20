/*
 * Copyright 2026 RisingWave Labs
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

package com.risingwave.connector.source.common;

import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** Translate MySQL SSL modes without changing legacy CA or pass-through behavior. */
public final class MySqlSslConfig {
    private final String mode;

    private MySqlSslConfig(String mode) {
        this.mode = mode;
    }

    public static MySqlSslConfig fromProperties(Map<String, String> properties) {
        // Keep canonical-option precedence and Connector/J's case-insensitive spellings.
        String value =
                properties.getOrDefault(
                        "ssl.mode",
                        properties.getOrDefault("debezium.database.sslmode", "disabled"));
        String mode =
                switch (value.toLowerCase(Locale.ROOT)) {
                    case "disabled", "disable" -> "disabled";
                    case "preferred", "prefer" -> "preferred";
                    case "required", "require" -> "required";
                    case "verify-ca", "verifyca", "verify_ca" -> "verify_ca";
                    case "verify-full", "verifyfull", "verify_identity" -> "verify_identity";
                    default ->
                            throw new IllegalArgumentException(
                                    "invalid MySQL ssl.mode; expected disabled, preferred, required, verify-ca, or verify-full");
                };
        // MySQL has never consumed ssl.root.cert or debezium.database.sslrootcert.
        // Keep ignoring them, including empty, missing, and malformed certificate files.
        // DbzConnectorConfig retains the existing merging of raw Debezium properties.
        return new MySqlSslConfig(mode);
    }

    public void configureJdbc(Properties properties) {
        properties.setProperty("sslMode", mode.toUpperCase(Locale.ROOT));
    }

    public void configureDebezium(Properties properties) {
        properties.setProperty("database.ssl.mode", mode);
    }
}
