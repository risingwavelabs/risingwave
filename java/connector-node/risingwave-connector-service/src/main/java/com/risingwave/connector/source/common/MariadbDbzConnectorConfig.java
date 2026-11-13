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

import io.debezium.connector.mariadb.MariaDbStreamingChangeEventSource;
import java.util.Map;
import java.util.Properties;

public class MariadbDbzConnectorConfig extends MysqlDbzConnectorConfig {
    MariadbDbzConnectorConfig(CdcSource source) {
        super(source);
    }

    public static String normalizeSslMode(String sslMode) {
        return switch (sslMode) {
            case "disabled" -> "disable";
            case "preferred" ->
                    throw new IllegalArgumentException(
                            "MariaDB CDC does not support ssl.mode 'preferred'; use 'required',"
                                    + " 'verify-ca', or 'verify-full'");
            case "required" -> "trust";
            default -> sslMode;
        };
    }

    public static String sslMode(Map<String, String> userProps) {
        return normalizeSslMode(
                userProps.getOrDefault(
                        SSL_MODE, userProps.getOrDefault("debezium.database.sslmode", "disabled")));
    }

    public static String sslRootCert(Map<String, String> userProps) {
        return userProps.getOrDefault(
                "ssl.root.cert", userProps.get("debezium.database.sslrootcert"));
    }

    @Override
    protected String getPropertiesTemplate() {
        return "risingwave-mariadb.properties";
    }

    @Override
    protected void customizeDebeziumProperties(Properties dbzProps) {
        super.customizeDebeziumProperties(dbzProps);
        dbzProps.setProperty("database.ssl.mode", sslMode(userProps));
        var rootCert = sslRootCert(userProps);
        if (rootCert != null) {
            dbzProps.setProperty("driver.serverSslCert", rootCert);
            dbzProps.setProperty(MariaDbStreamingChangeEventSource.SSL_ROOT_CERT_CONFIG, rootCert);
        }
    }
}
