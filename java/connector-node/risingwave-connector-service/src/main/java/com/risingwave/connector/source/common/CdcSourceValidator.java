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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.source.SourceTypeE;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class CdcSourceValidator implements AutoCloseable {

    public static void ensureRequiredProps(Map<String, String> props, boolean isCdcSourceJob) {
        ensurePropNotBlank(props, DbzConnectorConfig.HOST);
        ensurePropNotBlank(props, DbzConnectorConfig.PORT);
        ensurePropNotBlank(props, DbzConnectorConfig.DB_NAME);
        ensurePropNotBlank(props, DbzConnectorConfig.USER);
        ensurePropsExists(props, DbzConnectorConfig.PASSWORD);
        // ensure table name is passed by user in non-sharing mode
        if (!isCdcSourceJob) {
            ensurePropNotBlank(props, DbzConnectorConfig.TABLE_NAME);
        }
    }

    public static void ensurePropNotBlank(Map<String, String> props, String name) {
        ensurePropsExists(props, name);
        if (StringUtils.isBlank(props.get(name))) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' cannot be empty. Please check the WITH properties", name));
        }
    }

    public static void ensurePropsExists(Map<String, String> props, String name) {
        if (!props.containsKey(name)) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' is not found. Please check the WITH properties", name));
        }
    }

    /**
     * Validate queue.memory.ratio if specified. The ratio must be a valid number between
     * MIN_QUEUE_MAX_MEMORY_RATIO and MAX_QUEUE_MAX_MEMORY_RATIO.
     */
    public static void validateQueueMemoryRatio(Map<String, String> props) {
        String ratioStr = props.get("debezium.queue.memory.ratio");
        if (ratioStr == null) {
            return; // Not specified, use default
        }

        double ratio;
        try {
            ratio = Double.parseDouble(ratioStr);
        } catch (NumberFormatException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be a valid number, got: '%s'",
                            ratioStr));
        }

        // Validate ratio is in reasonable range (1% - 80%)
        if (ratio < DbzConnectorConfig.MIN_QUEUE_MAX_MEMORY_RATIO
                || ratio > DbzConnectorConfig.MAX_QUEUE_MAX_MEMORY_RATIO) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be between %s and %s, got: %s",
                            DbzConnectorConfig.MIN_QUEUE_MAX_MEMORY_RATIO,
                            DbzConnectorConfig.MAX_QUEUE_MAX_MEMORY_RATIO,
                            ratio));
        }
    }

    /**
     * Requires an explicitly supplied heartbeat interval to be a positive signed 32-bit integer.
     * Validates user-supplied options, not the final Debezium configuration. If omitted,
     * DbzConnectorConfig supplies the connector default. Keep this policy in sync with Rust's
     * validate_heartbeat_interval (CREATE and ALTER).
     */
    public static void validateHeartbeatInterval(Map<String, String> props) {
        String intervalStr = props.get("debezium.heartbeat.interval.ms");
        if (intervalStr == null) {
            return; // Not specified, use default
        }

        int interval;
        try {
            // Match Rust's ASCII integer syntax; Integer.parseInt also accepts Unicode digits.
            if (!intervalStr.matches("[+-]?[0-9]+")) {
                throw new NumberFormatException();
            }
            // Debezium uses a signed 32-bit integer; RW also rejects zero to keep heartbeats
            // enabled.
            interval = Integer.parseInt(intervalStr);
            if (interval <= 0) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.heartbeat.interval.ms' must be a positive integer, got: '%s'",
                            intervalStr));
        }
    }

    public static void validateHeartbeatTableAutoInitialize(
            Map<String, String> props, SourceTypeE sourceType) {
        var value = props.get(DbzConnectorConfig.HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY);
        if (value == null) {
            return;
        }
        if (!value.equals("true") && !value.equals("false")) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'%s' must be 'true' or 'false'",
                            DbzConnectorConfig.HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY));
        }
        if (value.equals("false")) {
            return;
        }

        switch (sourceType) {
            case ORACLE:
                break;
            default:
                throw ValidatorUtils.invalidArgument(
                        String.format(
                                "'%s=true' is not supported for connector '%s'",
                                DbzConnectorConfig.HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY,
                                sourceType.name()));
        }
    }

    public void validateAll() {
        validateDbConfig();
        validateUserPrivilege();
        // If the source connector is created by share source, it will capture events from
        // multiple tables, skip validate its schema
        if (!isCdcSourceJob()) {
            validateTable();
        }
    }

    /** Validate the config of the upstream database */
    abstract void validateDbConfig();

    /** Validate the required privileges to start the connector */
    abstract void validateUserPrivilege();

    /** Validate the properties of the source table */
    abstract void validateTable();

    /** Check if the validation is for CDC source job */
    abstract boolean isCdcSourceJob();
}
