/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.oracle.logminer.buffered;

import com.risingwave.connector.cdc.debezium.internal.OracleMiningInitialization;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OraclePartition;
import io.debezium.connector.oracle.Scn;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import java.sql.SQLException;

/** Reports Debezium's selected initial mining position before dispatching any mined events. */
public class RisingWaveBufferedLogMinerStreamingChangeEventSource
        extends BufferedLogMinerStreamingChangeEventSource {
    private final String logicalName;
    private boolean initialPositionReported;

    public RisingWaveBufferedLogMinerStreamingChangeEventSource(
            OracleConnectorConfig config,
            OracleConnection connection,
            EventDispatcher<OraclePartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            OracleDatabaseSchema schema,
            Configuration jdbcConfig,
            LogMinerStreamingChangeEventSourceMetrics metrics) {
        super(config, connection, dispatcher, errorHandler, clock, schema, jdbcConfig, metrics);
        this.logicalName = config.getLogicalName();
    }

    @Override
    protected boolean startMiningSession(Scn startScn, Scn endScn, int attempts)
            throws SQLException {
        boolean started = super.startMiningSession(startScn, endScn, attempts);
        if (started && !initialPositionReported) {
            try {
                OracleMiningInitialization.report(
                        logicalName,
                        getOffsetContext().getScn().toString(),
                        getPartition().getSourcePartition(),
                        getOffsetContext().getOffset());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SQLException(
                        "Interrupted while reporting Oracle mining initialization", e);
            }
            initialPositionReported = true;
        }
        return started;
    }
}
