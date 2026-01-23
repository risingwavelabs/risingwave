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

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql;

import static io.debezium.connector.common.OffsetUtils.longOffsetValue;

import io.debezium.connector.SnapshotType;
import io.debezium.connector.binlog.BinlogOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlOffsetContext extends BinlogOffsetContext<SourceInfo> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(MySqlOffsetContext.class);

    public MySqlOffsetContext(
            SnapshotType snapshot,
            boolean snapshotCompleted,
            TransactionContext transactionContext,
            IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
            SourceInfo sourceInfo) {
        super(
                snapshot,
                snapshotCompleted,
                transactionContext,
                incrementalSnapshotContext,
                sourceInfo);
    }

    public static MySqlOffsetContext initial(MySqlConnectorConfig config) {
        final MySqlOffsetContext offset =
                new MySqlOffsetContext(
                        null,
                        false,
                        new TransactionContext(),
                        config.isReadOnlyConnection()
                                ? new MySqlReadOnlyIncrementalSnapshotContext<>()
                                : new SignalBasedIncrementalSnapshotContext<>(),
                        new SourceInfo(config));
        offset.setBinlogStartPoint("", 0L); // start from the beginning of the binlog
        return offset;
    }

    public static class Loader extends BinlogOffsetContext.Loader<MySqlOffsetContext> {

        private final MySqlConnectorConfig connectorConfig;

        public Loader(MySqlConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public MySqlOffsetContext load(Map<String, ?> offset) {
            final String binlogFilename =
                    (String) offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
            if (binlogFilename == null) {
                throw new ConnectException(
                        "Source offset '"
                                + SourceInfo.BINLOG_FILENAME_OFFSET_KEY
                                + "' parameter is missing");
            }
            long binlogPosition = longOffsetValue(offset, SourceInfo.BINLOG_POSITION_OFFSET_KEY);
            final MySqlOffsetContext offsetContext =
                    new MySqlOffsetContext(
                            loadSnapshot(offset).orElse(null),
                            loadSnapshotCompleted(offset),
                            TransactionContext.load(offset),
                            connectorConfig.isReadOnlyConnection()
                                    ? MySqlReadOnlyIncrementalSnapshotContext.load(offset)
                                    : SignalBasedIncrementalSnapshotContext.load(offset),
                            new SourceInfo(connectorConfig));
            offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
            offsetContext.setInitialSkips(
                    longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY),
                    (int) longOffsetValue(offset, SourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY));
            offsetContext.setCompletedGtidSet((String) offset.get(GTID_SET_KEY)); // may be null

            LOGGER.debug("provided offset: " + offset);
            long serverId = longOffsetValue(offset, SourceInfo.SERVER_ID_KEY);
            offsetContext.setServerIdFromOffset(serverId);
            long timestamp = longOffsetValue(offset, TIMESTAMP_KEY);
            offsetContext.setTsSecFromOffset(timestamp);
            return offsetContext;
        }
    }
}
