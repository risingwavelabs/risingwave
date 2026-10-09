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

package io.debezium.connector.binlog.history;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogSourceInfo;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.gtid.MySqlGtidSetFactory;
import io.debezium.connector.mysql.history.MySqlHistoryRecordComparator;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;

public class BinlogHistoryRecordComparatorTest {
    private static final Map<String, Object> COMMITTED_OFFSET =
            Map.of(
                    "file", "mysql-bin.000003",
                    "pos", 489226L,
                    "server_id", 1L,
                    "ts_sec", 1000L);

    private final HistoryRecordComparator comparator =
            new MySqlHistoryRecordComparator(null, new MySqlGtidSetFactory());

    @Test
    public void snapshotRecordBeforeRestartPositionIsApplied() throws IOException {
        // Snapshot records carry the wall-clock time they were written at, which can be later
        // than the event time of the committed offset.
        HistoryRecord restart = restartPosition();
        assertTrue(
                comparator.isAtOrBefore(
                        recorded(
                                "{\"ts_sec\": 2000, \"file\": \"mysql-bin.000003\", \"pos\": 157,"
                                        + " \"server_id\": 0, \"snapshot\": \"INITIAL\"}"),
                        restart));
        assertTrue(
                comparator.isAtOrBefore(
                        recorded(
                                "{\"ts_sec\": 2000, \"file\": \"mysql-bin.000003\", \"pos\": 157,"
                                        + " \"snapshot\": \"INITIAL\"}"),
                        restart));
    }

    @Test
    public void restartPositionCarriesNoServerId() {
        assertFalse(
                loader().load(COMMITTED_OFFSET)
                        .getOffset()
                        .containsKey(BinlogSourceInfo.SERVER_ID_KEY));
    }

    @Test
    public void streamingRecordsAreComparedByCoordinates() throws IOException {
        HistoryRecord restart = restartPosition();
        assertFalse(
                comparator.isAtOrBefore(
                        recorded(
                                "{\"ts_sec\": 900, \"file\": \"mysql-bin.000003\", \"pos\": 500000,"
                                        + " \"server_id\": 1}"),
                        restart));
        // The server id of a position is the origin of its event, which can differ within one
        // binlog stream, e.g. when reading from a replica.
        assertTrue(
                comparator.isAtOrBefore(
                        recorded(
                                "{\"ts_sec\": 1100, \"file\": \"mysql-bin.000003\", \"pos\": 400000,"
                                        + " \"server_id\": 2}"),
                        restart));
    }

    @Test
    public void positionsFromDifferentServersAreComparedByTimestamp() throws IOException {
        for (long serverId : new long[] {223344L, 4000000000L}) {
            HistoryRecord earlier =
                    recorded(
                            "{\"ts_sec\": 1000, \"file\": \"mysql-bin.000009\", \"pos\": 900,"
                                    + " \"server_id\": "
                                    + serverId
                                    + "}");
            HistoryRecord later =
                    recorded(
                            "{\"ts_sec\": 2000, \"file\": \"mysql-bin.000002\", \"pos\": 154,"
                                    + " \"server_id\": "
                                    + (serverId + 1)
                                    + "}");
            assertTrue(comparator.isAtOrBefore(earlier, later));
            assertFalse(comparator.isAtOrBefore(later, earlier));
        }
    }

    @Test
    public void positionsWithDifferentBaseNamesAreApplied() throws IOException {
        HistoryRecord oldUpstream =
                recorded("{\"ts_sec\": 1000, \"file\": \"mysql-bin.000010\", \"pos\": 100}");
        HistoryRecord newUpstream =
                recorded("{\"ts_sec\": 2000, \"file\": \"binlog.000002\", \"pos\": 200}");
        assertTrue(comparator.isAtOrBefore(oldUpstream, newUpstream));
        assertTrue(comparator.isAtOrBefore(newUpstream, oldUpstream));
    }

    @Test
    public void positionsAboveIntMaxAreCompared() throws IOException {
        HistoryRecord small =
                recorded("{\"ts_sec\": 1000, \"file\": \"mysql-bin.000003\", \"pos\": 157}");
        HistoryRecord large =
                recorded("{\"ts_sec\": 1000, \"file\": \"mysql-bin.000003\", \"pos\": 3000000000}");
        assertTrue(comparator.isAtOrBefore(small, large));
        assertFalse(comparator.isAtOrBefore(large, small));
    }

    private static HistoryRecord recorded(String position) throws IOException {
        return new HistoryRecord(
                DocumentReader.defaultReader()
                        .read(
                                "{\"source\": {\"server\": \"RW_CDC_1\"}, \"position\": "
                                        + position
                                        + "}"));
    }

    private static HistoryRecord restartPosition() {
        return new HistoryRecord(
                Map.of("server", "RW_CDC_1"),
                loader().load(COMMITTED_OFFSET).getOffset(),
                null,
                null,
                null,
                null,
                null);
    }

    private static MySqlOffsetContext.Loader loader() {
        return new MySqlOffsetContext.Loader(
                new MySqlConnectorConfig(
                        Configuration.create()
                                .with("topic.prefix", "test")
                                .with("database.hostname", "localhost")
                                .with("database.user", "test")
                                .with("database.server.id", 1)
                                .build()));
    }
}
