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

import com.risingwave.java.binding.SingletonRow;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Byte-array JNI facade for {@link OracleExternalTable}. */
public final class JniOracleExternalTable {
    private static final AtomicLong NEXT_SESSION_ID = new AtomicLong(1);
    private static final Map<Long, OracleExternalTable.Session> SESSIONS =
            new ConcurrentHashMap<>();

    private JniOracleExternalTable() {}

    public static byte[] discover(byte[] propertiesBytes) {
        return invoke(
                () ->
                        OracleExternalTable.discover(
                                        ConnectorServiceProto.OracleConnectionProperties.parseFrom(
                                                propertiesBytes))
                                .toByteArray());
    }

    public static long openSession(byte[] propertiesBytes) {
        return invoke(
                () -> {
                    var properties =
                            ConnectorServiceProto.OracleConnectionProperties.parseFrom(
                                    propertiesBytes);
                    var sessionId = NEXT_SESSION_ID.getAndIncrement();
                    SESSIONS.put(
                            sessionId,
                            OracleExternalTable.openSession(properties.getPropertiesMap()));
                    return sessionId;
                });
    }

    public static long currentScn(long sessionId) {
        return invoke(() -> session(sessionId).currentScn());
    }

    public static void snapshotRead(
            long sessionId, byte[] requestBytes, long startPkPointer, long channelPtr) {
        invoke(
                () -> {
                    var request =
                            ConnectorServiceProto.OracleSnapshotReadRequest.parseFrom(requestBytes);
                    var tableSchema = request.getTableSchema();
                    var pkTypes =
                            tableSchema.getPkIndicesList().stream()
                                    .map(
                                            index ->
                                                    tableSchema
                                                            .getColumns(index)
                                                            .getColumnType()
                                                            .getTypeName())
                                    .toList();
                    var startPk =
                            startPkPointer == 0
                                    ? List.<Object>of()
                                    : new SingletonRow(startPkPointer).getValues(pkTypes);
                    session(sessionId).snapshotRead(request, startPk, channelPtr);
                    return null;
                });
    }

    public static void closeSession(long sessionId) {
        var session = SESSIONS.remove(sessionId);
        if (session != null) {
            session.close();
        }
    }

    private static OracleExternalTable.Session session(long sessionId) {
        var session = SESSIONS.get(sessionId);
        if (session == null) {
            throw new IllegalStateException("Oracle session " + sessionId + " is not open");
        }
        return session;
    }

    private static <T> T invoke(Operation<T> operation) {
        try {
            return operation.run();
        } catch (Exception error) {
            var message = error.getMessage();
            if (message == null || message.isBlank()) {
                message = error.getClass().getSimpleName();
            }
            throw new RuntimeException(message, error);
        }
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run() throws Exception;
    }
}
