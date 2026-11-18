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
import oracle.ucp.jdbc.PoolDataSource;

/** Byte-array JNI facade for {@link OracleExternalTable}. */
public final class JniOracleExternalTable {
    private JniOracleExternalTable() {}

    public static byte[] discover(byte[] propertiesBytes) {
        return invoke(
                () ->
                        OracleExternalTable.discover(
                                        ConnectorServiceProto.OracleConnectionProperties.parseFrom(
                                                propertiesBytes))
                                .toByteArray());
    }

    public static Object openPool(byte[] propertiesBytes) {
        return invoke(
                () ->
                        OracleExternalTable.openPool(
                                ConnectorServiceProto.OracleConnectionProperties.parseFrom(
                                                propertiesBytes)
                                        .getPropertiesMap()));
    }

    public static void closePool(Object pool) {
        invoke(
                () -> {
                    OracleExternalTable.closePool((PoolDataSource) pool);
                    return null;
                });
    }

    public static long currentScn(Object pool, Object cancel) {
        return invoke(
                () ->
                        OracleExternalTable.currentScn(
                                (PoolDataSource) pool,
                                (OracleExternalTable.OracleStatementCancel) cancel));
    }

    public static Object newStatementCancel(String table, String query) {
        return new OracleExternalTable.OracleStatementCancel(table, query);
    }

    public static void cancelStatement(Object cancel) {
        ((OracleExternalTable.OracleStatementCancel) cancel).cancel();
    }

    public static void snapshotRead(
            Object pool, byte[] requestBytes, long startPkPointer, long channelPtr, Object cancel) {
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
                    OracleExternalTable.snapshotRead(
                            (PoolDataSource) pool,
                            request,
                            startPk,
                            channelPtr,
                            (OracleExternalTable.OracleStatementCancel) cancel);
                    return null;
                });
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
