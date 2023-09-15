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

package com.risingwave.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.proto.Data;
import com.risingwave.proto.Data.DataType.TypeName;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSink extends SinkWriterBase {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);
    private final CqlSession session;
    private final List<SinkRow> updateRowCache = new ArrayList<>(1);
    private final HashMap<String, PreparedStatement> stmtMap;
    private final List<String> nonKeyColumns;
    private final BatchStatementBuilder batchBuilder;
    private final CassandraConfig config;

    public CassandraSink(TableSchema tableSchema, CassandraConfig config) {
        super(tableSchema);
        String url = config.getUrl();
        String[] hostPort = url.split(":");
        if (hostPort.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid cassandraURL: expected `host:port`, got " + url);
        }
        // check connection
        CqlSessionBuilder sessionBuilder =
                CqlSession.builder()
                        .addContactPoint(
                                new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])))
                        .withKeyspace(config.getKeyspace())
                        .withLocalDatacenter(config.getDatacenter());
        if (config.getUsername() != null && config.getPassword() != null) {
            sessionBuilder =
                    sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
        }
        this.session = sessionBuilder.build();
        if (session.isClosed()) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("Cannot connect to " + config.getUrl())
                    .asRuntimeException();
        }

        this.config = config;
        this.batchBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);

        // fetch non-pk columns for prepared statements
        nonKeyColumns =
                Arrays.stream(tableSchema.getColumnNames())
                        // cassandra does not allow SET on primary keys
                        .filter(c -> !tableSchema.getPrimaryKeys().contains(c))
                        .collect(Collectors.toList());

        this.stmtMap = new HashMap<>();
        // prepare statement for insert
        this.stmtMap.put(
                "insert", session.prepare(createInsertStatement(config.getTable(), tableSchema)));
        if (config.getType().equals("upsert")) {
            // prepare statement for update-insert/update-delete
            this.stmtMap.put(
                    "update",
                    session.prepare(createUpdateStatement(config.getTable(), tableSchema)));

            // prepare the delete statement
            this.stmtMap.put(
                    "delete",
                    session.prepare(createDeleteStatement(config.getTable(), tableSchema)));
        }
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        if (this.config.getType().equals("append-only")) {
            write_append_only(rows);
        } else {
            write_upsert(rows);
        }
    }

    private void write_append_only(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            Data.Op op = row.getOp();
            switch (op) {
                case INSERT:
                    batchBuilder.addStatement(bindInsertStatement(this.stmtMap.get("insert"), row));
                    break;
                case UPDATE_DELETE:
                    break;
                case UPDATE_INSERT:
                    break;
                case DELETE:
                    break;
                default:
                    throw Status.INTERNAL
                            .withDescription("Unknown operation: " + op)
                            .asRuntimeException();
            }
        }
    }

    private void write_upsert(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            Data.Op op = row.getOp();
            switch (op) {
                case INSERT:
                    batchBuilder.addStatement(bindInsertStatement(this.stmtMap.get("insert"), row));
                    break;
                case UPDATE_DELETE:
                    updateRowCache.clear();
                    updateRowCache.add(row);
                    break;
                case UPDATE_INSERT:
                    SinkRow old = updateRowCache.remove(0);
                    if (old == null) {
                        throw Status.FAILED_PRECONDITION
                                .withDescription("UPDATE_INSERT without UPDATE_DELETE")
                                .asRuntimeException();
                    }
                    batchBuilder.addStatement(
                            bindUpdateInsertStatement(this.stmtMap.get("update"), old, row));
                    break;
                case DELETE:
                    batchBuilder.addStatement(bindDeleteStatement(this.stmtMap.get("delete"), row));
                    break;
                default:
                    throw Status.INTERNAL
                            .withDescription("Unknown operation: " + op)
                            .asRuntimeException();
            }
        }
    }

    @Override
    public void sync() {
        try {
            session.execute(batchBuilder.build());
            batchBuilder.clearStatements();
        } catch (Exception e) {
            throw Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException();
        }
    }

    @Override
    public void drop() {
        session.close();
    }

    private String createInsertStatement(String tableName, TableSchema tableSchema) {
        String[] columnNames = tableSchema.getColumnNames();
        String columnNamesString = String.join(", ", columnNames);
        String placeholdersString = String.join(", ", Collections.nCopies(columnNames.length, "?"));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName, columnNamesString, placeholdersString);
    }

    private String createUpdateStatement(String tableName, TableSchema tableSchema) {
        List<String> primaryKeys = tableSchema.getPrimaryKeys();
        String setClause = // cassandra does not allow SET on primary keys
                nonKeyColumns.stream()
                        .map(columnName -> columnName + " = ?")
                        .collect(Collectors.joining(", "));
        String whereClause =
                primaryKeys.stream()
                        .map(columnName -> columnName + " = ?")
                        .collect(Collectors.joining(" AND "));
        return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
    }

    private static String createDeleteStatement(String tableName, TableSchema tableSchema) {
        List<String> primaryKeys = tableSchema.getPrimaryKeys();
        String whereClause =
                primaryKeys.stream()
                        .map(columnName -> columnName + " = ?")
                        .collect(Collectors.joining(" AND "));
        return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
    }

    private BoundStatement bindInsertStatement(PreparedStatement stmt, SinkRow row) {
        TableSchema schema = getTableSchema();
        return stmt.bind(
                IntStream.range(0, row.size())
                        .mapToObj(
                                (index) ->
                                        CassandraUtil.convertRow(
                                                row.get(index),
                                                schema.getColumnDescs()
                                                        .get(index)
                                                        .getDataType()
                                                        .getTypeName()))
                        .toArray());
    }

    private BoundStatement bindDeleteStatement(PreparedStatement stmt, SinkRow row) {
        TableSchema schema = getTableSchema();
        Map<String, TypeName> columnDescs = schema.getColumnTypes();
        return stmt.bind(
                getTableSchema().getPrimaryKeys().stream()
                        .map(
                                key ->
                                        CassandraUtil.convertRow(
                                                schema.getFromRow(key, row), columnDescs.get(key)))
                        .toArray());
    }

    private BoundStatement bindUpdateInsertStatement(
            PreparedStatement stmt, SinkRow updateRow, SinkRow insertRow) {
        TableSchema schema = getTableSchema();
        int numKeys = schema.getPrimaryKeys().size();
        int numNonKeys = updateRow.size() - numKeys;
        Map<String, TypeName> columnDescs = schema.getColumnTypes();
        Object[] values = new Object[numNonKeys + numKeys];

        // bind "SET" clause
        Iterator<String> nonKeyIter = nonKeyColumns.iterator();
        for (int i = 0; i < numNonKeys; i++) {
            String name = nonKeyIter.next();
            values[i] =
                    CassandraUtil.convertRow(
                            schema.getFromRow(name, insertRow), columnDescs.get(name));
        }

        // bind "WHERE" clause
        Iterator<String> keyIter = schema.getPrimaryKeys().iterator();
        for (int i = 0; i < numKeys; i++) {
            String name = keyIter.next();
            values[numNonKeys + i] =
                    CassandraUtil.convertRow(
                            schema.getFromRow(name, updateRow), columnDescs.get(name));
        }
        return stmt.bind(values);
    }
}
