// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriterBase;
import com.risingwave.proto.Data;
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
    private final PreparedStatement insertStmt;
    private final PreparedStatement updateStmt;
    private final PreparedStatement deleteStmt;
    private final List<String> nonKeyColumns;
    private final BatchStatementBuilder batchBuilder;
    private final CassandraConfig config;
    private final List<Integer> primaryKeyIndexes;

    public CassandraSink(TableSchema tableSchema, CassandraConfig config) {
        super(tableSchema);
        String url = config.getUrl();
        String[] hostPort = url.split(":");
        if (hostPort.length != 2) {
            throw new IllegalArgumentException(
                    "Invalid cassandraURL: expected `host:port`, got " + url);
        }
        // 2. check connection
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

        primaryKeyIndexes = new ArrayList<Integer>();
        for (String primaryKey : tableSchema.getPrimaryKeys()) {
            primaryKeyIndexes.add(tableSchema.getColumnIndex(primaryKey));
        }

        this.batchBuilder =
                BatchStatement.builder(DefaultBatchType.LOGGED).setKeyspace(config.getKeyspace());

        // fetch non-pk columns for prepared statements
        nonKeyColumns =
                Arrays.stream(tableSchema.getColumnNames())
                        // cassandra does not allow SET on primary keys
                        .filter(c -> !tableSchema.getPrimaryKeys().contains(c))
                        .collect(Collectors.toList());

        // prepare statement for insert
        this.insertStmt = session.prepare(createInsertStatement(config.getTable(), tableSchema));

        // prepare statement for update-insert/update-delete
        this.updateStmt = session.prepare(createUpdateStatement(config.getTable(), tableSchema));

        // prepare the delete statement
        this.deleteStmt = session.prepare(createDeleteStatement(config.getTable(), tableSchema));
    }

    @Override
    public void write(Iterator<SinkRow> rows) {
        if (this.config.getType() == "append-only") {
            write_apend_only(rows);
        } else {
            write_upsert(rows);
        }
    }

    private void write_apend_only(Iterator<SinkRow> rows) {
        while (rows.hasNext()) {
            SinkRow row = rows.next();
            Data.Op op = row.getOp();
            switch (op) {
                case INSERT:
                    batchBuilder.addStatement(bindInsertStatement(insertStmt, row));
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
                    batchBuilder.addStatement(bindInsertStatement(insertStmt, row));
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
                    batchBuilder.addStatement(bindUpdateInsertStatement(updateStmt, old, row));
                    break;
                case DELETE:
                    batchBuilder.addStatement(bindDeleteStatement(deleteStmt, row));
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
        return stmt.bind(IntStream.range(0, row.size()).mapToObj(row::get).toArray());
    }

    private BoundStatement bindDeleteStatement(PreparedStatement stmt, SinkRow row) {
        return stmt.bind(
                getTableSchema().getPrimaryKeys().stream()
                        .map(key -> getTableSchema().getFromRow(key, row))
                        .toArray());
    }

    private BoundStatement bindUpdateInsertStatement(
            PreparedStatement stmt, SinkRow updateRow, SinkRow insertRow) {
        TableSchema schema = getTableSchema();
        int numKeys = schema.getPrimaryKeys().size();
        int numNonKeys = updateRow.size() - numKeys;
        Object[] values = new Object[numNonKeys + numKeys];

        // bind "SET" clause
        Iterator<String> nonKeyIter = nonKeyColumns.iterator();
        for (int i = 0; i < numNonKeys; i++) {
            values[i] = schema.getFromRow(nonKeyIter.next(), insertRow);
        }

        // bind "WHERE" clause
        Iterator<String> keyIter = schema.getPrimaryKeys().iterator();
        for (int i = 0; i < numKeys; i++) {
            values[numNonKeys + i] = schema.getFromRow(keyIter.next(), updateRow);
        }
        return stmt.bind(values);
    }
}
