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

package com.risingwave.sourcenode;

import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data.DataType;
import com.risingwave.sourcenode.common.DbzConnectorConfig;
import com.risingwave.sourcenode.core.SourceHandlerFactory;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceRequestHandler {
    private final StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver;
    static final Logger LOG = LoggerFactory.getLogger(SourceRequestHandler.class);

    public SourceRequestHandler(
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    public void handle(ConnectorServiceProto.GetEventStreamRequest request) {
        switch (request.getRequestCase()) {
            case VALIDATE:
                // try to start a JDBC connection to external database
                validateDbProperties(request.getValidate(), responseObserver);
                break;
            case START:
                var startRequest = request.getStart();
                try {
                    var handler =
                            SourceHandlerFactory.createSourceHandler(
                                    SourceTypeE.valueOf(startRequest.getSourceType()),
                                    startRequest.getSourceId(),
                                    startRequest.getStartOffset(),
                                    startRequest.getPropertiesMap());
                    handler.startSource(
                            (ServerCallStreamObserver<ConnectorServiceProto.GetEventStreamResponse>)
                                    responseObserver);
                } catch (Throwable t) {
                    LOG.error("failed to start source", t);
                    responseObserver.onError(t);
                }
                break;
            case REQUEST_NOT_SET:
                LOG.warn("request not set");
                responseObserver.onCompleted();
                break;
        }
    }

    private void validateDbProperties(
            ConnectorServiceProto.GetEventStreamRequest.ValidateProperties validate,
            StreamObserver<ConnectorServiceProto.GetEventStreamResponse> responseObserver) {
        var props = validate.getPropertiesMap();
        String jdbcUrl =
                toJdbcPrefix(validate.getSourceType())
                        + "://"
                        + props.get(DbzConnectorConfig.HOST)
                        + ":"
                        + props.get(DbzConnectorConfig.PORT)
                        + "/"
                        + props.get(DbzConnectorConfig.DB_NAME);
        LOG.debug("validate jdbc url: {}", jdbcUrl);

        var sqlStmts = new Properties();
        try (var input =
                getClass().getClassLoader().getResourceAsStream("validate_sql.properties")) {
            sqlStmts.load(input);
        } catch (IOException e) {
            LOG.error("failed to load sql statements", e);
            throw new RuntimeException(e);
        }
        try (var conn =
                DriverManager.getConnection(
                        jdbcUrl,
                        props.get(DbzConnectorConfig.USER),
                        props.get(DbzConnectorConfig.PASSWORD))) {
            // usernamed and password are correct
            var dbMeta = conn.getMetaData();

            LOG.debug("source schema: {}", validate.getTableSchema().getColumnsList());
            LOG.debug("source pk: {}", validate.getTableSchema().getPkIndicesList());

            // validate schema name and table name
            switch (validate.getSourceType()) {
                case MYSQL:
                    // check whether source db has enabled binlog
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_log"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ON")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL doesn't enable binlog.\nPlease set the value of log_bin to 'ON' and restart your MySQL server."));
                            }
                        }
                    }
                    // check binlog format
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_format"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("ROW")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_format should be 'ROW'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("mysql.bin_row_image"));
                        while (res.next()) {
                            if (!res.getString(2).equalsIgnoreCase("FULL")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "MySQL binlog_row_image should be 'FULL'.\nPlease modify the config and restart your MySQL server."));
                            }
                        }
                    }
                    // check whether table exist
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("mysql.table"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            var ret = res.getInt(1);
                            if (ret == 0) {
                                throw new RuntimeException("MySQL table doesn't exist");
                            }
                        }
                    }
                    // check whether PK constraint match source table definition
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("mysql.table_schema"))) {
                        var sourceSchema = validate.getTableSchema();
                        stmt.setString(1, props.get(DbzConnectorConfig.DB_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        var pkFields = new HashSet<String>();
                        int index = 0;
                        while (res.next()) {
                            var field = res.getString(1);
                            var dataType = res.getString(2);
                            var key = res.getString(3);

                            if (index >= sourceSchema.getColumnsCount()) {
                                throw new RuntimeException(("The number of columns mismatch"));
                            }

                            var srcCol = sourceSchema.getColumns(index++);
                            if (!srcCol.getName().equals(field)) {
                                throw new RuntimeException(
                                        String.format(
                                                "column name mismatch: %s, [%s]",
                                                field, srcCol.getName()));
                            }
                            if (!isMySQLDataTypeCompatible(dataType, srcCol.getDataType())) {
                                throw new RuntimeException(
                                        String.format(
                                                "incompatible data type of column %s",
                                                srcCol.getName()));
                            }
                            if (key.equalsIgnoreCase("PRI")) {
                                pkFields.add(field);
                            }
                        }

                        if (!isPkMatch(sourceSchema, pkFields)) {
                            throw new RuntimeException("Primary key mismatch");
                        }
                    }
                    break;
                case POSTGRES:
                    // check whether source db has enabled wal
                    try (var stmt = conn.createStatement()) {
                        var res = stmt.executeQuery(sqlStmts.getProperty("postgres.wal"));
                        while (res.next()) {
                            if (!res.getString(1).equals("logical")) {
                                throw new StatusException(
                                        Status.INTERNAL.withDescription(
                                                "Postgres wal_level should be 'logical'.\nPlease modify the config and restart your Postgres server."));
                            }
                        }
                    }
                    // check schema name and table name
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.table"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        while (res.next()) {
                            var ret = res.getString(1);
                            if (ret.equalsIgnoreCase("f") || ret.equalsIgnoreCase("false")) {
                                throw new RuntimeException(
                                        "Postgres table or schema doesn't exist");
                            }
                        }
                    }
                    // check primary key
                    // reference: https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
                    try (var stmt = conn.prepareStatement(sqlStmts.getProperty("postgres.pk"))) {
                        stmt.setString(
                                1,
                                props.get(DbzConnectorConfig.PG_SCHEMA_NAME)
                                        + "."
                                        + props.get(DbzConnectorConfig.TABLE_NAME));

                        var res = stmt.executeQuery();
                        var pkFields = new HashSet<String>();
                        while (res.next()) {
                            var name = res.getString(1);
                            pkFields.add(name);
                        }

                        if (!isPkMatch(validate.getTableSchema(), pkFields)) {
                            throw new RuntimeException("Primary key mismatch");
                        }
                    }
                    // check whether source schema match table schema on upstream
                    try (var stmt =
                            conn.prepareStatement(sqlStmts.getProperty("postgres.table_schema"))) {
                        stmt.setString(1, props.get(DbzConnectorConfig.PG_SCHEMA_NAME));
                        stmt.setString(2, props.get(DbzConnectorConfig.TABLE_NAME));
                        var res = stmt.executeQuery();
                        var sourceSchema = validate.getTableSchema();
                        int index = 0;
                        while (res.next()) {
                            var field = res.getString(1);
                            var dataType = res.getString(2);
                            if (index >= sourceSchema.getColumnsCount()) {
                                throw new RuntimeException("The number of columns mismatch");
                            }
                            var srcCol = sourceSchema.getColumns(index++);
                            if (!srcCol.getName().equals(field)) {
                                throw new RuntimeException(
                                        String.format(
                                                "table column defined in the source mismatches upstream column %s",
                                                field));
                            }
                            if (!isPostgresDataTypeCompatible(dataType, srcCol.getDataType())) {
                                throw new RuntimeException(
                                        String.format(
                                                "incompatible data type of column %s",
                                                srcCol.getName()));
                            }
                        }
                    }
                    break;
                default:
                    break;
            }

            LOG.info(
                    "validate properties success. product: {}, version: {}",
                    dbMeta.getDatabaseProductName(),
                    dbMeta.getDatabaseProductVersion());
            responseObserver.onCompleted();

        } catch (Exception e) {
            LOG.error("invalid connector properties:", e);
            responseObserver.onError(
                    new StatusException(Status.INVALID_ARGUMENT.withDescription(e.getMessage())));
        }
    }

    private boolean isPkMatch(
            ConnectorServiceProto.TableSchema sourceSchema, Set<String> pkFields) {
        if (sourceSchema.getPkIndicesCount() != pkFields.size()) {
            return false;
        }
        for (var index : sourceSchema.getPkIndicesList()) {
            if (!pkFields.contains(sourceSchema.getColumns(index).getName())) {
                return false;
            }
        }
        return true;
    }

    private boolean isMySQLDataTypeCompatible(String mysqlDataType, DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (mysqlDataType) {
            case "tinyint": // boolean
                return (val == DataType.TypeName.BOOLEAN_VALUE)
                        || (DataType.TypeName.INT16_VALUE <= val
                                && val <= DataType.TypeName.INT64_VALUE);
            case "smallint":
                return DataType.TypeName.INT16_VALUE <= val && val <= DataType.TypeName.INT64_VALUE;
            case "mediumint":
            case "int":
                return DataType.TypeName.INT32_VALUE <= val && val <= DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == DataType.TypeName.INT64_VALUE;

            case "float":
            case "real":
                return val == DataType.TypeName.FLOAT_VALUE
                        || val == DataType.TypeName.DOUBLE_VALUE;
            case "double":
                return val == DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
                return val == DataType.TypeName.DECIMAL_VALUE;
            case "varchar":
                return val == DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
    }

    private boolean isPostgresDataTypeCompatible(String pgDataType, DataType.TypeName typeName) {
        int val = typeName.getNumber();
        switch (pgDataType) {
            case "smallint":
                return DataType.TypeName.INT16_VALUE <= val && val <= DataType.TypeName.INT64_VALUE;
            case "integer":
                return DataType.TypeName.INT32_VALUE <= val && val <= DataType.TypeName.INT64_VALUE;
            case "bigint":
                return val == DataType.TypeName.INT64_VALUE;
            case "float":
            case "real":
                return val == DataType.TypeName.FLOAT_VALUE
                        || val == DataType.TypeName.DOUBLE_VALUE;
            case "boolean":
                return val == DataType.TypeName.BOOLEAN_VALUE;
            case "double":
            case "double precision":
                return val == DataType.TypeName.DOUBLE_VALUE;
            case "decimal":
            case "numeric":
                return val == DataType.TypeName.DECIMAL_VALUE;
            case "varchar":
            case "character varying":
                return val == DataType.TypeName.VARCHAR_VALUE;
            default:
                return true; // true for other uncovered types
        }
    }

    private String toJdbcPrefix(ConnectorServiceProto.SourceType sourceType) {
        switch (sourceType) {
            case MYSQL:
                return "jdbc:mysql";
            case POSTGRES:
                return "jdbc:postgresql";
            default:
                return "";
        }
    }
}
