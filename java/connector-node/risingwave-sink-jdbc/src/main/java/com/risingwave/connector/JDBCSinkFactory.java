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

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import io.grpc.Status;
import java.sql.*;
import java.util.Map;

public class JDBCSinkFactory implements SinkFactory {
    public static final String JDBC_URL_PROP = "jdbc.url";
    public static final String TABLE_NAME_PROP = "table.name";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        // TODO: Remove this call to `validate` after supporting sink validation in risingwave.
        validate(tableSchema, tableProperties);

        String tableName = tableProperties.get(TABLE_NAME_PROP);
        String jdbcUrl = tableProperties.get(JDBC_URL_PROP);
        return new JDBCSink(tableName, jdbcUrl, tableSchema);
    }

    @Override
    public void validate(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(JDBC_URL_PROP)
                || !tableProperties.containsKey(TABLE_NAME_PROP)) {
            throw Status.INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s or %s is not specified", JDBC_URL_PROP, TABLE_NAME_PROP))
                    .asRuntimeException();
        }

        String jdbcUrl = tableProperties.get(JDBC_URL_PROP);

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);
            conn.close();
        } catch (SQLException e) {
            throw Status.INTERNAL.withCause(e).asRuntimeException();
        }
    }
}
