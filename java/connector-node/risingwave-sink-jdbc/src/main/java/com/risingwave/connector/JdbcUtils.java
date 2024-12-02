// Copyright 2024 RisingWave Labs
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

import com.risingwave.connector.jdbc.JdbcDialectFactory;
import com.risingwave.connector.jdbc.MySqlDialectFactory;
import com.risingwave.connector.jdbc.PostgresDialectFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

public abstract class JdbcUtils {

    static final int CONNECTION_TIMEOUT = 30;
    static final int SOCKET_TIMEOUT = 300;

    public static Optional<JdbcDialectFactory> getDialectFactory(String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:mysql") || jdbcUrl.startsWith("jdbc:mariadb")) {
            return Optional.of(new MySqlDialectFactory());
        } else if (jdbcUrl.startsWith("jdbc:postgresql")) {
            return Optional.of(new PostgresDialectFactory());
        } else {
            return Optional.empty();
        }
    }

    /** The connection returned by this method is *not* autoCommit */
    public static Connection getConnection(String jdbcUrl) throws SQLException {
        var props = new Properties();
        // enable TCP keep alive to avoid connection closed by server
        // both MySQL and PG support this property
        // https://jdbc.postgresql.org/documentation/use/
        // https://dev.mysql.com/doc/connectors/en/connector-j-connp-props-networking.html#cj-conn-prop_tcpKeepAlive
        props.setProperty("tcpKeepAlive", "true");

        // default timeout in seconds
        boolean isPg = jdbcUrl.startsWith("jdbc:postgresql");

        // postgres use seconds and mysql use milliseconds
        int connectTimeout = isPg ? CONNECTION_TIMEOUT : CONNECTION_TIMEOUT * 1000;
        int socketTimeout = isPg ? SOCKET_TIMEOUT : SOCKET_TIMEOUT * 1000;
        props.setProperty("connectTimeout", String.valueOf(connectTimeout));
        props.setProperty("socketTimeout", String.valueOf(socketTimeout));

        var conn = DriverManager.getConnection(jdbcUrl, props);
        // disable auto commit can improve performance
        conn.setAutoCommit(false);
        // explicitly set isolation level to RC
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }
}
