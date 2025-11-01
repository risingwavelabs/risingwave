// Copyright 2025 RisingWave Labs
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
import com.risingwave.connector.jdbc.RedShiftDialectFactory;
import com.risingwave.connector.jdbc.SnowflakeDialectFactory;
import com.risingwave.connector.jdbc.SqlServerDialectFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JdbcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcUtils.class);

    static final int CONNECTION_TIMEOUT = 30;
    static final int SOCKET_TIMEOUT = 300;

    public static Optional<JdbcDialectFactory> getDialectFactory(String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:mysql") || jdbcUrl.startsWith("jdbc:mariadb")) {
            return Optional.of(new MySqlDialectFactory());
        } else if (jdbcUrl.startsWith("jdbc:postgresql")) {
            return Optional.of(new PostgresDialectFactory());
        } else if (jdbcUrl.startsWith("jdbc:redshift")) {
            return Optional.of(new RedShiftDialectFactory());
        } else if (jdbcUrl.startsWith("jdbc:snowflake")) {
            return Optional.of(new SnowflakeDialectFactory());
        } else if (jdbcUrl.startsWith("jdbc:sqlserver")) {
            return Optional.of(new SqlServerDialectFactory());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Creates base JDBC connection properties with common settings. This is a helper method that
     * can be used by default and specialized connection logic.
     *
     * @param jdbcUrl JDBC URL to determine database-specific settings
     * @param user Username for authentication
     * @return Properties object with base connection settings
     */
    static Properties createBaseProperties(String jdbcUrl, String user) {
        var props = new Properties();

        // enable TCP keep alive to avoid connection closed by server
        props.setProperty("tcpKeepAlive", "true");

        // default timeout in seconds
        boolean isPg = jdbcUrl.startsWith("jdbc:postgresql");

        // postgres use seconds and mysql use milliseconds
        int connectTimeout = isPg ? CONNECTION_TIMEOUT : CONNECTION_TIMEOUT * 1000;
        int socketTimeout = isPg ? SOCKET_TIMEOUT : SOCKET_TIMEOUT * 1000;
        props.setProperty("connectTimeout", String.valueOf(connectTimeout));
        props.setProperty("socketTimeout", String.valueOf(socketTimeout));

        if (user != null) {
            props.put("user", user);
        }

        return props;
    }

    /**
     * Creates a JDBC connection for the default configuration (password authentication). The
     * connection returned by this method is *not* autoCommit by default.
     *
     * @param config JDBC sink configuration
     * @return JDBC connection
     * @throws SQLException if connection fails
     */
    static Connection getConnectionDefault(JDBCSinkConfig config) throws SQLException {
        String jdbcUrl = config.getJdbcUrl();
        var props = createBaseProperties(jdbcUrl, config.getUser());

        // Default password authentication
        if (config.getPassword() != null) {
            props.put("password", config.getPassword());
        }

        if (jdbcUrl.startsWith("jdbc:redshift") && config.getBatchInsertRows() > 0) {
            props.setProperty("reWriteBatchedInserts", "true");
            props.setProperty(
                    "reWriteBatchedInsertsSize", String.valueOf(config.getBatchInsertRows()));
        }

        var conn = DriverManager.getConnection(jdbcUrl, props);
        // disable auto commit can improve performance
        conn.setAutoCommit(config.isAutoCommit());
        // explicitly set isolation level to RC
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }
}
