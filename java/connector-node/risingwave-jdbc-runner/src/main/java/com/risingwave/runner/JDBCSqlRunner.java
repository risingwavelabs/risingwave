/*
 * Copyright 2025 RisingWave Labs
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

package com.risingwave.runner;

import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSqlRunner {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSqlRunner.class);

    public static void executeSql(String fullUrl, String[] sqls) throws Exception {
        Connection connection = null;
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            connection = DriverManager.getConnection(fullUrl);
            connection.setAutoCommit(false);
            LOG.info("[JDBCRunner] Transaction started, auto-commit disabled");
            Statement stmt = connection.createStatement();
            for (int i = 0; i < sqls.length; i++) {
                String sql = sqls[i];
                int result = stmt.executeUpdate(sql);
            }
            connection.commit();
            LOG.info("[JDBCRunner] Transaction committed successfully, SQL statements executed");
            stmt.close();
        } catch (Exception e) {
            LOG.error("[JDBCRunner] Exception occurred: {}", e.getMessage(), e);
            if (connection != null) {
                try {
                    connection.rollback();
                    LOG.info("[JDBCRunner] Transaction rolled back due to error");
                } catch (SQLException rollbackEx) {
                    LOG.error(
                            "[JDBCRunner] Failed to rollback transaction: {}",
                            rollbackEx.getMessage(),
                            rollbackEx);
                }
            }
            throw e;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                    LOG.info("[JDBCRunner] Connection closed.");
                } catch (SQLException e) {
                    LOG.error("[JDBCRunner] SQLException on close: {}", e.getMessage(), e);
                }
            }
        }
    }
}
