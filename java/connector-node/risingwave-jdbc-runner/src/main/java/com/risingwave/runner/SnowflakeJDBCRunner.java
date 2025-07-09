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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SnowflakeJDBCRunner {
    private static final Logger logger = LogManager.getLogger(SnowflakeJDBCRunner.class);

    public static void executeSql(String fullUrl, String sql) {
        Connection connection = null;
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            connection = DriverManager.getConnection(fullUrl);
            Statement stmt = connection.createStatement();
            int result = stmt.executeUpdate(sql);
            logger.info("[JDBCRunner] Executing SQL");
            stmt.close();
        } catch (Exception e) {
            logger.error("[JDBCRunner] SQLException: {}", e.getMessage(), e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                    logger.info("[JDBCRunner] Connection closed.");
                } catch (SQLException e) {
                    logger.error("[JDBCRunner] SQLException on close: {}", e.getMessage(), e);
                }
            }
        }
    }
}
