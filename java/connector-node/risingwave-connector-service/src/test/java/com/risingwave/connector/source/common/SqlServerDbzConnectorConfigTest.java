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

import static org.junit.Assert.assertEquals;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import com.risingwave.proto.ConnectorServiceProto.SourceType;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SqlServerDbzConnectorConfigTest {
    @Parameterized.Parameters(name = "databaseName={0}")
    public static String[] databaseNames() {
        return new String[] {
            "sales",
            "sales;east",
            "sales;applicationName=changed",
            "sales;databaseName=other",
            "sales;encrypt=true;trustServerCertificate=false",
            "sales{east",
            "sales}east",
            "{sales}",
            "sales}};applicationName=changed",
            " sales",
            "sales ",
            " sales;{east} "
        };
    }

    @Parameterized.Parameter public String databaseName;

    @Test
    public void preservesDatabaseNameWithoutChangingConnectionProperties() throws SQLException {
        for (var encrypt : new String[] {"false", "true"}) {
            var expected = parseJdbcUrl("sales", encrypt);
            assertEquals(encrypt, expected.get("encrypt"));
            assertEquals("true", expected.get("trustServerCertificate"));
            expected.put("databaseName", databaseName);

            assertEquals(expected, parseJdbcUrl(databaseName, encrypt));
        }
    }

    private static Map<String, String> parseJdbcUrl(String databaseName, String encrypt)
            throws SQLException {
        var config =
                new SqlServerDbzConnectorConfig(
                        GetEventStreamRequest.newBuilder()
                                .setSourceType(SourceType.SQL_SERVER)
                                .putProperties("hostname", "example.invalid")
                                .putProperties("port", "1433")
                                .putProperties("database.name", databaseName)
                                .putProperties("database.encrypt", encrypt)
                                .build());
        var properties = new HashMap<String, String>();
        // Parse with the real driver without opening a connection to SQL Server.
        for (var property :
                new SQLServerDriver().getPropertyInfo(config.getJdbcUrl(), new Properties())) {
            properties.put(property.name, property.value);
        }
        return properties;
    }
}
