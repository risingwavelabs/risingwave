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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

public class PostgresIpVersionTest {
    @Test
    public void testParseIpVersion() {
        assertEquals(PostgresIpVersion.ANY, PostgresIpVersion.fromUserValue(null));
        assertEquals(PostgresIpVersion.ANY, PostgresIpVersion.fromUserValue(""));
        assertEquals(PostgresIpVersion.ANY, PostgresIpVersion.fromUserValue("any"));
        assertEquals(PostgresIpVersion.IPV4, PostgresIpVersion.fromUserValue("ipv4"));
        assertEquals(PostgresIpVersion.IPV4, PostgresIpVersion.fromUserValue("4"));
        assertEquals(PostgresIpVersion.IPV6, PostgresIpVersion.fromUserValue("ipv6"));
        assertEquals(PostgresIpVersion.IPV6, PostgresIpVersion.fromUserValue("6"));
        assertThrows(
                StatusRuntimeException.class, () -> PostgresIpVersion.fromUserValue("invalid"));
    }

    @Test
    public void testResolveLiteralAddress() {
        assertEquals("127.0.0.1", PostgresIpVersion.IPV4.resolveHostForJdbcUrl("127.0.0.1"));
        assertEquals("[0:0:0:0:0:0:0:1]", PostgresIpVersion.IPV6.resolveHostForJdbcUrl("::1"));
        assertThrows(
                StatusRuntimeException.class,
                () -> PostgresIpVersion.IPV6.resolveHostForJdbcUrl("127.0.0.1"));
    }

    @Test
    public void testApplyToDebeziumPostgresPropertiesKeepsHostnameDynamic() {
        var postgresProps = new Properties();
        PostgresIpVersion.applyToDebeziumPostgresProperties(
                postgresProps,
                Map.of(
                        DbzConnectorConfig.HOST,
                        "127.0.0.1",
                        PostgresIpVersion.PROPERTY_NAME,
                        "ipv4"));

        assertNull(postgresProps.getProperty("database.hostname"));
        assertEquals(
                PostgresIpVersionSocketFactory.class.getName(),
                postgresProps.getProperty("database.socketFactory"));
        assertEquals("ipv4", postgresProps.getProperty("database.socketFactoryArg"));
    }

    @Test
    public void testSocketFactoryRejectsMismatchedLiteralAddressBeforeConnect() throws IOException {
        var socketFactory = new PostgresIpVersionSocketFactory("ipv6");
        var socket = socketFactory.createSocket();

        assertThrows(
                IOException.class,
                () -> socket.connect(InetSocketAddress.createUnresolved("127.0.0.1", 5432), 1));
    }
}
