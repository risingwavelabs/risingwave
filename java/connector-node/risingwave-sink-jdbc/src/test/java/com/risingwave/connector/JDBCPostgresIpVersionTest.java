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

package com.risingwave.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import org.junit.Test;

public class JDBCPostgresIpVersionTest {
    @Test
    public void testParseIpVersion() throws SQLException {
        assertEquals(JDBCPostgresIpVersion.ANY, JDBCPostgresIpVersion.fromUserValue(null));
        assertEquals(JDBCPostgresIpVersion.ANY, JDBCPostgresIpVersion.fromUserValue(""));
        assertEquals(JDBCPostgresIpVersion.ANY, JDBCPostgresIpVersion.fromUserValue("any"));
        assertEquals(JDBCPostgresIpVersion.IPV4, JDBCPostgresIpVersion.fromUserValue("ipv4"));
        assertEquals(JDBCPostgresIpVersion.IPV4, JDBCPostgresIpVersion.fromUserValue("4"));
        assertEquals(JDBCPostgresIpVersion.IPV6, JDBCPostgresIpVersion.fromUserValue("ipv6"));
        assertEquals(JDBCPostgresIpVersion.IPV6, JDBCPostgresIpVersion.fromUserValue("6"));
        assertThrows(SQLException.class, () -> JDBCPostgresIpVersion.fromUserValue("invalid"));
    }

    @Test
    public void testSocketFactoryRejectsMismatchedLiteralAddressBeforeConnect()
            throws SQLException {
        JDBCSocketFactory.configure(false, 60, 10, 3, JDBCPostgresIpVersion.fromUserValue("ipv6"));
        try {
            var socketFactory = new JDBCSocketFactory();
            var socket = socketFactory.createSocket();

            assertThrows(
                    IOException.class,
                    () -> socket.connect(InetSocketAddress.createUnresolved("127.0.0.1", 5432), 1));
        } finally {
            JDBCSocketFactory.clearConfig();
        }
    }
}
