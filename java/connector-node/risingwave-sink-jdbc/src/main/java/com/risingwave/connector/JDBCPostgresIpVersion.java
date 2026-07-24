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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

enum JDBCPostgresIpVersion {
    ANY("any"),
    IPV4("ipv4"),
    IPV6("ipv6");

    private final String displayName;

    JDBCPostgresIpVersion(String displayName) {
        this.displayName = displayName;
    }

    static JDBCPostgresIpVersion fromUserValue(String value) throws SQLException {
        if (value == null || value.isBlank()) {
            return ANY;
        }

        return switch (value.toLowerCase(Locale.ROOT)) {
            case "any" -> ANY;
            case "4", "v4", "ipv4" -> IPV4;
            case "6", "v6", "ipv6" -> IPV6;
            default ->
                    throw new SQLException(
                            String.format("invalid postgres ip.version `%s`", value));
        };
    }

    boolean matches(InetAddress address) {
        return switch (this) {
            case ANY -> true;
            case IPV4 -> address instanceof Inet4Address;
            case IPV6 -> address instanceof Inet6Address;
        };
    }

    List<InetAddress> resolveAddresses(String host) throws SQLException {
        var addresses = new LinkedHashSet<InetAddress>();
        try {
            Arrays.stream(InetAddress.getAllByName(host))
                    .filter(this::matches)
                    .forEach(addresses::add);
        } catch (UnknownHostException e) {
            throw new SQLException(
                    String.format(
                            "failed to resolve postgres host `%s` for %s: %s",
                            host, displayName, e.getMessage()),
                    e);
        }

        if (addresses.isEmpty()) {
            throw new SQLException(
                    String.format(
                            "postgres host `%s` did not resolve to any %s address",
                            host, displayName));
        }

        return List.copyOf(addresses);
    }

    @Override
    public String toString() {
        return displayName;
    }
}
