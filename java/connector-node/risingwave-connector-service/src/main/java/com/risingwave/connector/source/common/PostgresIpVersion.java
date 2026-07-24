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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

enum PostgresIpVersion {
    ANY("any"),
    IPV4("ipv4"),
    IPV6("ipv6");

    static final String PROPERTY_NAME = "ip.version";

    private static final String DBZ_SOCKET_FACTORY_PROPERTY = "database.socketFactory";
    private static final String DBZ_SOCKET_FACTORY_ARG_PROPERTY = "database.socketFactoryArg";

    private final String displayName;

    PostgresIpVersion(String displayName) {
        this.displayName = displayName;
    }

    static PostgresIpVersion fromProperties(Map<String, String> props) {
        return fromUserValue(props.get(PROPERTY_NAME));
    }

    static PostgresIpVersion fromUserValue(String value) {
        if (value == null || value.isBlank()) {
            return ANY;
        }

        return switch (value.toLowerCase(Locale.ROOT)) {
            case "any" -> ANY;
            case "4", "v4", "ipv4" -> IPV4;
            case "6", "v6", "ipv6" -> IPV6;
            default ->
                    throw ValidatorUtils.invalidArgument(
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

    String resolveHostForJdbcUrl(String host) {
        if (this == ANY) {
            return host;
        }

        return toJdbcUrlHost(resolveAddresses(host).getFirst());
    }

    static String resolveHostForJdbcUrl(Map<String, String> props) {
        var host = props.get(DbzConnectorConfig.HOST);
        return fromProperties(props).resolveHostForJdbcUrl(host);
    }

    static void applyToDebeziumPostgresProperties(
            Properties postgresProps, Map<String, String> userProps) {
        var ipVersion = fromProperties(userProps);
        if (ipVersion == ANY) {
            return;
        }

        postgresProps.setProperty(
                DBZ_SOCKET_FACTORY_PROPERTY, PostgresIpVersionSocketFactory.class.getName());
        postgresProps.setProperty(DBZ_SOCKET_FACTORY_ARG_PROPERTY, ipVersion.toString());
    }

    List<InetAddress> resolveAddresses(String host) {
        var addresses = new LinkedHashSet<InetAddress>();
        try {
            Arrays.stream(InetAddress.getAllByName(host))
                    .filter(this::matches)
                    .forEach(addresses::add);
        } catch (UnknownHostException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "failed to resolve postgres host `%s` for %s: %s",
                            host, displayName, e.getMessage()));
        }

        if (addresses.isEmpty()) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "postgres host `%s` did not resolve to any %s address",
                            host, displayName));
        }

        return List.copyOf(addresses);
    }

    private static String toJdbcUrlHost(InetAddress address) {
        var host = address.getHostAddress();
        if (address instanceof Inet6Address) {
            return "[" + host.replace("%", "%25") + "]";
        }
        return host;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
