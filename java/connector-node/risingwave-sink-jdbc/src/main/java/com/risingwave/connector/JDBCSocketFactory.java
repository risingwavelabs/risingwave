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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import javax.net.SocketFactory;
import jdk.net.ExtendedSocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCSocketFactory extends SocketFactory {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCSocketFactory.class);

    private static class SocketConfig {
        boolean keepaliveEnabled = false;
        int idleSeconds = 60;
        int intervalSeconds = 10;
        int count = 3;
        JDBCPostgresIpVersion ipVersion = JDBCPostgresIpVersion.ANY;

        SocketConfig() {}

        SocketConfig(
                boolean keepaliveEnabled,
                int idle,
                int interval,
                int count,
                JDBCPostgresIpVersion ipVersion) {
            this.keepaliveEnabled = keepaliveEnabled;
            this.idleSeconds = idle;
            this.intervalSeconds = interval;
            this.count = count;
            this.ipVersion = ipVersion;
        }
    }

    private static final ThreadLocal<SocketConfig> socketConfig =
            ThreadLocal.withInitial(SocketConfig::new);

    static void configure(
            boolean keepaliveEnabled,
            int idle,
            int interval,
            int count,
            JDBCPostgresIpVersion ipVersion) {
        socketConfig.set(new SocketConfig(keepaliveEnabled, idle, interval, count, ipVersion));
    }

    static void clearConfig() {
        socketConfig.remove();
    }

    @Override
    public Socket createSocket() {
        return configureSocket(new AddressFamilySocket(socketConfig.get()));
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        var socket = createSocket();
        socket.connect(InetSocketAddress.createUnresolved(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
            throws IOException {
        var socket = createSocket();
        socket.bind(new InetSocketAddress(localHost, localPort));
        socket.connect(InetSocketAddress.createUnresolved(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        var socket = createSocket();
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(
            InetAddress address, int port, InetAddress localAddress, int localPort)
            throws IOException {
        var socket = createSocket();
        socket.bind(new InetSocketAddress(localAddress, localPort));
        socket.connect(new InetSocketAddress(address, port));
        return socket;
    }

    private Socket configureSocket(Socket socket) {
        var config = socketConfig.get();
        if (!config.keepaliveEnabled) {
            return socket;
        }

        try {
            socket.setKeepAlive(true);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, config.idleSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, config.intervalSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, config.count);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to set TCP keep-alive extended options, "
                            + "falling back to basic keep-alive only: {}",
                    e.getMessage());
        }
        return socket;
    }

    private static class AddressFamilySocket extends Socket {
        private final SocketConfig config;

        AddressFamilySocket(SocketConfig config) {
            this.config = config;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            if (config.ipVersion == JDBCPostgresIpVersion.ANY
                    || !(endpoint instanceof InetSocketAddress)) {
                super.connect(endpoint, timeout);
                return;
            }

            var inetEndpoint = (InetSocketAddress) endpoint;
            var host = inetEndpoint.getHostString();
            var port = inetEndpoint.getPort();
            try {
                var address = config.ipVersion.resolveAddresses(host).getFirst();
                super.connect(new InetSocketAddress(address, port), timeout);
            } catch (RuntimeException | java.sql.SQLException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }
}
