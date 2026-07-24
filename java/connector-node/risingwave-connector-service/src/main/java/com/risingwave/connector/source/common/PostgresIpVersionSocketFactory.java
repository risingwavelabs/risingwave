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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import javax.net.SocketFactory;

public class PostgresIpVersionSocketFactory extends SocketFactory {
    private final PostgresIpVersion ipVersion;

    public PostgresIpVersionSocketFactory() {
        this(PostgresIpVersion.ANY.toString());
    }

    public PostgresIpVersionSocketFactory(String ipVersion) {
        this.ipVersion = PostgresIpVersion.fromUserValue(ipVersion);
    }

    @Override
    public Socket createSocket() {
        return new AddressFamilySocket(ipVersion);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        var socket = createSocket();
        socket.connect(InetSocketAddress.createUnresolved(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, java.net.InetAddress localHost, int localPort)
            throws IOException {
        var socket = createSocket();
        socket.bind(new InetSocketAddress(localHost, localPort));
        socket.connect(InetSocketAddress.createUnresolved(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(java.net.InetAddress host, int port) throws IOException {
        var socket = createSocket();
        socket.connect(new InetSocketAddress(host, port));
        return socket;
    }

    @Override
    public Socket createSocket(
            java.net.InetAddress address,
            int port,
            java.net.InetAddress localAddress,
            int localPort)
            throws IOException {
        var socket = createSocket();
        socket.bind(new InetSocketAddress(localAddress, localPort));
        socket.connect(new InetSocketAddress(address, port));
        return socket;
    }

    private static class AddressFamilySocket extends Socket {
        private final PostgresIpVersion ipVersion;

        AddressFamilySocket(PostgresIpVersion ipVersion) {
            this.ipVersion = ipVersion;
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            if (ipVersion == PostgresIpVersion.ANY || !(endpoint instanceof InetSocketAddress)) {
                super.connect(endpoint, timeout);
                return;
            }

            var inetEndpoint = (InetSocketAddress) endpoint;
            var host = inetEndpoint.getHostString();
            var port = inetEndpoint.getPort();
            try {
                var address = ipVersion.resolveAddresses(host).getFirst();
                super.connect(new InetSocketAddress(address, port), timeout);
            } catch (RuntimeException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
    }
}
