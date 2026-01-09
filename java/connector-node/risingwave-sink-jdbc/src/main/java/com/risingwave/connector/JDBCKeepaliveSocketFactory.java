/*
 * Copyright 2023 RisingWave Labs
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
import java.net.Socket;
import javax.net.SocketFactory;
import jdk.net.ExtendedSocketOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom SocketFactory that enables TCP keep-alive with configurable parameters. This factory
 * configures TCP_KEEPIDLE, TCP_KEEPINTVL, and TCP_KEEPCNT at the socket level.
 */
public class JDBCKeepaliveSocketFactory extends SocketFactory {
    private static final Logger LOG = LoggerFactory.getLogger(JDBCKeepaliveSocketFactory.class);

    private static int keepaliveIdleSeconds = 60;
    private static int keepaliveIntervalSeconds = 10;
    private static int keepaliveCount = 3;

    /** Configure keep-alive parameters. Must be called before creating connections. */
    public static void configure(int idle, int interval, int count) {
        keepaliveIdleSeconds = idle;
        keepaliveIntervalSeconds = interval;
        keepaliveCount = count;
        LOG.info(
                "TCP keep-alive configured: idle={}s, interval={}s, count={}",
                idle,
                interval,
                count);
    }

    @Override
    public Socket createSocket() throws IOException {
        Socket socket = new Socket();
        configureSocket(socket);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        configureSocket(socket);
        return socket;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
            throws IOException {
        Socket socket = new Socket(host, port, localHost, localPort);
        configureSocket(socket);
        return socket;
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        configureSocket(socket);
        return socket;
    }

    @Override
    public Socket createSocket(
            InetAddress address, int port, InetAddress localAddress, int localPort)
            throws IOException {
        Socket socket = new Socket(address, port, localAddress, localPort);
        configureSocket(socket);
        return socket;
    }

    private void configureSocket(Socket socket) throws IOException {
        socket.setKeepAlive(true);

        try {
            // Use JDK 9+ ExtendedSocketOptions for TCP keep-alive parameters
            // These options are supported on Linux, macOS, and Windows
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, keepaliveIdleSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, keepaliveIntervalSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, keepaliveCount);

            LOG.info("TCP keep-alive options set successfully on socket");
        } catch (Exception e) {
            LOG.warn(
                    "Failed to set TCP keep-alive extended options, "
                            + "falling back to basic keep-alive only: {}",
                    e.getMessage());
            // Fallback: at least basic keep-alive is enabled
        }
    }
}
