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

    private static class KeepaliveConfig {
        int idleSeconds = 60;
        int intervalSeconds = 10;
        int count = 3;

        KeepaliveConfig() {}

        KeepaliveConfig(int idle, int interval, int count) {
            this.idleSeconds = idle;
            this.intervalSeconds = interval;
            this.count = count;
        }
    }

    private static final ThreadLocal<KeepaliveConfig> keepaliveConfig =
            ThreadLocal.withInitial(KeepaliveConfig::new);

    /**
     * Configure keep-alive parameters for the current thread. Must be called before creating
     * connections.
     */
    public static void configure(int idle, int interval, int count) {
        keepaliveConfig.set(new KeepaliveConfig(idle, interval, count));
        LOG.info(
                "TCP keep-alive configured for thread {}: idle={}s, interval={}s, count={}",
                Thread.currentThread().getName(),
                idle,
                interval,
                count);
    }

    /** Clear keep-alive configuration for the current thread. */
    public static void clearConfig() {
        keepaliveConfig.remove();
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

        KeepaliveConfig config = keepaliveConfig.get();

        try {
            // Use JDK 9+ ExtendedSocketOptions for TCP keep-alive parameters
            // These options are supported on Linux, macOS, and Windows
            socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, config.idleSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, config.intervalSeconds);
            socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, config.count);

            LOG.debug(
                    "TCP keep-alive options set on socket: idle={}s, interval={}s, count={}",
                    config.idleSeconds,
                    config.intervalSeconds,
                    config.count);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to set TCP keep-alive extended options, "
                            + "falling back to basic keep-alive only: {}",
                    e.getMessage());
            // Fallback: at least basic keep-alive is enabled
        }
    }
}
