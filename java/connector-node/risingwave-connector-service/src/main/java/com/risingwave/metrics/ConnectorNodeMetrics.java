// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.metrics;

import static io.grpc.Status.INTERNAL;

import com.sun.management.OperatingSystemMXBean;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;

public class ConnectorNodeMetrics {
    private static final Counter activeSourceConnections =
            Counter.build()
                    .name("active_source_connections")
                    .labelNames("source_type", "ip")
                    .help("Number of active source connections")
                    .register();

    private static final Counter activeSinkConnections =
            Counter.build()
                    .name("active_sink_connections")
                    .labelNames("connector_type", "ip")
                    .help("Number of active sink connections")
                    .register();

    private static final Counter totalSinkConnections =
            Counter.build()
                    .name("total_sink_connections")
                    .labelNames("connector_type", "ip")
                    .help("Number of total connections")
                    .register();
    private static final Counter cpuUsage =
            Counter.build()
                    .name("process_cpu_seconds_total")
                    .labelNames("job")
                    .help("Total user and system CPU time spent in seconds.")
                    .register();
    private static final Gauge ramUsage =
            Gauge.build()
                    .name("process_resident_memory_bytes")
                    .labelNames("job")
                    .help("RAM usage in bytes")
                    .register();

    private static final Counter sourceRowsReceived =
            Counter.build()
                    .name("connector_source_rows_received")
                    .labelNames("source_type", "source_id")
                    .help("Number of rows received by source")
                    .register();
    private static final Counter sinkRowsReceived =
            Counter.build()
                    .name("connector_sink_rows_received")
                    .labelNames("connector_type", "sink_id")
                    .help("Number of rows received by sink")
                    .register();

    private static final Counter errorCount =
            Counter.build()
                    .name("error_count")
                    .labelNames("sink_type", "ip")
                    .help("Number of errors")
                    .register();

    static class PeriodicMetricsCollector extends Thread {
        private final int interval;
        private final OperatingSystemMXBean osBean;
        private final String job;

        public PeriodicMetricsCollector(int intervalMillis, String job) {
            this.interval = intervalMillis;
            this.job = job;
            this.osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        }

        @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(interval);
                    collect();
                } catch (InterruptedException e) {
                    throw INTERNAL.withCause(e).asRuntimeException();
                }
            }
        }

        private void collect() {
            double cpuTotal = osBean.getProcessCpuTime() / 1000000000.0;
            double cpuPast = ConnectorNodeMetrics.cpuUsage.labels(job).get();
            ConnectorNodeMetrics.cpuUsage.labels(job).inc(cpuTotal - cpuPast);
            long ramUsageBytes =
                    Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            ConnectorNodeMetrics.ramUsage.labels(job).set(ramUsageBytes);
        }
    }

    public static void startHTTPServer(String host, int port) {
        CollectorRegistry registry = new CollectorRegistry();
        registry.register(activeSourceConnections);
        registry.register(activeSinkConnections);
        registry.register(sourceRowsReceived);
        registry.register(sinkRowsReceived);
        registry.register(cpuUsage);
        registry.register(ramUsage);
        PeriodicMetricsCollector collector = new PeriodicMetricsCollector(1000, "connector");
        collector.start();
        try {
            new HTTPServer(new InetSocketAddress(host, port), registry);
        } catch (IOException e) {
            throw INTERNAL.withDescription("Failed to start HTTP server")
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    public static void incActiveSourceConnections(String sourceType, String ip) {
        activeSourceConnections.labels(sourceType, ip).inc();
    }

    public static void decActiveSourceConnections(String sourceType, String ip) {
        activeSourceConnections.remove(sourceType, ip);
    }

    public static void incActiveSinkConnections(String connectorName, String ip) {
        activeSinkConnections.labels(connectorName, ip).inc();
    }

    public static void decActiveSinkConnections(String connectorName, String ip) {
        activeSinkConnections.remove(connectorName, ip);
    }

    public static void incSourceRowsReceived(String sourceType, String sourceId, double amt) {
        sourceRowsReceived.labels(sourceType, sourceId).inc(amt);
    }

    public static void incSinkRowsReceived(String connectorName, String sinkId, double amt) {
        sinkRowsReceived.labels(connectorName, sinkId).inc(amt);
    }

    public static void incTotalConnections(String sinkType, String ip) {
        totalSinkConnections.labels(sinkType, ip).inc();
    }

    public static void incErrorCount(String sinkType, String ip) {
        errorCount.labels(sinkType, ip).inc();
    }

    public static void setRamUsage(String ip, long usedRamInBytes) {
        ramUsage.labels(ip).set(usedRamInBytes);
    }
}
