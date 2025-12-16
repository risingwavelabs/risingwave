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
    private static volatile HTTPServer prometheusHttpServer;

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

    // Debezium CDC engine error metrics.
    //
    // NOTE: We intentionally keep error_kind low-cardinality (e.g. "oom" / "other"), and avoid
    // labeling by the full error message/stacktrace to prevent metric explosion.
    private static final Counter cdcEngineFailureTotal =
            Counter.build()
                    .name("connector_cdc_engine_failure_total")
                    .labelNames("source_type", "source_id", "connector_class", "error_kind")
                    .help("Number of Debezium CDC engine failures observed in connector-node.")
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
        registry.register(totalSinkConnections);
        registry.register(sourceRowsReceived);
        registry.register(sinkRowsReceived);
        registry.register(errorCount);
        registry.register(cdcEngineFailureTotal);
        registry.register(cpuUsage);
        registry.register(ramUsage);
        PeriodicMetricsCollector collector = new PeriodicMetricsCollector(1000, "connector");
        collector.start();
        try {
            // Keep a reference so it can be closed gracefully on JVM shutdown.
            prometheusHttpServer = new HTTPServer(new InetSocketAddress(host, port), registry);
            Runtime.getRuntime()
                    .addShutdownHook(
                            new Thread(
                                    () -> {
                                        try {
                                            if (prometheusHttpServer != null) {
                                                prometheusHttpServer.close();
                                            }
                                        } catch (Throwable ignored) {
                                            // Best-effort cleanup.
                                        }
                                    },
                                    "connector-node-prometheus-httpserver-shutdown"));
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

    public static void recordCdcEngineFailure(
            String sourceType,
            String sourceId,
            String connectorClass,
            String message,
            Throwable error) {
        String kind = classifyCdcEngineFailureKind(message, error);
        cdcEngineFailureTotal.labels(sourceType, sourceId, connectorClass, kind).inc();
    }

    private static String classifyCdcEngineFailureKind(String message, Throwable error) {
        // Use the actual exception type as error_kind, instead of keyword matching.
        // This stays low-cardinality in practice and is much more precise for alerting.
        Throwable root = rootCause(error);
        if (root != null) {
            String simple = root.getClass().getSimpleName();
            if (simple != null && !simple.isBlank()) {
                return simple;
            }
            // Fallback to the full class name if needed (still finite), but keep it short.
            String full = root.getClass().getName();
            return full == null || full.isBlank() ? "Unknown" : full;
        }
        // If we don't have a Throwable at all, we can't reliably infer the type.
        // Keep a stable fallback value to avoid label explosion.
        return "Unknown";
    }

    private static Throwable rootCause(Throwable t) {
        if (t == null) {
            return null;
        }
        Throwable cur = t;
        // Guard against cause cycles or overly deep chains.
        // NOTE: Future improvement: detect non-trivial cycles (beyond `next == cur`) and
        // de-duplicate / filter wrapper exceptions that add little signal, so `error_kind` can be
        // derived from the most useful root cause.
        for (int i = 0; i < 32; i++) {
            Throwable next = cur.getCause();
            if (next == null || next == cur) {
                return cur;
            }
            cur = next;
        }
        return cur;
    }
}
