// Copyright 2023 RisingWave Labs
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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.source.SourceTypeE;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbzSourceUtils {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceUtils.class);

    public static boolean waitForStreamingRunning(SourceTypeE sourceType, String dbServerName) {
        // Right now, we only needs to wait for MySQL source, as it's the only source that support
        // backfill. After we support backfill for other sources, we need to wait for all sources
        // too
        if (sourceType == SourceTypeE.MYSQL) {
            LOG.info("Waiting for streaming source of {} to start", dbServerName);
            return waitForStreamingRunningInner("mysql", dbServerName);
        } else {
            LOG.info("Unsupported backfill source, just return true for {}", dbServerName);
            return true;
        }
    }

    private static boolean waitForStreamingRunningInner(String connector, String dbServerName) {
        int maxPollCount = 10;
        while (!isStreamingRunning(connector, dbServerName, "streaming")) {
            maxPollCount--;
            if (maxPollCount == 0) {
                LOG.error("Debezium streaming source of {} failed to start", dbServerName);
                return false;
            }
            try {
                TimeUnit.SECONDS.sleep(1); // poll interval
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting for streaming source to start", e);
            }
        }

        LOG.info("Debezium streaming source of {} started", dbServerName);
        return true;
    }

    // Copy from debezium test suite: io.debezium.embedded.AbstractConnectorTest
    // Notes: although this method is recommended by the community
    // (https://debezium.zulipchat.com/#narrow/stream/302529-community-general/topic/.E2.9C.94.20Embedded.20engine.20has.20started.20StreamingChangeEventSource/near/405121659),
    // but it is not solid enough. As the jmx bean metric is marked as true before starting the
    // binlog client, which may fail to connect the upstream database.
    private static boolean isStreamingRunning(String connector, String server, String contextName) {
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        try {
            return (boolean)
                    mbeanServer.getAttribute(
                            getStreamingMetricsObjectName(connector, server, contextName),
                            "Connected");
        } catch (JMException ex) {
            LOG.warn("Failed to get streaming metrics", ex);
        }
        return false;
    }

    private static ObjectName getStreamingMetricsObjectName(
            String connector, String server, String context) throws MalformedObjectNameException {
        return new ObjectName(
                "debezium."
                        + connector
                        + ":type=connector-metrics,context="
                        + context
                        + ",server="
                        + server);
    }
}
