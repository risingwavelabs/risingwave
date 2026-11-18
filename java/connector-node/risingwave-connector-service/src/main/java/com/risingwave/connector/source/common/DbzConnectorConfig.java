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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.proto.ConnectorServiceProto.GetEventStreamRequest;
import io.debezium.config.CommonConnectorConfig;
import java.io.IOException;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The configuration of one {@link CdcSource}: everything the connector node derives from the
 * reader's request, including the WITH options of the source and the Debezium properties resolved
 * from the common {@code debezium.properties}, the connector's own template, and those options.
 */
public abstract class DbzConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DbzConnectorConfig.class);

    private static final String QUEUE_MAX_MEMORY_RATIO = "queue.memory.ratio";
    public static final double MIN_QUEUE_MAX_MEMORY_RATIO = 0.01;
    public static final double MAX_QUEUE_MAX_MEMORY_RATIO = 0.8;

    /* Common configs */
    public static final String HOST = "hostname";
    public static final String PORT = "port";
    public static final String USER = "username";
    public static final String PASSWORD = "password";

    public static final String DB_NAME = "database.name";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String TABLE_NAME = "table.name";

    /* RisingWave configs */
    private static final String DBZ_CONFIG_FILE = "debezium.properties";

    public static final String DBZ_PROPERTY_PREFIX = "debezium.";

    public static final String HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY =
            "heartbeat.table.auto.initialize";

    private static final String WAIT_FOR_STREAMING_START_TIMEOUT_SECS =
            "cdc.source.wait.streaming.start.timeout";
    private static final String SNAPSHOT_MODE_KEY = "debezium.snapshot.mode";
    private static final String SNAPSHOT_MODE_BACKFILL = "rw_cdc_backfill";
    private static final String NEEDS_OFFSET_COMMIT_ON_CHECKPOINT_KEY =
            "cdc.source.needs.offset.commit.on.checkpoint";

    protected final SourceTypeE sourceType;
    protected final long sourceId;
    protected final String startOffset;
    protected final Map<String, String> userProps;
    protected final boolean snapshotDone;
    protected final boolean isCdcSourceJob;
    protected final boolean isBackfillSource;
    protected final int waitStreamingStartTimeout;
    private final boolean needsOffsetCommitOnCheckpoint;
    private final StringSubstitutor substitutor;
    private final Properties resolvedDbzProps;

    protected DbzConnectorConfig(GetEventStreamRequest request) {
        this.sourceType = SourceTypeE.valueOf(request.getSourceType());
        this.sourceId = request.getSourceId();
        this.startOffset = request.getStartOffset();
        this.userProps = request.getPropertiesMap();
        this.snapshotDone = request.getSnapshotDone();
        this.isCdcSourceJob = request.getIsSourceJob();
        this.isBackfillSource = SNAPSHOT_MODE_BACKFILL.equals(userProps.get(SNAPSHOT_MODE_KEY));
        this.waitStreamingStartTimeout =
                Integer.parseInt(
                        userProps.getOrDefault(WAIT_FOR_STREAMING_START_TIMEOUT_SECS, "60"));
        this.needsOffsetCommitOnCheckpoint =
                Boolean.parseBoolean(userProps.get(NEEDS_OFFSET_COMMIT_ON_CHECKPOINT_KEY));
        LOG.info(
                "Building the Debezium config: source={}, sourceId={}, startOffset={},"
                        + " snapshotDone={}, isCdcBackfill={}, isCdcSourceJob={},"
                        + " waitStreamingStartTimeout={}, needsOffsetCommitOnCheckpoint={}",
                sourceType,
                sourceId,
                startOffset,
                snapshotDone,
                isBackfillSource,
                isCdcSourceJob,
                waitStreamingStartTimeout,
                needsOffsetCommitOnCheckpoint);

        var substitutionProps = new HashMap<>(userProps);
        substitutionProps.put("source.id", Long.toString(sourceId));
        this.substitutor = new StringSubstitutor(substitutionProps);

        var dbzProps = loadPropertiesTemplate(DBZ_CONFIG_FILE);
        customizeDebeziumProperties(dbzProps);
        if (isCdcSourceJob) {
            LOG.info("Disable table filtering for the shared {} source", sourceType);
            dbzProps.remove("table.include.list");
        }
        var otherProps = extractDebeziumProperties(userProps);
        for (var entry : otherProps.entrySet()) {
            dbzProps.putIfAbsent(entry.getKey(), entry.getValue());
        }

        // Calculate max.queue.size.in.bytes based on JVM heap size and ratio
        calculateAndSetMaxQueueSizeInBytes(dbzProps);

        LOG.info(
                "Debezium max.queue.size.in.bytes: {} bytes ({} MB)",
                dbzProps.getProperty("max.queue.size.in.bytes", "not set"),
                dbzProps.getProperty("max.queue.size.in.bytes") != null
                        ? Long.parseLong(dbzProps.getProperty("max.queue.size.in.bytes"))
                                / 1024
                                / 1024
                        : 0);

        this.resolvedDbzProps = dbzProps;
    }

    public SourceTypeE getSourceType() {
        return sourceType;
    }

    public long getSourceId() {
        return sourceId;
    }

    public boolean isCdcSourceJob() {
        return isCdcSourceJob;
    }

    public boolean isBackfillSource() {
        return isBackfillSource;
    }

    /**
     * Whether Debezium may acknowledge a record only after RisingWave has checkpointed it. This is
     * needed when the acknowledged offset is committed back to the upstream database, where it lets
     * the database discard the change log before the offset.
     */
    public boolean needsOffsetCommitOnCheckpoint() {
        return needsOffsetCommitOnCheckpoint;
    }

    public Properties getResolvedDebeziumProps() {
        return resolvedDbzProps;
    }

    public String getHost() {
        return userProps.get(HOST);
    }

    public String getPort() {
        return userProps.get(PORT);
    }

    public String getUser() {
        return userProps.get(USER);
    }

    public String getPassword() {
        return userProps.get(PASSWORD);
    }

    public String getDbName() {
        return userProps.get(DB_NAME);
    }

    public String getSchemaName() {
        return userProps.get(SCHEMA_NAME);
    }

    public String getTableName() {
        return userProps.get(TABLE_NAME);
    }

    public boolean isAutoInitializeHeartbeatTable() {
        return "true".equals(userProps.get(HEARTBEAT_TABLE_AUTO_INITIALIZE_KEY));
    }

    /**
     * Checks the WITH options the connector requires and then the upstream database, as {@code
     * CREATE SOURCE} and {@code CREATE TABLE} do before accepting the statement. It connects to the
     * upstream database, so it runs only when called; building the config does not run it.
     */
    public abstract void validateSource(TableSchema tableSchema, boolean isBackfillTable)
            throws Exception;

    /**
     * Waits until Debezium reports the streaming source as connected. Returns false when the wait
     * is cancelled by {@code shouldContinue} or times out after the configured number of seconds.
     */
    public boolean waitForStreamingRunning(BooleanSupplier shouldContinue)
            throws InterruptedException {
        var dbServerName = resolvedDbzProps.getProperty(CommonConnectorConfig.TOPIC_PREFIX.name());
        LOG.info("Waiting for streaming source of {} to start", dbServerName);
        var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        int pollCount = 0;
        while (true) {
            // Check cancellation before readiness: a replacement engine can register metrics
            // under the same server name while the previous reader is being dropped.
            if (!shouldContinue.getAsBoolean()) {
                LOG.info("Cancelled waiting for streaming source of {} to start", dbServerName);
                return false;
            }
            // Copy from debezium test suite: io.debezium.embedded.AbstractConnectorTest. The jmx
            // bean metric is marked as true before starting the binlog client, which may still
            // fail to connect the upstream database.
            try {
                var metricsName = getStreamingMetricsObjectName(dbServerName);
                if ((boolean) mbeanServer.getAttribute(metricsName, "Connected")) {
                    LOG.info("Debezium streaming source of {} started", dbServerName);
                    return true;
                }
            } catch (JMException _ex) {
                // expected while the streaming source (aka. binlog client) is not ready
            }
            if (pollCount > waitStreamingStartTimeout) {
                LOG.error(
                        "Debezium streaming source of {} failed to start in timeout {}",
                        dbServerName,
                        waitStreamingStartTimeout);
                return false;
            }
            TimeUnit.SECONDS.sleep(1); // poll interval; interruption cancels startup
            pollCount++;
        }
    }

    /**
     * Returns the JMX name under which Debezium registers the streaming metrics of {@code
     * dbServerName}, which {@link #waitForStreamingRunning} polls.
     */
    protected ObjectName getStreamingMetricsObjectName(String dbServerName)
            throws MalformedObjectNameException {
        return new ObjectName(
                "debezium."
                        + sourceType.name().toLowerCase(Locale.ROOT)
                        + ":type=connector-metrics,context=streaming,server="
                        + dbServerName);
    }

    /**
     * Adds the connector's settings to {@code dbzProps}, which already holds the common settings of
     * {@code debezium.properties}. The constructor calls it once, before the user-supplied {@code
     * debezium.*} options are merged, so those options never override what it sets. Implementations
     * read the source through the protected fields of this class and must not keep their own
     * fields, because the call happens before a subclass constructor body runs.
     */
    protected abstract void customizeDebeziumProperties(Properties dbzProps);

    protected boolean hasStartOffset() {
        return startOffset != null && !startOffset.isBlank();
    }

    protected void ensureRequiredProps() {
        ensurePropNotBlank(HOST);
        ensurePropNotBlank(PORT);
        ensurePropNotBlank(DB_NAME);
        ensurePropNotBlank(USER);
        ensurePropExists(PASSWORD);
        // ensure table name is passed by user in non-sharing mode
        if (!isCdcSourceJob) {
            ensurePropNotBlank(TABLE_NAME);
        }
    }

    protected void ensurePropNotBlank(String name) {
        ensurePropExists(name);
        if (StringUtils.isBlank(userProps.get(name))) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' cannot be empty. Please check the WITH properties", name));
        }
    }

    protected void ensurePropExists(String name) {
        if (!userProps.containsKey(name)) {
            throw ValidatorUtils.invalidArgument(
                    String.format("'%s' is not found. Please check the WITH properties", name));
        }
    }

    /**
     * Loads a {@code .properties} template from the classpath and replaces its {@code ${...}}
     * placeholders with the WITH options of the source.
     */
    protected Properties loadPropertiesTemplate(String fileName) {
        var dbProps = new Properties();
        try (var input = getClass().getClassLoader().getResourceAsStream(fileName)) {
            assert input != null;
            var inputStr = IOUtils.toString(input, StandardCharsets.UTF_8);
            // load before substitution, so that we do not need to escape the substituted text
            dbProps.load(new StringReader(inputStr));
            dbProps.replaceAll((k, v) -> substitutor.replace(v));
        } catch (IOException e) {
            throw new RuntimeException("failed to load config file " + fileName, e);
        }
        return dbProps;
    }

    private static Map<String, String> extractDebeziumProperties(
            Map<String, String> userProperties) {
        // retain only debezium properties if any
        var dbzProps = new HashMap<String, String>();
        for (var entry : userProperties.entrySet()) {
            var key = entry.getKey();
            if (key.startsWith(DBZ_PROPERTY_PREFIX)) {
                // remove the prefix
                dbzProps.put(key.substring(DBZ_PROPERTY_PREFIX.length()), entry.getValue());
            }
        }
        return dbzProps;
    }

    /**
     * Calculate and set max.queue.size.in.bytes based on JVM heap size and memory ratio.
     *
     * <p>If user does not specify queue.memory.ratio, this method does nothing. Debezium will use
     * whatever is configured in debezium.properties (max.queue.size = 8192 by default).
     *
     * <p>If user specifies a valid ratio (0.01-0.8), calculate and set max.queue.size.in.bytes
     * accordingly. An invalid ratio is rejected here, which fails validation with the message.
     *
     * <p>JVM heap size is obtained from Runtime.getRuntime().maxMemory(), which reflects the -Xmx
     * value set by Rust code during JVM initialization. See: src/jni_core/src/jvm_runtime.rs:85-103
     *
     * <p>The Rust code sets -Xmx based on: 1. JVM_HEAP_SIZE env var if set 2. Otherwise:
     * system_memory_available_bytes() * 0.07
     *
     * @param dbzProps The Debezium properties to update
     */
    private void calculateAndSetMaxQueueSizeInBytes(Properties dbzProps) {
        // Check if user specified a custom ratio
        String ratioStr = dbzProps.getProperty(QUEUE_MAX_MEMORY_RATIO);
        if (ratioStr == null) {
            // User did not specify ratio, do nothing
            // Debezium will use its default max.queue.size from debezium.properties
            LOG.info(
                    "Debezium {} not specified, skipping calculating and setting"
                            + " max.queue.size.in.bytes calculation",
                    QUEUE_MAX_MEMORY_RATIO);
            return;
        }

        double queueMemoryRatio;
        try {
            queueMemoryRatio = Double.parseDouble(ratioStr);
        } catch (NumberFormatException e) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be a valid number, got: '%s'",
                            ratioStr));
        }
        if (queueMemoryRatio < MIN_QUEUE_MAX_MEMORY_RATIO
                || queueMemoryRatio > MAX_QUEUE_MAX_MEMORY_RATIO) {
            throw ValidatorUtils.invalidArgument(
                    String.format(
                            "'debezium.queue.memory.ratio' must be between %s and %s, got: %s",
                            MIN_QUEUE_MAX_MEMORY_RATIO,
                            MAX_QUEUE_MAX_MEMORY_RATIO,
                            queueMemoryRatio));
        }

        // Get JVM max heap size from Runtime
        // This reflects the -Xmx value set by Rust during JVM initialization
        // No need to read JVM_HEAP_SIZE env var again - it's already baked into the JVM
        Runtime runtime = Runtime.getRuntime();
        long jvmHeapSize = runtime.maxMemory();

        LOG.info(
                "JVM max heap size (from Runtime): {} bytes ({} MB)",
                jvmHeapSize,
                jvmHeapSize / 1024 / 1024);
        LOG.info("Debezium {}: {}", QUEUE_MAX_MEMORY_RATIO, queueMemoryRatio);

        // Calculate max queue size in bytes
        long maxQueueSizeInBytes = (long) (jvmHeapSize * queueMemoryRatio);

        // Set the calculated value
        dbzProps.setProperty("max.queue.size.in.bytes", String.valueOf(maxQueueSizeInBytes));

        LOG.info(
                "Calculated debezium max.queue.size.in.bytes: {} bytes ({} MB)",
                maxQueueSizeInBytes,
                maxQueueSizeInBytes / 1024 / 1024);
    }
}
