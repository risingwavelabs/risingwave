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

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Debezium configuration of one {@link CdcSource}, resolved from the common {@code
 * debezium.properties}, the connector's own template, and the WITH options of the source.
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

    protected final long sourceId;
    protected final String startOffset;
    protected final Map<String, String> userProps;
    protected final boolean snapshotDone;
    protected final boolean isCdcSourceJob;
    protected final boolean isBackfillSource;
    private final StringSubstitutor substitutor;
    private final Properties resolvedDbzProps;

    protected DbzConnectorConfig(CdcSource source) {
        this.sourceId = source.getSourceId();
        this.startOffset = source.getStartOffset();
        this.userProps = source.getUserProps();
        this.snapshotDone = source.isSnapshotDone();
        this.isCdcSourceJob = source.isCdcSourceJob();
        this.isBackfillSource = source.isBackfillSource();

        var substitutionProps = new HashMap<>(userProps);
        substitutionProps.put("source.id", Long.toString(sourceId));
        this.substitutor = new StringSubstitutor(substitutionProps);

        var dbzProps = loadPropertiesTemplate(DBZ_CONFIG_FILE);
        customizeDebeziumProperties(dbzProps);
        if (isCdcSourceJob) {
            LOG.info("Disable table filtering for the shared {} source", source.getSourceType());
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

    public Properties getResolvedDebeziumProps() {
        return resolvedDbzProps;
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
     * accordingly.
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

        // Validation is already done in SourceValidateHandler during CREATE/ALTER SOURCE
        // At this point, ratioStr is guaranteed to be valid (0.01-0.8)
        double queueMemoryRatio = Double.parseDouble(ratioStr);

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
