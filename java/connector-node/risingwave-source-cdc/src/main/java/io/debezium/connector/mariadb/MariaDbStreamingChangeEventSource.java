/*
 * Copyright 2026 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mariadb;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.MariadbGtidSet;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.BinlogTaskContext;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mariadb.metrics.MariaDbStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MariaDB streaming source with RisingWave's post-binlog-connect readiness callback. */
public class MariaDbStreamingChangeEventSource
        extends BinlogStreamingChangeEventSource<MariaDbPartition, MariaDbOffsetContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MariaDbStreamingChangeEventSource.class);

    private final MariaDbConnectorConfig connectorConfig;
    private final TableId signalDataCollectionId;
    private MariadbGtidSet gtidSet;
    private Runnable onConnectedCallback;
    private final AtomicBoolean connectedSignaled = new AtomicBoolean(false);

    public MariaDbStreamingChangeEventSource(
            MariaDbConnectorConfig connectorConfig,
            BinlogConnectorConnection connection,
            EventDispatcher<MariaDbPartition, TableId> dispatcher,
            ErrorHandler errorHandler,
            Clock clock,
            MariaDbTaskContext taskContext,
            MariaDbStreamingChangeEventSourceMetrics metrics,
            SnapshotterService snapshotterService) {
        super(
                connectorConfig,
                connection,
                dispatcher,
                errorHandler,
                clock,
                taskContext,
                taskContext.getSchema(),
                metrics,
                snapshotterService);
        this.connectorConfig = connectorConfig;
        this.signalDataCollectionId = getSignalDataCollectionId(connectorConfig);
    }

    public void setOnConnectedCallback(Runnable callback) {
        this.onConnectedCallback = callback;
    }

    @Override
    protected BinaryLogClient createBinaryLogClient(
            BinlogTaskContext<?> taskContext,
            BinlogConnectorConfig connectorConfig,
            Map<String, Thread> clientThreads,
            BinlogConnectorConnection connection) {
        BinaryLogClient client =
                super.createBinaryLogClient(
                        taskContext, connectorConfig, clientThreads, connection);
        if (connectorConfig.isSqlQueryIncluded()) {
            client.setUseSendAnnotateRowsEvent(true);
        }
        client.registerLifecycleListener(
                new BinaryLogClient.AbstractLifecycleListener() {
                    @Override
                    public void onConnect(BinaryLogClient client) {
                        if (connectedSignaled.compareAndSet(false, true)
                                && onConnectedCallback != null) {
                            onConnectedCallback.run();
                        }
                    }
                });
        return client;
    }

    @Override
    public void init(MariaDbOffsetContext offsetContext) {
        setEffectiveOffsetContext(
                offsetContext != null
                        ? offsetContext
                        : MariaDbOffsetContext.initial(connectorConfig));
    }

    @Override
    protected Class<? extends SourceConnector> getConnectorClass() {
        return MariaDbConnector.class;
    }

    @Override
    protected void configureReplicaCompatibility(BinaryLogClient client) {
        client.setMariaDbSlaveCapability(4);
    }

    @Override
    protected void setEventTimestamp(Event event, long eventTs) {
        eventTimestamp = Instant.ofEpochMilli(eventTs);
    }

    @Override
    protected void handleGtidEvent(
            MariaDbPartition partition,
            MariaDbOffsetContext offsetContext,
            Event event,
            Predicate<String> gtidDmlSourceFilter)
            throws InterruptedException {
        LOGGER.debug("MariaDB GTID transaction: {}", event);
        MariadbGtidEventData gtidEvent = unwrapData(event);
        String gtid =
                String.format(
                        "%d-%d-%d",
                        gtidEvent.getDomainId(),
                        event.getHeader().getServerId(),
                        gtidEvent.getSequence());
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString());
        setIgnoreDmlEventByGtidSource(false);
        if (gtidDmlSourceFilter != null && gtid != null) {
            String uuid = gtidEvent.getDomainId() + "-" + gtidEvent.getServerId();
            if (!gtidDmlSourceFilter.test(uuid)) {
                setIgnoreDmlEventByGtidSource(true);
            }
        }
        setGtidChanged(gtid);
        handleTransactionBegin(partition, offsetContext, event, null);
    }

    @Override
    protected void handleRecordingQuery(MariaDbOffsetContext offsetContext, Event event) {
        EventData eventData = unwrapData(event);
        if (eventData instanceof AnnotateRowsEventData) {
            String query = ((AnnotateRowsEventData) eventData).getRowsQuery();
            if (signalDataCollectionId != null
                    && query.toLowerCase()
                            .contains(signalDataCollectionId.toQuotedString('`').toLowerCase())) {
                return;
            }
            offsetContext.setQuery(query);
        }
    }

    @Override
    protected EventType getIncludeQueryEventType() {
        return EventType.ANNOTATE_ROWS;
    }

    @Override
    protected EventType getGtidEventType() {
        return EventType.MARIADB_GTID;
    }

    @Override
    protected void initializeGtidSet(String value) {
        this.gtidSet = new MariadbGtidSet(value);
    }

    @Override
    protected SSLMode sslModeFor(BinlogConnectorConfig.SecureConnectionMode mode) {
        return switch ((MariaDbConnectorConfig.MariaDbSecureConnectionMode) mode) {
            case DISABLE -> SSLMode.DISABLED;
            case TRUST -> SSLMode.REQUIRED;
            case VERIFY_CA -> SSLMode.VERIFY_CA;
            case VERIFY_FULL -> SSLMode.VERIFY_IDENTITY;
        };
    }

    private static TableId getSignalDataCollectionId(MariaDbConnectorConfig connectorConfig) {
        if (!Strings.isNullOrBlank(connectorConfig.getSignalingDataCollectionId())) {
            return TableId.parse(connectorConfig.getSignalingDataCollectionId());
        }
        return null;
    }
}
