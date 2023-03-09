package com.risingwave.sourcenode.postgres;

import com.risingwave.connector.api.source.ConnectorConfig;
import com.risingwave.connector.api.source.SourceConfig;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.connector.cdc.debezium.internal.ConfigurableOffsetBackingStore;
import com.risingwave.sourcenode.common.DebeziumCdcUtils;
import io.debezium.heartbeat.Heartbeat;
import java.time.Duration;
import java.util.Properties;

/** Postgres Source Config */
public class PostgresSourceConfig implements SourceConfig {
    static final String DB_SERVER_NAME_PREFIX = "RW_CDC_";
    private final Properties props = DebeziumCdcUtils.createCommonConfig();
    private final long id;
    private final String sourceName;
    private static final long DEFAULT_HEARTBEAT_MS = Duration.ofMinutes(5).toMillis();

    public PostgresSourceConfig(long sourceId, String startOffset, ConnectorConfig userProps) {
        id = sourceId;
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty(
                "offset.storage", ConfigurableOffsetBackingStore.class.getCanonicalName());
        props.setProperty(
                "database.history", "io.debezium.relational.history.MemoryDatabaseHistory");

        // if offset is specified, we will continue reading changes from the specified offset
        if (null != startOffset && !startOffset.isBlank()) {
            props.setProperty("snapshot.mode", "never");
            props.setProperty(ConfigurableOffsetBackingStore.OFFSET_STATE_VALUE, startOffset);
        }

        String dbName = userProps.getNonNull(ConnectorConfig.DB_NAME);
        String schema =
                userProps.getOrDefault(
                        ConnectorConfig.PG_SCHEMA_NAME, ConnectorConfig.PG_DEFAULT_SCHEMA);
        String table = userProps.getNonNull(ConnectorConfig.TABLE_NAME);

        // Begin of connector configs
        props.setProperty("database.hostname", userProps.get(ConnectorConfig.HOST));
        props.setProperty("database.port", userProps.get(ConnectorConfig.PORT));
        props.setProperty("database.user", userProps.get(ConnectorConfig.USER));
        props.setProperty("database.password", userProps.get(ConnectorConfig.PASSWORD));
        props.setProperty("database.dbname", dbName);
        // The name of the PostgreSQL logical decoding plug-in installed on the PostgreSQL server.
        // Supported values are decoderbufs, and pgoutput.
        // The wal2json plug-in is deprecated and scheduled for removal.
        // see
        // https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-property-plugin-name
        props.setProperty("plugin.name", "pgoutput");

        // The name of the PostgreSQL logical decoding slot that was created for streaming changes
        // from a particular plug-in for a particular database/schema. The server uses this slot to
        // stream events
        // to the Debezium connector that you are configuring.
        // Slot names must conform to PostgreSQL replication slot naming rules,
        // which state: "Each replication slot has a name, which can contain lower-case letters,
        // numbers, and the underscore character."
        props.setProperty("slot.name", userProps.get(ConnectorConfig.PG_SLOT_NAME));

        // Sending heartbeat messages enables the connector to send the latest retrieved LSN to the
        // database, which allows the database to reclaim disk space being
        // used by no longer needed WAL files.
        // https://debezium.io/documentation/reference/1.9/connectors/postgresql.html#postgresql-property-heartbeat-interval-ms
        props.setProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_MS));
        props.setProperty(
                Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(),
                Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString());

        String tableFilter = schema + "." + table;
        props.setProperty("table.include.list", tableFilter);
        props.setProperty("database.server.name", DB_SERVER_NAME_PREFIX + tableFilter);

        // host:port:database.schema.table
        sourceName =
                userProps.getNonNull(ConnectorConfig.HOST)
                        + ":"
                        + userProps.getNonNull(ConnectorConfig.PORT)
                        + ":"
                        + dbName
                        + "."
                        + schema
                        + "."
                        + table;
        props.setProperty("name", sourceName);

        // pass through debezium properties if any
        var dbzProperties = ConnectorConfig.extractDebeziumProperties(userProps);
        props.putAll(dbzProperties);
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }

    @Override
    public SourceTypeE getSourceType() {
        return SourceTypeE.POSTGRES;
    }

    @Override
    public Properties getProperties() {
        return props;
    }
}
