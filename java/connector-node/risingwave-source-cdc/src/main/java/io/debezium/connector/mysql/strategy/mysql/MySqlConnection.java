/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.mysql.strategy.mysql;

import com.mysql.cj.CharsetMapping;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.MySqlFieldReader;
import io.debezium.connector.mysql.strategy.AbstractConnectorConnection;
import java.sql.SQLException;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AbstractConnectorConnection} to be used with MySQL.
 *
 * @author Jiri Pechanec, Randell Hauch, Chris Cranford
 */
public class MySqlConnection extends AbstractConnectorConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnection.class);

    public MySqlConnection(
            MySqlConnectionConfiguration connectionConfig, MySqlFieldReader fieldReader) {
        super(connectionConfig, fieldReader);
    }

    @Override
    public boolean isGtidModeEnabled() {
        try {
            return queryAndMap(
                    "SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'",
                    rs -> {
                        if (rs.next()) {
                            return "ON".equalsIgnoreCase(rs.getString(2));
                        }
                        return false;
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    @Override
    public GtidSet knownGtidSet() {
        try {
            /* patch code */
            // MySQL 8.4 removed/deprecated "SHOW MASTER STATUS" for binlog metadata.
            // From 8.4+, use "SHOW BINARY LOG STATUS" to read GTID set; for older versions keep
            // using
            // "SHOW MASTER STATUS".
            final String sql = isMySql84OrLater() ? "SHOW BINARY LOG STATUS" : "SHOW MASTER STATUS";
            LOGGER.debug("Using SQL command: {} for GTID set query", sql);
            /* patch code */
            return queryAndMap(
                    sql,
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                            return new MySqlGtidSet(
                                    rs.getString(
                                            5)); // GTID set, may be null, blank, or contain a GTID
                            // set
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while looking at GTID mode: ", e);
        }
    }

    private boolean isMySql84OrLater() {
        /* patch code */
        // Version guard: detect server version via SELECT VERSION() and choose statement
        // accordingly.
        // Reason: MySQL 8.4+ expects SHOW BINARY LOG STATUS, while <8.4 still uses SHOW MASTER
        // STATUS.
        try {
            String version =
                    queryAndMap("SELECT VERSION()", rs -> rs.next() ? rs.getString(1) : null);
            if (version == null) {
                return false;
            }
            String[] parts = version.split("\\.");
            int major = parts.length > 0 ? Integer.parseInt(parts[0]) : 0;
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return major > 8 || (major == 8 && minor >= 4);
        } catch (SQLException | NumberFormatException ex) {
            LOGGER.warn("Failed to parse MySQL version, assume < 8.4", ex);
            return false;
        }
        /* patch code */
    }

    @Override
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return prepareQueryAndMap(
                    "SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new MySqlGtidSet(rs.getString(1));
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Unexpected error while executing GTID_SUBTRACT: ", e);
        }
    }

    @Override
    public GtidSet purgedGtidSet() {
        try {
            return queryAndMap(
                    "SELECT @@global.gtid_purged",
                    rs -> {
                        if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                            return new MySqlGtidSet(
                                    rs.getString(
                                            1)); // GTID set, may be null, blank, or contain a GTID
                            // set
                        }
                        return new MySqlGtidSet("");
                    });
        } catch (SQLException e) {
            throw new DebeziumException(
                    "Unexpected error while looking at gtid_purged variable: ", e);
        }
    }

    @Override
    public GtidSet filterGtidSet(
            Predicate<String> gtidSourceFilter,
            String offsetGtids,
            GtidSet availableServerGtidSet,
            GtidSet purgedServerGtidSet) {
        String gtidStr = offsetGtids;
        if (gtidStr == null) {
            return null;
        }
        LOGGER.info("Attempting to generate a filtered GTID set");
        LOGGER.info("GTID set from previous recorded offset: {}", gtidStr);
        GtidSet filteredGtidSet = new MySqlGtidSet(gtidStr);
        if (gtidSourceFilter != null) {
            filteredGtidSet = filteredGtidSet.retainAll(gtidSourceFilter);
            LOGGER.info(
                    "GTID set after applying GTID source includes/excludes to previous recorded offset: {}",
                    filteredGtidSet);
        }
        LOGGER.info("GTID set available on server: {}", availableServerGtidSet);

        final GtidSet knownGtidSet = filteredGtidSet;
        LOGGER.info("Using first available positions for new GTID channels");
        final GtidSet relevantAvailableServerGtidSet =
                (gtidSourceFilter != null)
                        ? availableServerGtidSet.retainAll(gtidSourceFilter)
                        : availableServerGtidSet;
        LOGGER.info("Relevant GTID set available on server: {}", relevantAvailableServerGtidSet);

        GtidSet mergedGtidSet =
                relevantAvailableServerGtidSet
                        .retainAll(
                                uuid -> ((MySqlGtidSet) knownGtidSet).forServerWithId(uuid) != null)
                        .with(purgedServerGtidSet)
                        .with(filteredGtidSet);

        LOGGER.info("Final merged GTID set to use when connecting to MySQL: {}", mergedGtidSet);
        return mergedGtidSet;
    }

    @Override
    public boolean isMariaDb() {
        return false;
    }

    @Override
    protected GtidSet createGtidSet(String gtids) {
        return new MySqlGtidSet(gtids);
    }

    public static String getJavaEncodingForCharSet(String charSetName) {
        return CharsetMappingWrapper.getJavaEncodingForMysqlCharSet(charSetName);
    }

    /** Helper to gain access to protected method */
    private static final class CharsetMappingWrapper extends CharsetMapping {
        static String getJavaEncodingForMysqlCharSet(String charSetName) {
            return CharsetMapping.getStaticJavaEncodingForMysqlCharset(charSetName);
        }
    }
}
