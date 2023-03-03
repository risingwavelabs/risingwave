package com.risingwave.sourcenode.core;

import com.risingwave.connector.api.source.SourceHandler;
import com.risingwave.connector.api.source.SourceTypeE;
import com.risingwave.sourcenode.mysql.MySqlSourceConfig;
import com.risingwave.sourcenode.postgres.PostgresSourceConfig;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SourceHandlerFactory {
    static final Logger LOG = LoggerFactory.getLogger(SourceHandlerFactory.class);

    public static SourceHandler createSourceHandler(
            SourceTypeE type, long sourceId, String startOffset, Map<String, String> userProps) {
        switch (type) {
            case MYSQL:
                return DefaultSourceHandler.newWithConfig(
                        new MySqlSourceConfig(sourceId, startOffset, userProps));
            case POSTGRES:
                return DefaultSourceHandler.newWithConfig(
                        new PostgresSourceConfig(sourceId, startOffset, userProps));
            default:
                LOG.warn("unknown source type: {}", type);
                return null;
        }
    }
}
