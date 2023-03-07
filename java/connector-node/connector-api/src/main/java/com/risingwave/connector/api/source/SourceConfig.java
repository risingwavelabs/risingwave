package com.risingwave.connector.api.source;

import java.util.Properties;

public interface SourceConfig {
    long getId();

    String getSourceName();

    SourceTypeE getSourceType();

    Properties getProperties();
}
