package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkRow;
import java.util.Iterator;

public interface Deserializer {
    Iterator<SinkRow> deserialize(Object payload);
}
