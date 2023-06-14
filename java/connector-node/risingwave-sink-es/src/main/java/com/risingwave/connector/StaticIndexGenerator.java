package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkRow;

final class StaticIndexGenerator extends IndexGeneratorBase {

    public StaticIndexGenerator(String index) {
        super(index);
    }

    public String generate(SinkRow row) {
        return index;
    }
}
