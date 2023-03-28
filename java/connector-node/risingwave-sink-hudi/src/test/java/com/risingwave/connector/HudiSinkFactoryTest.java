package com.risingwave.connector;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Catalog;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class HudiSinkFactoryTest {

    @Test
    public void simple() {
        HudiSinkFactory factory = new HudiSinkFactory();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put(HudiSinkFactory.BASE_PATH_PROP, "file:///tmp/hoodie/sample-table");
        factory.validate(
                TableSchema.getMockTableSchema(), tableProperties, Catalog.SinkType.UPSERT);
    }
}
