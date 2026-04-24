/*
 * Copyright 2026 RisingWave Labs
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

package com.risingwave.connector.cdc.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Converts PostgreSQL composite values into plain strings. */
public class PgCompositeToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private boolean debug;

    @Override
    public void configure(Properties props) {
        debug = Boolean.parseBoolean(props.getProperty("debug", "false"));
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (debug) {
            System.err.printf(
                    "PgCompositeToStringConverter field=%s.%s jdbcType=%d nativeType=%d typeName=%s typeExpression=%s optional=%s%n",
                    column.dataCollection(),
                    column.name(),
                    column.jdbcType(),
                    column.nativeType(),
                    column.typeName(),
                    column.typeExpression(),
                    column.isOptional());
        }

        if (!matches(column)) {
            return;
        }

        var schemaBuilder = SchemaBuilder.string();
        if (column.isOptional()) {
            schemaBuilder.optional();
        }

        registration.register(schemaBuilder, this::convertValue);
    }

    private boolean matches(RelationalColumn column) {
        return column.jdbcType() == Types.STRUCT;
    }

    private String convertValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value instanceof ByteBuffer buffer) {
            var readOnly = buffer.asReadOnlyBuffer();
            var bytes = new byte[readOnly.remaining()];
            readOnly.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }
}
