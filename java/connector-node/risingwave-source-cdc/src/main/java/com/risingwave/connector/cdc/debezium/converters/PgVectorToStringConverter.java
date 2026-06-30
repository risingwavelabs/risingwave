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
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Converts PostgreSQL pgvector values into plain strings.
 *
 * <p>Debezium's default handler maps the {@code vector} type to {@code
 * io.debezium.data.DoubleVector}, a {@code Schema.Type.ARRAY} of {@code FLOAT64}. That schema
 * cannot accept Debezium's unchanged-TOAST placeholder (a Java {@code String}), so a single UPDATE
 * that leaves a TOAST'd vector column unchanged crashes {@code Struct.put} and kills the streaming
 * coordinator.
 *
 * <p>This converter bypasses that path by registering an OPTIONAL STRING schema for the {@code
 * vector} column and passing the raw value through unchanged — either the pgvector text form {@code
 * "[a,b,...]"} or the placeholder {@code __debezium_unavailable_value}. The downstream RisingWave
 * parser handles both cases and the TOAST replacement logic in the materialize executor takes over
 * from there.
 */
public class PgVectorToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    // Must match `DEBEZIUM_UNAVAILABLE_VALUE` on the Rust side
    // (src/common/src/types/mod.rs). When a vector column is TOAST'd and the
    // UPDATE does not touch it, Debezium hands us a bare `java.lang.Object`
    // singleton instead of the usual placeholder string, because the default
    // schema for vector is ARRAY (a String placeholder would fail schema
    // validation). We translate that sentinel back to the canonical string so
    // the downstream RisingWave parser + materialize executor handle it
    // through the same path as varchar/jsonb/bytea TOAST columns.
    private static final String UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        // pgvector ships the `vector` type as an extension; the OID is assigned dynamically at
        // CREATE EXTENSION time, so we match on the type name instead.
        if ("vector".equalsIgnoreCase(column.typeName())) {
            registration.register(SchemaBuilder.string().optional(), this::convertVector);
        }
    }

    private String convertVector(Object value) {
        if (value == null) {
            return null;
        }
        // Debezium's unchanged-TOAST sentinel for non-STRING schema columns is a
        // bare `java.lang.Object` singleton. Normal vector values arrive as a
        // concrete subclass (String / byte[] / PGobject / ...), so a value whose
        // runtime class is exactly Object can only be the sentinel.
        if (value.getClass() == Object.class) {
            return UNAVAILABLE_VALUE_PLACEHOLDER;
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
