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

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        // pgvector ships the `vector` type as an extension; the OID is assigned dynamically at
        // CREATE EXTENSION time, so we match on the type name instead.

        if ("vector".equalsIgnoreCase(column.typeName())) {
            registration.register(
                    SchemaBuilder.string().optional(), PgTextConversionUtils::convertScalar);
        }
    }
}
