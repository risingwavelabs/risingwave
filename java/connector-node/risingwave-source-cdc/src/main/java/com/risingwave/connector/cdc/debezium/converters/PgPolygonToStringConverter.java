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
import java.sql.Types;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** Converts PostgreSQL polygon values and polygon arrays into plain strings. */
public class PgPolygonToStringConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {

    @Override
    public void configure(Properties props) {}

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if ("polygon".equalsIgnoreCase(column.typeName())) {
            registration.register(
                    SchemaBuilder.string().optional(), PgTextConversionUtils::convertScalar);
            return;
        }

        // `polygon[]` is a built-in type, so its OID sits below PG_FIRST_USER_OID and
        // PgCompositeToStringConverter's user-OID rule never sees it. Match it by name instead.
        if (column.jdbcType() == Types.ARRAY
                && PgTextConversionUtils.isPostgresArrayOf(column, "polygon")) {
            var elementSchema = SchemaBuilder.string().optional().build();
            registration.register(
                    SchemaBuilder.array(elementSchema).optional(),
                    PgTextConversionUtils::convertArray);
        }
    }
}
