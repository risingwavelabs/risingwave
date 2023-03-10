// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector.cdc.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.time.Date;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.kafka.connect.data.SchemaBuilder;

/** RisingWave assumes DATE type in JSON is a string in "yyyy-MM-dd" pattern */
public class DatetimeTypeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;

    @Override
    public void configure(Properties props) {
        dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }

    @Override
    public void converterFor(
            RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String sqlType = column.typeName().toUpperCase();
        SchemaBuilder schemaBuilder = null;
        Converter converter = null;
        if ("DATE".equals(sqlType)) {
            schemaBuilder = SchemaBuilder.string().name("rw.cdc.date.string");
            converter = this::convertDate;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }
    }

    private String convertDate(Object input) {
        if (input == null) {
            return null;
        }
        var epochDay = Date.toEpochDay(input, null);
        LocalDate date = LocalDate.ofEpochDay(epochDay);
        return dateFormatter.format(date);
    }

    public static void main(String[] args) {
        var converter = new DatetimeTypeConverter();
        var d1 = LocalDate.of(1988, 5, 4);
        var d2 = java.sql.Date.valueOf("1960-01-01");
        Integer d3 = 8989;

        System.out.println(converter.convertDate(null));
        System.out.println(converter.convertDate(d1));
        System.out.println(converter.convertDate(d2));
        System.out.println(converter.convertDate(d3));
    }
}
