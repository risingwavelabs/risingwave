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
    private static final String EPOCH_DAY = "1970-01-01";

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
            schemaBuilder = SchemaBuilder.string().name("risingwave.cdc.date.string");
            converter = this::convertDate;
        }
        if (schemaBuilder != null) {
            registration.register(schemaBuilder, converter);
        }
    }

    private String convertDate(Object input) {
        if (input == null) {
            return EPOCH_DAY;
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
