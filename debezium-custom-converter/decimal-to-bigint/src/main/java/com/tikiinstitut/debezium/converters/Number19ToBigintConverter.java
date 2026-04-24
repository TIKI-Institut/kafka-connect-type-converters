package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Debezium custom converter that converts NUMBER(19,0) columns to INT64 (bigint).
 * Note: Any potential overflow when converting from BigDecimal to long is ignored.
 */
public class Number19ToBigintConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Number19ToBigintConverter.class);

    @Override
    public void configure(Properties props) {
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if ("NUMBER".equalsIgnoreCase(column.typeName())
                && column.length().orElse(-1) == 19
                && column.scale().orElse(-1) == 0) {

            registration.register(SchemaBuilder.int64().optional(), value -> {
                if (value == null) {
                    return null;
                }
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).longValue();
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                LOGGER.warn("Unexpected value type for column {}: {}", column.name(), value.getClass().getName());
                return null;
            });
        }
    }
}
