package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import oracle.sql.NUMBER;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Types;
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
        if (Types.NUMERIC == column.jdbcType()
                && column.length().orElse(-1) == 19
                && column.scale().orElse(-1) == 0) {

            SchemaBuilder schemaBuilder = SchemaBuilder.int64();
            if (column.isOptional()) {
                schemaBuilder.optional();
            }

            registration.register(schemaBuilder, value -> {
                if (value == null) {
                    return null;
                }
                if (value instanceof BigDecimal) {
                    return ((BigDecimal) value).longValue();
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                if (value instanceof String) {
                    if (OracleHexToRawHelper.isHexToRawFunctionCall((String) value)) {
                        return new BigDecimal(new NUMBER(OracleHexToRawHelper.convertHexToRawFunctionToByteArray((String) value)).stringValue()).longValue();
                    }

                    return new BigDecimal((String) value).longValue();
                }
                LOGGER.warn("Unexpected value type for column {}: {} (value: {})", column.name(), value.getClass().getName(), value);
                return null;
            });
        }
    }

    /**
     * This Helper Class has extracted source code from the official Debezium project at
     * [[io.debezium.connector.oracle.OracleValueConverters]
     * to avoid a direct dependency.
     * As otherwise binary incompatible changes in these methods would break our SPI implementation.
     */
    static class OracleHexToRawHelper {
        /*
         * Copyright Debezium Authors.
         *
         * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
         */
        public static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
        public static final String HEXTORAW_FUNCTION_END = "')";

        public static boolean isHexToRawFunctionCall(String value) {
            return value != null && value.startsWith(HEXTORAW_FUNCTION_START) && value.endsWith(HEXTORAW_FUNCTION_END);
        }

        public static String getHexToRawHexString(String hexToRawValue) {
            if (isHexToRawFunctionCall(hexToRawValue)) {
                return hexToRawValue.substring(10, hexToRawValue.length() - 2);
            }
            return hexToRawValue;
        }

        private static byte[] convertHexToRawFunctionToByteArray(String value) {
            final String rawValue = getHexToRawHexString(value);
            int len = rawValue.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(rawValue.charAt(i), 16) << 4)
                        + Character.digit(rawValue.charAt(i + 1), 16));
            }
            return data;
        }
    }

}
