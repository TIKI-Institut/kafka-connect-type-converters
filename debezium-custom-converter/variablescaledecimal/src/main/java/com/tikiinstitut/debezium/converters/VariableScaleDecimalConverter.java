package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import oracle.sql.NUMBER;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.function.Function;

public class VariableScaleDecimalConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VariableScaleDecimalConverter.class);
    private String integerColumnsRegex = null;

    @Override
    public void configure(Properties props) {
        this.integerColumnsRegex = (String) props.getOrDefault("integerColumnsRegex", "^$");
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        // All types that are converted to VariableScaleDecimal have their scale attribute unset
        if(column.scale().isPresent())
            return;

        String columnTypeName = column.typeName();
        String columnName = column.name();

        SchemaBuilder schemaBuilder;
        Converter converterFunction;

        if (columnTypeName.equals("NUMBER") && columnName.matches(this.integerColumnsRegex)) {
            schemaBuilder = SchemaBuilder.int64();
            converterFunction = getConverterFunction(columnName, Number::longValue);
        }
        else if (columnTypeName.equals("NUMBER") || columnTypeName.equals("FLOAT")) {
            // FLOAT(*), DOUBLE PRECISION and REAL types all have typeName FLOAT
            schemaBuilder = SchemaBuilder.float64();
            converterFunction = getConverterFunction(columnName, Number::doubleValue);
        }
        else {
            LOGGER.warn("{} does not have a scale but does not match any type for conversion", columnName);
            return;
        }

        if (column.isOptional()){
            schemaBuilder.optional();
        }
        registration.register(schemaBuilder, converterFunction);
    }

    private static Converter getConverterFunction(String columnName, Function<Number, Object> extractFunction){
        return value -> {
            if (value == null) {
                return null;
            }
            Number number = asNumber(columnName, value);
            // Returning the unconverted value would not match the INT64/FLOAT64 schema and be nulled anyway
            return number == null ? null : extractFunction.apply(number);
        };
    }

    // The snapshot reads through JDBC and yields BigDecimal, LogMiner parses the redo statement
    // and yields String, sometimes wrapped in HEXTORAW('..')
    private static Number asNumber(String columnName, Object value) {
        if (value instanceof Number number) {
            return number;
        }
        if (value instanceof String text) {
            try {
                if (OracleHexToRawHelper.isHexToRawFunctionCall(text)) {
                    return new BigDecimal(new NUMBER(OracleHexToRawHelper.convertHexToRawFunctionToByteArray(text)).stringValue());
                }
                return new BigDecimal(text);
            } catch (NumberFormatException e) {
                LOGGER.warn("{} cannot be converted because '{}' is not a number", columnName, text);
                return null;
            }
        }
        LOGGER.warn("{} cannot be converted because it is not of type Number or String ({})", columnName, value.getClass().getName());
        return null;
    }

    /**
     * This Helper Class has extracted source code from the official Debezium project at
     * [[io.debezium.connector.oracle.OracleValueConverters]
     * to avoid a direct dependency.
     * As otherwise binary incompatible changes in these methods would break our SPI implementation.
     * Note: duplicated from the decimal-to-bigint module; converters are deployed as standalone jars.
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
