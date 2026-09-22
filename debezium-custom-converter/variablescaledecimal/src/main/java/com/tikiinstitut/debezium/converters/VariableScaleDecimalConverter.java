package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Properties;
import java.util.function.Function;

public class VariableScaleDecimalConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VariableScaleDecimalConverter.class);
    private String integerColumnsRegex = null;
    private String decimalColumnsRegex = null;
    private int decimalPrecision;
    private int decimalScale;

    @Override
    public void configure(Properties props) {
        this.integerColumnsRegex = (String) props.getOrDefault("integerColumnsRegex", "^$");
        this.decimalColumnsRegex = (String) props.getOrDefault("decimalColumnsRegex", "^$");
        this.decimalPrecision = Integer.parseInt((String) props.getOrDefault("decimalPrecision", "19"));
        this.decimalScale = Integer.parseInt((String) props.getOrDefault("decimalScale", "6"));
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        // All types that are converted to VariableScaleDecimal have their scale attribute unset
        if(column.scale().isPresent())
            return;

        String columnTypeName = column.typeName();
        String columnName = column.name();

        // Only NUMBER and FLOAT become VariableScaleDecimal. We don't
        // care about other types without a scale (i.e. VARCHAR2, DATE)
        if (!columnTypeName.equals("NUMBER") && !columnTypeName.equals("FLOAT")) {
            return;
        }

        SchemaBuilder schemaBuilder;
        Converter converterFunction;

        // integerColumnsRegex wins over decimalColumnsRegex when a name matches both
        if (columnTypeName.equals("NUMBER") && columnName.matches(this.integerColumnsRegex)) {
            schemaBuilder = SchemaBuilder.int64();
            converterFunction = getConverterFunction(columnName, Number::longValue);
        }
        // FLOAT is binary floating point, so it is never mapped to a decimal
        else if (columnTypeName.equals("NUMBER") && columnName.matches(this.decimalColumnsRegex)) {
            schemaBuilder = Decimal.builder(this.decimalScale)
                    .parameter("connect.decimal.precision", String.valueOf(this.decimalPrecision));
            int scale = this.decimalScale;
            converterFunction = getConverterFunction(columnName,
                    number -> toBigDecimal(number).setScale(scale, RoundingMode.HALF_UP));
        }
        else {
            // FLOAT(*), DOUBLE PRECISION and REAL types all have typeName FLOAT
            schemaBuilder = SchemaBuilder.float64();
            converterFunction = getConverterFunction(columnName, Number::doubleValue);
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

    private static BigDecimal toBigDecimal(Number number) {
        return number instanceof BigDecimal decimal ? decimal : new BigDecimal(number.toString());
    }

    // The snapshot reads through JDBC and yields BigDecimal, LogMiner parses the redo statement
    // and yields String, sometimes wrapped in HEXTORAW('..')
    private static Number asNumber(String columnName, Object value) {
        if (value instanceof Number number) {
            return number;
        }
        if (value instanceof String text) {
            try {
                return OracleHexToRawHelper.toBigDecimal(text);
            } catch (NumberFormatException e) {
                LOGGER.warn("{} cannot be converted because '{}' is not a number", columnName, text);
                return null;
            }
        }
        LOGGER.warn("{} cannot be converted because it is not of type Number or String ({})", columnName, value.getClass().getName());
        return null;
    }
}
