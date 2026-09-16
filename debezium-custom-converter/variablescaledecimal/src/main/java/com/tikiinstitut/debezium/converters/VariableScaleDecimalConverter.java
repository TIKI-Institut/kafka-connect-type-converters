package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            if (!(value instanceof Number)) {
                if (value != null)
                    LOGGER.warn("{} cannot be converted because it is not of type Number ({})", columnName, value.getClass().getName());
                return value;
            }
            return extractFunction.apply((Number) value);
        };
    }
}
