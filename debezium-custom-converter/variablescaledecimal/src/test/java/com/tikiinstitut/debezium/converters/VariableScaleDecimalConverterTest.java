package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.OptionalInt;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class VariableScaleDecimalConverterTest {

    private static final String INTEGER_COLUMNS_REGEX = "(^.*_ID$)|(^IDENT$)";

    @Test
    void shouldConvertNumberWithoutScaleToDouble() {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.name()).thenReturn("NOMINALVALUE");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(true);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<CustomConverter.Converter> converterCaptor = ArgumentCaptor.forClass(CustomConverter.Converter.class);
        ArgumentCaptor<SchemaBuilder> schemaBuilderCaptor = ArgumentCaptor.forClass(SchemaBuilder.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);

        verify(registration).register(schemaBuilderCaptor.capture(), converterCaptor.capture());

        Schema schema = schemaBuilderCaptor.getValue().build();
        assertEquals(Schema.Type.FLOAT64, schema.type());
        assertTrue(schema.isOptional());

        CustomConverter.Converter valueConverter = converterCaptor.getValue();

        // Snapshot reads through JDBC and yields BigDecimal
        assertEquals(300.0, valueConverter.convert(new BigDecimal("300")));
        assertEquals(7622.289912, valueConverter.convert(new BigDecimal("7622.289912")));

        // LogMiner parses the redo statement and yields String
        assertEquals(300.0, valueConverter.convert("300"));
        assertEquals(7622.289912, valueConverter.convert("7622.289912"));

        // Precision beyond 2^53 is silently lost (doubleValue() rounds)
        assertEquals(9007199254740992.0, valueConverter.convert(new BigDecimal("9007199254740993")));

        // Null handling
        assertNull(valueConverter.convert(null));
    }

    @Test
    void shouldConvertMatchingColumnToLong() {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.name()).thenReturn("ORDERS_ID");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(true);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<CustomConverter.Converter> converterCaptor = ArgumentCaptor.forClass(CustomConverter.Converter.class);
        ArgumentCaptor<SchemaBuilder> schemaBuilderCaptor = ArgumentCaptor.forClass(SchemaBuilder.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);

        verify(registration).register(schemaBuilderCaptor.capture(), converterCaptor.capture());

        Schema schema = schemaBuilderCaptor.getValue().build();
        assertEquals(Schema.Type.INT64, schema.type());

        CustomConverter.Converter valueConverter = converterCaptor.getValue();

        assertEquals(4000L, valueConverter.convert(new BigDecimal("4000")));
        assertEquals(4000L, valueConverter.convert("4000"));

        // Fractional values are truncated towards zero (longValue())
        assertEquals(1L, valueConverter.convert(new BigDecimal("1.9")));

        assertNull(valueConverter.convert(null));
    }

    @Test
    void shouldConvertFloatToDouble() {
        // FLOAT(*), DOUBLE PRECISION and REAL types all have typeName FLOAT
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("FLOAT");
        when(column.name()).thenReturn("CYCLE_TIME");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(true);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<SchemaBuilder> schemaBuilderCaptor = ArgumentCaptor.forClass(SchemaBuilder.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);

        verify(registration).register(schemaBuilderCaptor.capture(), any());

        assertEquals(Schema.Type.FLOAT64, schemaBuilderCaptor.getValue().build().type());
    }

    @Test
    void shouldConvertHexToRawFunction() {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.name()).thenReturn("IDENT");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(true);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<CustomConverter.Converter> converterCaptor = ArgumentCaptor.forClass(CustomConverter.Converter.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);
        verify(registration).register(any(), converterCaptor.capture());

        CustomConverter.Converter valueConverter = converterCaptor.getValue();

        byte[] numberBytes = new oracle.sql.NUMBER(12345).getBytes();
        StringBuilder hexString = new StringBuilder();
        for (byte b : numberBytes) {
            hexString.append(String.format("%02x", b));
        }

        String hexToRawCall = VariableScaleDecimalConverter.OracleHexToRawHelper.HEXTORAW_FUNCTION_START +
                hexString +
                VariableScaleDecimalConverter.OracleHexToRawHelper.HEXTORAW_FUNCTION_END;

        assertEquals(12345L, valueConverter.convert(hexToRawCall));
    }

    @Test
    void shouldReturnNullForUnconvertibleValues() {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.name()).thenReturn("NOMINALVALUE");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(true);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<CustomConverter.Converter> converterCaptor = ArgumentCaptor.forClass(CustomConverter.Converter.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);
        verify(registration).register(any(), converterCaptor.capture());

        CustomConverter.Converter valueConverter = converterCaptor.getValue();

        // Returning the value unconverted would not match the registered schema
        assertNull(valueConverter.convert("not a number"));
        assertNull(valueConverter.convert(new Object()));
    }

    @Test
    void shouldHonorNonOptionalColumn() {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.name()).thenReturn("NOMINALVALUE");
        when(column.scale()).thenReturn(OptionalInt.empty());
        when(column.isOptional()).thenReturn(false);

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<SchemaBuilder> schemaBuilderCaptor = ArgumentCaptor.forClass(SchemaBuilder.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);

        verify(registration).register(schemaBuilderCaptor.capture(), any());

        Schema schema = schemaBuilderCaptor.getValue().build();
        assertEquals(Schema.Type.FLOAT64, schema.type());
        assertFalse(schema.isOptional());
    }

    @Test
    void shouldNotRegisterForColumnsWithScale() {
        // Only types converted to VariableScaleDecimal have their scale unset, so columns
        // handled elsewhere must be left alone
        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);

        // NUMBER(19,0), handled by Number19ToBigintConverter
        RelationalColumn column1 = mock(RelationalColumn.class);
        when(column1.scale()).thenReturn(OptionalInt.of(0));
        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column1, registration);

        // NUMBER(*,0)
        RelationalColumn column2 = mock(RelationalColumn.class);
        when(column2.scale()).thenReturn(OptionalInt.of(0));
        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column2, registration);

        // NUMBER(19,3), already mapped to Decimal
        RelationalColumn column3 = mock(RelationalColumn.class);
        when(column3.scale()).thenReturn(OptionalInt.of(3));
        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column3, registration);

        verify(registration, never()).register(any(), any());
    }

    @Test
    void shouldNotRegisterForOtherTypes() {
        // VARCHAR2, DATE and the like also have no scale
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("VARCHAR2");
        when(column.name()).thenReturn("DESCRIPTION");
        when(column.scale()).thenReturn(OptionalInt.empty());

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);

        newConverter(INTEGER_COLUMNS_REGEX).converterFor(column, registration);

        verify(registration, never()).register(any(), any());
    }

    private static VariableScaleDecimalConverter newConverter(String integerColumnsRegex) {
        VariableScaleDecimalConverter converter = new VariableScaleDecimalConverter();
        Properties props = new Properties();
        props.setProperty("integerColumnsRegex", integerColumnsRegex);
        converter.configure(props);
        return converter;
    }
}
