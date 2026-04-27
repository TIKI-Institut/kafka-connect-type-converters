package com.tikiinstitut.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class Number19ToBigintConverterTest {

    @Test
    void shouldConvertDecimal19_0ToLong() {
        Number19ToBigintConverter converter = new Number19ToBigintConverter();
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("NUMBER");
        when(column.length()).thenReturn(OptionalInt.of(19));
        when(column.scale()).thenReturn(OptionalInt.of(0));

        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);
        ArgumentCaptor<CustomConverter.Converter> converterCaptor = ArgumentCaptor.forClass(CustomConverter.Converter.class);
        ArgumentCaptor<SchemaBuilder> schemaBuilderCaptor = ArgumentCaptor.forClass(SchemaBuilder.class);

        converter.converterFor(column, registration);

        verify(registration).register(schemaBuilderCaptor.capture(), converterCaptor.capture());

        Schema schema = schemaBuilderCaptor.getValue().build();
        assertEquals(Schema.Type.INT64, schema.type());
        assertTrue(schema.isOptional());

        CustomConverter.Converter valueConverter = converterCaptor.getValue();
        
        // Normal conversion
        assertEquals(12345L, valueConverter.convert(new BigDecimal("12345")));
        
        // Large value within long range
        assertEquals(Long.MAX_VALUE, valueConverter.convert(new BigDecimal(String.valueOf(Long.MAX_VALUE))));
        
        // Overflow case - should ignore overflow as per requirement (longValue() truncates)
        BigDecimal overflowValue = new BigDecimal(String.valueOf(Long.MAX_VALUE)).add(BigDecimal.ONE);
        assertEquals(Long.MIN_VALUE, valueConverter.convert(overflowValue));
        
        // Null handling
        assertNull(valueConverter.convert(null));
    }

    @Test
    void shouldNotRegisterForOtherTypes() {
        Number19ToBigintConverter converter = new Number19ToBigintConverter();
        CustomConverter.ConverterRegistration<SchemaBuilder> registration = mock(CustomConverter.ConverterRegistration.class);

        // Wrong type name
        RelationalColumn column1 = mock(RelationalColumn.class);
        when(column1.typeName()).thenReturn("VARCHAR");
        converter.converterFor(column1, registration);
        
        // Wrong precision
        RelationalColumn column2 = mock(RelationalColumn.class);
        when(column2.typeName()).thenReturn("NUMBER");
        when(column2.length()).thenReturn(OptionalInt.of(10));
        when(column2.scale()).thenReturn(OptionalInt.of(0));
        converter.converterFor(column2, registration);

        // Wrong scale
        RelationalColumn column3 = mock(RelationalColumn.class);
        when(column3.typeName()).thenReturn("NUMBER");
        when(column3.length()).thenReturn(OptionalInt.of(19));
        when(column3.scale()).thenReturn(OptionalInt.of(2));
        converter.converterFor(column3, registration);

        verify(registration, never()).register(any(), any());
    }
}
