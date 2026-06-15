package com.tikiinstitut.debezium.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class Number19ToBigintConverterIT extends AbstractOracleConnectorTest {

    @Test
    public void shouldConvertBigDecimalValues() throws Exception {
        final Properties props = createDebeziumProperties(testInfo.getDisplayName());

        startDebeziumEngine(props);

        awaitRunningDebeziumEngine();

        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO TEST." + testInfo.getDisplayName() + " (ID, VAL_DECIMAL, VAL_NOT_NULL) VALUES (1, 1234567890123456789, 111)");
        }

        SourceRecord record = consumedRecords.poll(20, TimeUnit.SECONDS);
        assertNotNull(record, "Should have captured a record");

        Struct value = (Struct) record.value();
        Struct after = value.getStruct("after");
        assertNotNull(after, "Event should have 'after' state");

        Object valDecimal = after.get("VAL_DECIMAL");
        assertNotNull(valDecimal);
        assertEquals(1234567890123456789L, valDecimal);
        assertEquals(Schema.INT64_SCHEMA.type(), after.schema().field("VAL_DECIMAL").schema().type());
        assertTrue(after.schema().field("VAL_DECIMAL").schema().isOptional());

        Object valNotNull = after.get("VAL_NOT_NULL");
        assertEquals(111L, valNotNull);
        assertFalse(after.schema().field("VAL_NOT_NULL").schema().isOptional());
    }


}
