package com.tikiinstitut.debezium.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class VariableScaleDecimalConverterIT extends AbstractOracleConnectorTest {

    @Test
    public void shouldConvertVariableScaleDecimal() throws Exception {

        final Properties props = createDebeziumProperties(testInfo.getDisplayName());
        props.setProperty("converters", "variablescaledecimal");
        props.setProperty("variablescaledecimal.type", "com.tikiinstitut.debezium.converters.VariableScaleDecimalConverter");
        props.setProperty("variablescaledecimal.integerColumnsRegex", "(^.*_ID$)|(^IDENT$)|(^.*_COUNT)");

        startDebeziumEngine(props);
        awaitRunningDebeziumEngine();

        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("""
                    INSERT INTO %s (ID, FLOAT_COL, NUMBER_COL, NUMBER_COL_ID, IDENT, NUMBER_COL_COUNT, DOUBLE_PRECISION_COL, REAL_COL) 
                    VALUES (1, 20.0, 300.0, 4000, 50000.0, 600000, 7622.289912, 89874.23986)""", getTableFQN(testInfo.getDisplayName())));
        }

        AtomicReference<SourceRecord> atomic_record = new AtomicReference<>();
        Awaitility.await().atMost(2, TimeUnit.MINUTES).until(() -> {
            atomic_record.set(consumedRecords.poll(5, TimeUnit.SECONDS));
            return atomic_record.get() != null;
        });

        SourceRecord record = atomic_record.get();
        assertNotNull(record, "Should have captured a record");

        Struct value = (Struct) record.value();
        Struct after = value.getStruct("after");
        assertNotNull(after, "Event should have 'after' state");

        Object valFloat = after.get("FLOAT_COL");
        assertNotNull(valFloat);
        assertEquals(20.0, valFloat);
        assertEquals(Schema.FLOAT64_SCHEMA.type(), after.schema().field("FLOAT_COL").schema().type());

        Object valNumber = after.get("NUMBER_COL");
        assertNotNull(valNumber);
        assertEquals(300.0, valNumber);
        assertEquals(Schema.FLOAT64_SCHEMA.type(), after.schema().field("NUMBER_COL").schema().type());

        Object valNumberId = after.get("NUMBER_COL_ID");
        assertNotNull(valNumberId);
        assertEquals(4000L, valNumberId);
        assertEquals(Schema.INT64_SCHEMA.type(), after.schema().field("NUMBER_COL_ID").schema().type());

        Object valIdent = after.get("IDENT");
        assertNotNull(valIdent);
        assertEquals(50000L, valIdent);
        assertEquals(Schema.INT64_SCHEMA.type(), after.schema().field("IDENT").schema().type());

        Object valNumberCount = after.get("NUMBER_COL_COUNT");
        assertNotNull(valNumberCount);
        assertEquals(600000L, valNumberCount);
        assertEquals(Schema.INT64_SCHEMA.type(), after.schema().field("NUMBER_COL_COUNT").schema().type());

        Object valDoublePrecision = after.get("DOUBLE_PRECISION_COL");
        assertNotNull(valDoublePrecision);
        assertEquals(7622.289912, valDoublePrecision);
        assertEquals(Schema.FLOAT64_SCHEMA.type(), after.schema().field("DOUBLE_PRECISION_COL").schema().type());

        Object valReal = after.get("REAL_COL");
        assertNotNull(valReal);
        assertEquals(89874.23986, valReal);
        assertEquals(Schema.FLOAT64_SCHEMA.type(), after.schema().field("REAL_COL").schema().type());

    }

    public static void createTestTable(String tableName) throws SQLException {
        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE %s
                        (
                            ID NUMBER(1,0) PRIMARY KEY,
                            FLOAT_COL FLOAT(126),
                            NUMBER_COL NUMBER,
                            NUMBER_COL_ID NUMBER,
                            IDENT NUMBER,
                            NUMBER_COL_COUNT NUMBER,
                            DOUBLE_PRECISION_COL DOUBLE PRECISION,
                            REAL_COL REAL
                        )""".formatted(tableName));
            stmt.execute("GRANT SELECT ON %s TO c##dbzuser".formatted(tableName));
            stmt.execute("ALTER TABLE %s ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS".formatted(tableName));
        }
    }
}
