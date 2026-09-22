package com.tikiinstitut.debezium.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers both Oracle readers: the snapshot yields BigDecimal, LogMiner yields String.
 */
public class VariableScaleDecimalConverterIT extends AbstractOracleConnectorTest {

    private static final String INTEGER_COLUMNS_REGEX = "(^.*_ID$)|(^IDENT$)|(^.*_COUNT)";

    // Row 1 gives the snapshot something to emit; row 2 is inserted only once the snapshot
    // is done, so it can only come from LogMiner
    @Test
    public void streamingPathShouldConvertVariableScaleDecimal() throws Exception {
        insertRow(1);

        startDebeziumEngine(converterProperties());
        awaitRunningDebeziumEngine();
        awaitSnapshotCompleted();

        insertRow(2);

        SourceRecord record = awaitRecord(r -> !isSnapshotRecord(r), "a streaming (LogMiner) record");
        assertFalse(isSnapshotRecord(record),
                "record must come from LogMiner, not the snapshot; marker was " + snapshotMarker(record));
        assertConvertedValues(record, 2);
    }

    // The row exists before the engine starts, so it is read over JDBC
    @Test
    public void snapshotPathShouldConvertVariableScaleDecimal() throws Exception {
        insertRow(1);

        startDebeziumEngine(converterProperties());
        awaitRunningDebeziumEngine();

        SourceRecord record = awaitRecord(AbstractOracleConnectorTest::isSnapshotRecord, "a snapshot (JDBC) record");
        assertTrue(isSnapshotRecord(record),
                "record must come from the snapshot reader; marker was " + snapshotMarker(record));
        assertConvertedValues(record, 1);
    }

    private Properties converterProperties() {
        final Properties props = createDebeziumProperties(testInfo.getDisplayName());
        props.setProperty("converters", "variablescaledecimal");
        props.setProperty("variablescaledecimal.type", "com.tikiinstitut.debezium.converters.VariableScaleDecimalConverter");
        props.setProperty("variablescaledecimal.integerColumnsRegex", INTEGER_COLUMNS_REGEX);
        return props;
    }

    private void insertRow(int id) throws SQLException {
        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("""
                    INSERT INTO %s (ID, FLOAT_COL, NUMBER_COL, NUMBER_COL_ID, IDENT, NUMBER_COL_COUNT, DOUBLE_PRECISION_COL, REAL_COL)
                    VALUES (%d, 20.0, 300.0, 4000, 50000.0, 600000, 7622.289912, 89874.23986)""",
                    getTableFQN(testInfo.getDisplayName()), id));
        }
    }

    private void assertConvertedValues(SourceRecord record, int expectedId) {
        assertNotNull(record, "Should have captured a record");

        Struct value = (Struct) record.value();
        Struct after = value.getStruct("after");
        assertNotNull(after, "Event should have 'after' state");

        assertEquals((long) expectedId, ((Number) after.get("ID")).longValue(), "unexpected row");

        assertFloat64(after, "FLOAT_COL", 20.0);
        assertFloat64(after, "NUMBER_COL", 300.0);
        assertFloat64(after, "DOUBLE_PRECISION_COL", 7622.289912);
        assertFloat64(after, "REAL_COL", 89874.23986);

        assertInt64(after, "NUMBER_COL_ID", 4000L);
        assertInt64(after, "IDENT", 50000L);
        assertInt64(after, "NUMBER_COL_COUNT", 600000L);
    }

    private static void assertFloat64(Struct after, String field, double expected) {
        assertEquals(Schema.FLOAT64_SCHEMA.type(), after.schema().field(field).schema().type(),
                field + " should be registered as FLOAT64");
        assertNotNull(after.get(field), field + " must not be null - the converter failed open");
        assertEquals(expected, after.get(field), field);
    }

    private static void assertInt64(Struct after, String field, long expected) {
        assertEquals(Schema.INT64_SCHEMA.type(), after.schema().field(field).schema().type(),
                field + " should be registered as INT64");
        assertNotNull(after.get(field), field + " must not be null - the converter failed open");
        assertEquals(expected, after.get(field), field);
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
