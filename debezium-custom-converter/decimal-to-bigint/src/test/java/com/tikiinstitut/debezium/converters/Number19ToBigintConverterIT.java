package com.tikiinstitut.debezium.converters;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Testing;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
public class Number19ToBigintConverterIT {

    @Container
    private static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("/debezium_cdc_oracle_setup.sh"),
                    "/container-entrypoint-initdb.d/"
            )
            .withStartupTimeout(java.time.Duration.ofMinutes(2));

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
    private ExecutorService executor;
    private BlockingQueue<SourceRecord> consumedRecords;

    @BeforeEach
    public void setup() throws Exception {
        consumedRecords = new ArrayBlockingQueue<>(100);
        Testing.Files.delete(Testing.Files.createTestingPath("data"));

        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TEST.TEST_DECIMAL (ID NUMBER(1,0) PRIMARY KEY, VAL_DECIMAL DECIMAL(19,0))");
            stmt.execute("ALTER TABLE TEST.TEST_DECIMAL ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void shouldConvertDecimalToBigintInOracle() throws Exception {
        final Properties props = new Properties();

        props.setProperty("converters", "decimal_to_bigint");
        props.setProperty("decimal_to_bigint.type", "com.tikiinstitut.debezium.converters.Number19ToBigintConverter");

        props.setProperty("name", "oracle-it-connector");
        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        props.setProperty("database.hostname", ORACLE.getHost());
        props.setProperty("database.port", String.valueOf(ORACLE.getOraclePort()));
        props.setProperty("database.user", "c##dbzuser");
        props.setProperty("database.password", "dbz");
        props.setProperty("database.url", ORACLE.getJdbcUrl());
        props.setProperty("database.dbname", ORACLE.getDatabaseName());
        props.setProperty("topic.prefix", "shouldConvertDecimalToBigintInOracle");
        props.setProperty("database.connection.adapter", "logminer");
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("log.mining.strategy", "online_catalog");

        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", Testing.Files.createTestingPath("data/history.dat").toString());
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", Testing.Files.createTestingPath("data/offsets.dat").toString());

        engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .notifying(event -> {
                            if (!event.value().valueSchema().name().contains("SchemaChange")) {
                                consumedRecords.add(event.value());
                            }
                        }
                )
                .build();

        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO TEST.TEST_DECIMAL (ID, VAL_DECIMAL) VALUES (1, 1234567890123456789)");
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
    }
}
