package com.tikiinstitut.debezium.converters;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.util.Testing;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public abstract class AbstractOracleConnectorTest {
    @Container
    protected static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:slim")
            .withCopyFileToContainer(MountableFile.forClasspathResource("/debezium_cdc_oracle_setup.sh"), "/container-entrypoint-initdb.d/debezium_cdc_oracle_setup.sh")
            .withStartupTimeout(java.time.Duration.ofMinutes(2));

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
    private ExecutorService executor;
    protected BlockingQueue<SourceRecord> consumedRecords;
    private final AtomicBoolean isEngineRunning = new AtomicBoolean(false);
    protected TestInfo testInfo;

    protected final DebeziumEngine.ConnectorCallback wrapperConnectorCallback = new DebeziumEngine.ConnectorCallback() {
        @Override
        public void connectorStarted() {
            isEngineRunning.compareAndExchange(false, true);
        }

        @Override
        public void connectorStopped() {
            isEngineRunning.set(false);
        }
    };

    @BeforeEach
    public void setup(TestInfo testInfo) throws SQLException {
        consumedRecords = new ArrayBlockingQueue<>(100);
        Testing.Files.delete(Testing.Files.createTestingPath("data"));

        this.testInfo = testInfo;

        createTestTable(testInfo.getDisplayName());
    }

    @AfterEach
    public void tearDown(TestInfo testInfo) throws IOException, SQLException {
        if (engine != null) {
            engine.close();
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> !isEngineRunning.get());
        }
        if (executor != null) {
            List<Runnable> neverRunTasks = executor.shutdownNow();
            assertTrue(neverRunTasks.isEmpty());
        }

        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE TEST." + testInfo.getDisplayName());
        }
    }

    private void createTestTable(String tableName) throws SQLException {
        try (Connection conn = ORACLE.createConnection(""); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE TEST." + tableName + " (ID NUMBER(1,0) PRIMARY KEY, VAL_DECIMAL DECIMAL(19,0), VAL_NOT_NULL NUMBER(19,0) NOT NULL)");
            stmt.execute(String.format("GRANT SELECT ON %s TO %s", "TEST." + tableName, "c##dbzuser"));
            stmt.execute("ALTER TABLE TEST." + tableName + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
    }

    protected Properties createDebeziumProperties(String testCaseName) {
        final Properties props = new Properties();

        props.setProperty("converters", "decimal_to_bigint");
        props.setProperty("decimal_to_bigint.type", "com.tikiinstitut.debezium.converters.Number19ToBigintConverter");

        props.setProperty("name", testCaseName);
        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        props.setProperty("database.hostname", ORACLE.getHost());
        props.setProperty("database.port", String.valueOf(ORACLE.getOraclePort()));
        props.setProperty("database.user", "c##dbzuser");
        props.setProperty("database.password", "dbz");
        props.setProperty("database.url", ORACLE.getJdbcUrl().replace(ORACLE.getDatabaseName(), "free"));
        props.setProperty("database.dbname", "free");
        props.setProperty("database.pdb.name", ORACLE.getDatabaseName());
        props.setProperty("topic.prefix", testCaseName);
        props.setProperty("table.include.list", "TEST." + testCaseName);
        props.setProperty("database.connection.adapter", "logminer");
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("log.mining.strategy", "hybrid");

        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", Testing.Files.createTestingPath("data/history.dat").toString());
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", Testing.Files.createTestingPath("data/offsets.dat").toString());

        props.setProperty("decimal.handling.mode", "precise");
        return props;
    }

    protected void startDebeziumEngine(Properties props) {
        engine = DebeziumEngine.create(Connect.class)
                .using(props)
                .notifying(event -> {
                    if (!event.value().valueSchema().name().contains("SchemaChange")) {
                        consumedRecords.add(event.value());
                    }
                })
                .using(wrapperConnectorCallback).build();

        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    protected void awaitRunningDebeziumEngine() {
        Awaitility.await().atMost(60, TimeUnit.SECONDS).until(isEngineRunning::get);
    }
}
