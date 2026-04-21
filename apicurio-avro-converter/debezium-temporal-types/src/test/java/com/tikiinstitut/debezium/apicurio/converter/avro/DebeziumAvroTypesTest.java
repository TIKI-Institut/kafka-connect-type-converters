package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.serde.avro.NonRecordContainer;
import io.apicurio.registry.utils.converter.avro.AvroData;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

public class DebeziumAvroTypesTest {

    @Test
    public void testDebeziumTimestampType() {
        long debeziumTimestamp = io.debezium.time.Timestamp.toEpochMillis(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Timestamp.SCHEMA_NAME,
                "long",
                "timestamp-millis");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Timestamp.schema(), debeziumTimestamp);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(debeziumTimestamp, (long) result.getValue());
    }

    @Test
    public void testDebeziumDateType() {
        int epochDays = io.debezium.time.Date.toEpochDay(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Date.SCHEMA_NAME,
                "int",
                "date");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Date.schema(), epochDays);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(epochDays, (int) result.getValue());
    }

    @Test
    public void testDebeziumMicroTimeType() {
        long microTime = io.debezium.time.MicroTime.toMicroOfDay(Duration.ofMinutes(5), true);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.MicroTime.SCHEMA_NAME,
                "long",
                "time-micros");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.MicroTime.schema(), microTime);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(microTime, (long) result.getValue());
    }

    @Test
    public void testDebeziumMicroTimestampType() {
        long microTimestamp = io.debezium.time.MicroTimestamp.toEpochMicros(new Date(), null);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.MicroTimestamp.SCHEMA_NAME,
                "long",
                "timestamp-micros");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.MicroTimestamp.schema(), microTimestamp);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(microTimestamp, (long) result.getValue());
    }

    @Test
    public void testDebeziumTimeType() {
        int milliTime = io.debezium.time.Time.toMilliOfDay(Duration.ofMinutes(5), true);

        org.apache.avro.Schema expectedAvroSchema = expectedAvroTypeSchema(
                io.debezium.time.Time.SCHEMA_NAME,
                "int",
                "time-millis");

        SchemaAndValue connectValue = new SchemaAndValue(io.debezium.time.Time.schema(), milliTime);
        AvroData avroData = new AvroData(0);
        //noinspection unchecked
        NonRecordContainer<Object> result = (NonRecordContainer<Object>) avroData.fromConnectData(connectValue.schema(), connectValue.value());

        Assertions.assertEquals(expectedAvroSchema, result.getSchema());
        Assertions.assertEquals(milliTime, (int) result.getValue());
    }

    private org.apache.avro.Schema expectedAvroTypeSchema(String connectName, String primitiveType, String logicalType) {
        String typeSchemaString =
                "{" +
                        "  \"type\" : \"" + primitiveType + "\"," +
                        "  \"connect.version\" : 1," +
                        "  \"connect.name\" : \"" + connectName + "\"," +
                        "  \"logicalType\" : \"" + logicalType + "\"" +
                        "}";
        return new org.apache.avro.Schema.Parser().parse(typeSchemaString);
    }
}
