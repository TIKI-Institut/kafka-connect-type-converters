package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.utils.converter.avro.ToAvroLogicalTypeConverter;
import io.debezium.time.Timestamp;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * {@link ToAvroLogicalTypeConverter} for converting debezium {@link Timestamp} to avro logical type 'timestamp-millis'.
 * Both uses milliseconds since epoch as int64 (ISO)
 * => no conversion needed.
 *
 * @see <a href="https://avro.apache.org/docs/1.12.0/specification/#timestamps">Avro Logical Type Timestamp Millis</a>
 */
public class DebeziumTimestampToAvro implements ToAvroLogicalTypeConverter {
    @Override
    public String kafkaConnectLogicalTypeName() {
        return Timestamp.SCHEMA_NAME;
    }

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        return LogicalTypes.timestampMillis();
    }

    @Override
    public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long)) {
            throw new DataException(
                    "Invalid type for io.debezium.time.Timestamp, expected int64 but was " + value.getClass());
        }
        return value;
    }
}
