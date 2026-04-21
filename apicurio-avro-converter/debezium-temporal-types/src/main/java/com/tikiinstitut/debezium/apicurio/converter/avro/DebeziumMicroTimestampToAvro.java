package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.utils.converter.avro.ToAvroLogicalTypeConverter;
import io.debezium.time.MicroTimestamp;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * {@link ToAvroLogicalTypeConverter} for converting debezium {@link MicroTimestamp} to avro logical type 'timestamp-micros'.
 * Both uses microseconds since epoch as int64 (ISO)
 * => no conversion needed.
 *
 * @see <a href="https://avro.apache.org/docs/1.12.0/specification/#timestamps">Avro Logical Type Timestamp Micros</a>
 */
public class DebeziumMicroTimestampToAvro implements ToAvroLogicalTypeConverter {
    @Override
    public String kafkaConnectLogicalTypeName() {
        return MicroTimestamp.SCHEMA_NAME;
    }

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        return LogicalTypes.timestampMicros();
    }

    @Override
    public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long)) {
            throw new DataException(
                    "Invalid type for io.debezium.time.MicroTimestamp, expected int64 but was " + value.getClass());
        }
        return value;
    }
}
