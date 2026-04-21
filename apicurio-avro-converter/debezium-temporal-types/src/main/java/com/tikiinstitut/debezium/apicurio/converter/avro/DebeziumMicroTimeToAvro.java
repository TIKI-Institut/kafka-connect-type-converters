package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.utils.converter.avro.ToAvroLogicalTypeConverter;
import io.debezium.time.MicroTime;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * {@link ToAvroLogicalTypeConverter} for converting debezium {@link MicroTime} to avro logical type 'time-micros'.
 * Both uses number of microseconds since midnight int64
 * => no conversion needed.
 *
 * @see <a href="https://avro.apache.org/docs/1.12.0/specification/#time-microsecond-precision">Avro Logical Type Time micros</a>
 */
public class DebeziumMicroTimeToAvro implements ToAvroLogicalTypeConverter {
    @Override
    public String kafkaConnectLogicalTypeName() {
        return MicroTime.SCHEMA_NAME;
    }

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        return LogicalTypes.timeMicros();
    }

    @Override
    public Object convert(Schema schema, Object value) {
        if (!(value instanceof Long)) {
            throw new DataException(
                    "Invalid type for io.debezium.time.MicroTime, expected int64 but was " + value.getClass());
        }
        return value;
    }
}
