package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.utils.converter.avro.ToAvroLogicalTypeConverter;
import io.debezium.time.Time;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * {@link ToAvroLogicalTypeConverter} for converting debezium {@link Time} to avro logical type 'time-millis'.
 * Both uses number of milliseconds since midnight int32
 * => no conversion needed.
 *
 * @see <a href="https://avro.apache.org/docs/1.12.0/specification/#time_ms">Avro Logical Type Time Millis</a>
 */
public class DebeziumTimeToAvro implements ToAvroLogicalTypeConverter {
    @Override
    public String kafkaConnectLogicalTypeName() {
        return Time.SCHEMA_NAME;
    }

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        return LogicalTypes.timeMillis();
    }

    @Override
    public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer)) {
            throw new DataException(
                    "Invalid type for io.debezium.time.Time, expected int32 but was " + value.getClass());
        }
        return value;
    }
}
