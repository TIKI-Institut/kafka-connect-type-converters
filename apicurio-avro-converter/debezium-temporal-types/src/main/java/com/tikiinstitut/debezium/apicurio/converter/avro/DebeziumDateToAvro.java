package com.tikiinstitut.debezium.apicurio.converter.avro;

import io.apicurio.registry.utils.converter.avro.ToAvroLogicalTypeConverter;
import io.debezium.time.Date;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

/**
 * {@link ToAvroLogicalTypeConverter} for converting debezium {@link Date} to avro logical type 'date'.
 * Both uses days since epoch as int32 (ISO)
 * => no conversion needed.
 *
 * @see <a href="https://avro.apache.org/docs/1.12.0/specification/#date">Avro Logical Type Date</a>
 */
public class DebeziumDateToAvro implements ToAvroLogicalTypeConverter {
    @Override
    public String kafkaConnectLogicalTypeName() {
        return Date.SCHEMA_NAME;
    }

    @Override
    public LogicalType avroLogicalType(Schema schema) {
        return LogicalTypes.date();
    }

    @Override
    public Object convert(Schema schema, Object value) {
        if (!(value instanceof Integer)) {
            throw new DataException(
                    "Invalid type for io.debezium.time.Date, expected int32 but was " + value.getClass());
        }
        return Date.toEpochDay(value, null);
    }
}
