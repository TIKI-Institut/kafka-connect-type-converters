# Kafka Connect type converters

The goal of this repository is to provide a collection of type converters for the kafka connect runtime, or frameworks
using the kafka connect runtime.

## Converters

### Debezium temporal types for Apicurio Avro converter

It uses
the [Apicuro Avro Converter SPI interface](https://github.com/Apicurio/apicurio-registry/blob/main/utils/converter/src/main/java/io/apicurio/registry/utils/converter/avro/ToAvroLogicalTypeConverter.java)
to provide additional avro converter
for [Debezium Kafka Connect temporal types](https://github.com/debezium/debezium/tree/main/debezium-connector-common/src/main/java/io/debezium/time)

#### Type matrix

| Debezium type                     | Avro type | Avro logical type  |
|-----------------------------------|-----------|--------------------|
| `io.debezium.time.Date`           | `int32`   | `date`             |
| `io.debezium.time.Time`           | `int32`   | `time-millis`      |
| `io.debezium.time.MicroTime`      | `int64`   | `time-micros`      |
| `io.debezium.time.Timestamp`      | `int64`   | `timestamp-millis` |
| `io.debezium.time.MicroTimestamp` | `int64`   | `timestamp-micros` |

### Number19 to Bigint converter

Debezium custom converter that converts NUMBER(19,0) columns to INT64 (bigint).
Debezium normally converts any decimal types with a precision of 19 and a scale of 0 to BigDecimal due to a
possible overflow in signed 64-bit integers.
This converter ignores any potential overflow and should only be used accordingly.
Java signed 64-bit integer max value = 2^63-1

```yaml
converters: <converterSymbolicName>
<converterSymbolicName>.type: <fullyQualifiedConverterClassName> 
```

example:

```yaml
converters: number19_to_bigint
number19_to_bigint.type: com.tikiinstitut.debezium.converters.Number19ToBigintConverter 
```


