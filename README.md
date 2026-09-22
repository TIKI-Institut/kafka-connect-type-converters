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

### VariableScaleDecimal converter

Debezium custom converter that replaces `io.debezium.data.VariableScaleDecimal` with a primitive type.

With `decimal.handling.mode: precise`, Oracle columns declared as scale-less `NUMBER` or as `FLOAT(n)`
carry no fixed scale, so Debezium emits `io.debezium.data.VariableScaleDecimal`, a struct of
`{scale, value}`. As a struct it becomes an Avro record, which the Apicurio Avro converter registers as
its own artifact and links by schema reference. Consumers that do not resolve schema references then fail
with `Undefined schema: io.debezium.data.VariableScaleDecimal`.

This converter maps those columns to `int64`, `Decimal` or `float64` instead, so no record type and no
schema reference is created. Columns that already have a scale, such as `NUMBER(19,0)`, `NUMBER(19,3)` and
`NUMBER(*,0)`, are left untouched.

#### Type matrix

| Oracle type                         | matches                 | Connect type                          |
|-------------------------------------|-------------------------|---------------------------------------|
| `NUMBER` (no precision or scale)    | `integerColumnsRegex`   | `int64`                               |
| `NUMBER` (no precision or scale)    | `decimalColumnsRegex`   | `Decimal(decimalPrecision, decimalScale)` |
| `NUMBER` (no precision or scale)    | neither                 | `float64`                             |
| `FLOAT(n)`                          | not consulted           | `float64`                             |

`integerColumnsRegex` wins when a column name matches both. `FLOAT` is binary floating point and is never
mapped to a decimal.

#### Configuration

| Property               | Default | Description                                                             |
|------------------------|---------|-------------------------------------------------------------------------|
| `integerColumnsRegex`  | `^$`    | Column names to map to `int64`                                          |
| `decimalColumnsRegex`  | `^$`    | Column names to map to `Decimal`                                        |
| `decimalPrecision`     | `19`    | Precision of the `Decimal` schema                                       |
| `decimalScale`         | `6`     | Scale of the `Decimal` schema, values beyond it are rounded half up     |

Both regexes default to `^$`, which matches nothing, so every scale-less `NUMBER` becomes `float64` until
they are configured.

example:

```yaml
converters: variablescaledecimal
variablescaledecimal.type: com.tikiinstitut.debezium.converters.VariableScaleDecimalConverter
variablescaledecimal.integerColumnsRegex: "(.*_ID)|(IDENT)"
variablescaledecimal.decimalColumnsRegex: "(.*_PRICE)"
variablescaledecimal.decimalPrecision: "19"
variablescaledecimal.decimalScale: "6"
```
