/*
 * Copyright 2026 Brandon Brown
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bbrownsound.kafka.connect.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka Connect Single Message Transform (SMT) that converts a Struct field to a JSON string.
 *
 * <p>This transformation is useful when you have protobuf-schemaed messages (which appear as
 * Structs in Kafka Connect) and you want to serialize a specific field to a JSON string for
 * downstream processing.
 *
 * <p>The resulting JSON string is valid JSON without escape characters and can be round-tripped
 * (parsed back to the original structure).
 *
 * @param <R> The type of ConnectRecord
 * @see StructToJsonConfig
 */
public abstract class StructToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(StructToJson.class);

    private StructToJsonConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new StructToJsonConfig(configs);
        this.schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        LOG.debug(
                "Configured StructToJson with fieldName={}, outputFieldName={}, skipMissingOrNull={}",
                config.fieldName(),
                config.outputFieldName(),
                config.skipMissingOrNull());
    }

    @Override
    public R apply(final R record) {
        if (operatingValue(record) == null) {
            if (config.skipMissingOrNull()) {
                return record;
            }
            throw new DataException("Value is null");
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(final R record) {
        final Object value = operatingValue(record);

        if (!(value instanceof Map)) {
            throw new DataException(
                    "Value must be a Map for schemaless records, but was: "
                            + value.getClass().getName());
        }

        @SuppressWarnings("unchecked")
        final Map<String, Object> valueMap = (Map<String, Object>) value;

        if (!valueMap.containsKey(config.fieldName())) {
            if (config.skipMissingOrNull()) {
                return record;
            }
            throw new DataException(
                    "Field '" + config.fieldName() + "' does not exist in the record");
        }

        final Object fieldValue = valueMap.get(config.fieldName());

        if (fieldValue == null) {
            if (config.skipMissingOrNull()) {
                return record;
            }
            throw new DataException("Field '" + config.fieldName() + "' is null");
        }

        final String jsonString = convertToJson(convertSchemalessValue(fieldValue));

        final Map<String, Object> updatedValue = new HashMap<>(valueMap);
        if (config.outputFieldName().equals(config.fieldName())) {
            updatedValue.put(config.fieldName(), jsonString);
        } else {
            updatedValue.put(config.outputFieldName(), jsonString);
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(final R record) {
        final Schema schema = operatingSchema(record);
        final Object value = operatingValue(record);

        if (!(value instanceof Struct)) {
            throw new DataException(
                    "Value must be a Struct when schema is present, but was: "
                            + value.getClass().getName());
        }

        final Struct struct = (Struct) value;
        final Field field = schema.field(config.fieldName());

        if (field == null) {
            if (config.skipMissingOrNull()) {
                return record;
            }
            throw new DataException("Field '" + config.fieldName() + "' does not exist in schema");
        }

        final Object fieldValue = struct.get(field);

        if (fieldValue == null) {
            if (config.skipMissingOrNull()) {
                return record;
            }
            throw new DataException("Field '" + config.fieldName() + "' is null");
        }

        final String jsonString = convertToJson(structToMap(fieldValue, field.schema()));

        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(schema);
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (final Field f : schema.fields()) {
            if (f.name().equals(config.fieldName())
                    && config.outputFieldName().equals(config.fieldName())) {
                updatedValue.put(config.outputFieldName(), jsonString);
            } else if (!f.name().equals(config.fieldName())
                    || !config.outputFieldName().equals(config.fieldName())) {
                updatedValue.put(f.name(), struct.get(f));
            }
        }

        if (!config.outputFieldName().equals(config.fieldName())) {
            updatedValue.put(config.outputFieldName(), jsonString);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(final Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (final Field field : schema.fields()) {
            if (field.name().equals(config.fieldName())
                    && config.outputFieldName().equals(config.fieldName())) {
                //builder.field(config.outputFieldName(), Schema.OPTIONAL_STRING_SCHEMA);
                // Replacing field in-place: preserve original field's schema parameters (including io.confluent.connect.protobuf.Tag)
                builder.field(config.outputFieldName(), buildOptionalStringSchemaWithParams(field.schema().parameters()));
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        if (!config.outputFieldName().equals(config.fieldName())) {
            //builder.field(config.outputFieldName(), Schema.OPTIONAL_STRING_SCHEMA);
            // Adding a new field: assign a unique protobuf field number
            maxFieldNumber++;
            builder.field(config.outputFieldName(), buildOptionalStringSchemaWithFieldNumber(maxFieldNumber));
        }

        return builder.build();
    }

/**
     * Finds the maximum protobuf field number (tag) in the schema.
     * Protobuf field numbers are stored in schema parameters with key "io.confluent.connect.protobuf.Tag".
     */
    private int findMaxProtobufFieldNumber(final Schema schema) {
        int maxFieldNumber = 0;
        for (final Field field : schema.fields()) {
            final Map<String, String> params = field.schema().parameters();
            if (params != null && params.containsKey("io.confluent.connect.protobuf.Tag")) {
                try {
                    final int tag = Integer.parseInt(params.get("io.confluent.connect.protobuf.Tag"));
                    maxFieldNumber = Math.max(maxFieldNumber, tag);
                } catch (final NumberFormatException e) {
                    LOG.warn("Invalid io.confluent.connect.protobuf.Tag value for field {}: {}", field.name(), params.get("io.confluent.connect.protobuf.Tag"));
                }
            }
        }
        return maxFieldNumber;
    }

    /**
     * Builds an optional string schema preserving the given parameters (e.g., io.confluent.connect.protobuf.Tag).
     */
    private Schema buildOptionalStringSchemaWithParams(final Map<String, String> params) {
        final SchemaBuilder fieldBuilder = SchemaBuilder.string().optional();
        if (params != null && !params.isEmpty()) {
            fieldBuilder.parameters(params);
        }
        return fieldBuilder.build();
    }

    /**
     * Builds an optional string schema with a specific protobuf field number.
     */
    private Schema buildOptionalStringSchemaWithFieldNumber(final int fieldNumber) {
        return SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", String.valueOf(fieldNumber))
                .build();
    }

    private String convertToJson(final Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (final JsonProcessingException e) {
            throw new DataException("Failed to convert value to JSON", e);
        }
    }

    /** Converts a Struct (or nested value) to a Map suitable for JSON serialization. */
    private Object structToMap(final Object value, final Schema schema) {
        if (value == null) {
            return null;
        }

        if (schema == null) {
            return convertSchemalessValue(value);
        }

        switch (schema.type()) {
            case STRUCT:
                return structToMap((Struct) value);
            case ARRAY:
                return arrayToList((List<?>) value, schema.valueSchema());
            case MAP:
                return mapToMap((Map<?, ?>) value, schema.keySchema(), schema.valueSchema());
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case STRING:
                return value;
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                return value;
            default:
                return value;
        }
    }

    private Object convertSchemalessValue(final Object value) {
        if (value instanceof Struct) {
            return structToMap((Struct) value);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> mapValue = (Map<String, Object>) value;
            final Map<String, Object> result = new HashMap<>();
            for (final Map.Entry<String, Object> entry : mapValue.entrySet()) {
                result.put(entry.getKey(), convertSchemalessValue(entry.getValue()));
            }
            return result;
        } else if (value instanceof List) {
            final List<?> listValue = (List<?>) value;
            final List<Object> result = new ArrayList<>();
            for (final Object item : listValue) {
                result.add(convertSchemalessValue(item));
            }
            return result;
        }
        return value;
    }

    private Map<String, Object> structToMap(final Struct struct) {
        if (struct == null) {
            return null;
        }

        final Map<String, Object> map = new HashMap<>();
        for (final Field field : struct.schema().fields()) {
            final Object fieldValue = struct.get(field);
            map.put(field.name(), structToMap(fieldValue, field.schema()));
        }
        return map;
    }

    private List<Object> arrayToList(final List<?> array, final Schema elementSchema) {
        if (array == null) {
            return null;
        }

        final List<Object> list = new ArrayList<>();
        for (final Object element : array) {
            list.add(structToMap(element, elementSchema));
        }
        return list;
    }

    private Map<Object, Object> mapToMap(
            final Map<?, ?> map, final Schema keySchema, final Schema valueSchema) {
        if (map == null) {
            return null;
        }

        final Map<Object, Object> result = new HashMap<>();
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final Object key = structToMap(entry.getKey(), keySchema);
            final Object value = structToMap(entry.getValue(), valueSchema);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public ConfigDef config() {
        return StructToJsonConfig.CONFIG_DEF;
    }

    @Override
    public void close() {}

    /**
     * Returns the schema to operate on (key or value schema).
     *
     * @param record the record to get the schema from
     * @return the schema to operate on
     */
    protected abstract Schema operatingSchema(R record);

    /**
     * Returns the value to operate on (key or value).
     *
     * @param record the record to get the value from
     * @return the value to operate on
     */
    protected abstract Object operatingValue(R record);

    /**
     * Creates a new record with the updated schema and value.
     *
     * @param record the original record
     * @param updatedSchema the updated schema
     * @param updatedValue the updated value
     * @return a new record with the updated schema and value
     */
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    /** Implementation for transforming record keys. */
    public static class Key<R extends ConnectRecord<R>> extends StructToJson<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.key();
        }

        @Override
        protected R newRecord(
                final R record, final Schema updatedSchema, final Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    updatedSchema,
                    updatedValue,
                    record.valueSchema(),
                    record.value(),
                    record.timestamp());
        }
    }

    /** Implementation for transforming record values. */
    public static class Value<R extends ConnectRecord<R>> extends StructToJson<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.value();
        }

        @Override
        protected R newRecord(
                final R record, final Schema updatedSchema, final Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp());
        }
    }
}
