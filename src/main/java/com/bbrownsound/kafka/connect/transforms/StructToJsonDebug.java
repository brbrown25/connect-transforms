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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Debug variant of StructToJson that supports multiple field mappings via a single config: {@code
 * input_field:output_field} with comma-separated entries (e.g. {@code
 * payload:payload_json,meta:meta_json}).
 *
 * @param <R> The type of ConnectRecord
 * @see StructToJsonDebugConfig
 */
public abstract class StructToJsonDebug<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LogManager.getLogger(StructToJsonDebug.class);

    private StructToJsonDebugConfig config;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new StructToJsonDebugConfig(configs);
        this.schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        LOG.info(
                "Configured StructToJsonDebug with fieldMappings={}, skipMissingOrNull={}",
                config.fieldMappings(),
                config.skipMissingOrNull());
    }

    @Override
    public R apply(final R record) {
        final String topic = record.topic();
        final Integer partition = record.kafkaPartition();
        LOG.info(
                "StructToJsonDebug.apply entry: topic={}, partition={}, hasSchema={}",
                topic,
                partition,
                operatingSchema(record) != null);
        try {
            if (operatingValue(record) == null) {
                if (config.skipMissingOrNull()) {
                    LOG.info(
                            "StructToJsonDebug skipped record: topic={}, partition={}, reason=value_is_null",
                            topic,
                            partition);
                    return record;
                }
                throw new DataException("Value is null");
            }

            R result;
            if (operatingSchema(record) == null) {
                LOG.info(
                        "Applying schemaless transformation: topic={}, partition={}",
                        topic,
                        partition);
                result = applySchemaless(record);
            } else {
                LOG.info(
                        "Applying schema transformation: topic={}, partition={}", topic, partition);
                result = applyWithSchema(record);
            }
            final int beforeRecordSize = recordSizeInBytes(record);
            final int afterRecordSize = recordSizeInBytes(result);
            LOG.info(
                    "StructToJsonDebug applied: topic={}, partition={}, mappings={}, "
                            + "beforeRecordSizeBytes={}, afterRecordSizeBytes={}",
                    topic,
                    partition,
                    config.fieldMappings().size(),
                    beforeRecordSize,
                    afterRecordSize);
            return result;
        } catch (final Exception e) {
            LOG.info(
                    "StructToJsonDebug encountered error for record: topic={}, partition={}, error={}",
                    topic,
                    partition,
                    e.getMessage());
            LOG.error(
                    "StructToJsonDebug failed: topic={}, partition={}, error={}, cause={}",
                    topic,
                    partition,
                    e.getMessage(),
                    e.getCause() != null ? e.getCause().getMessage() : null,
                    e);
            throw e;
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
        final Map<String, Object> valueMap = new HashMap<>((Map<String, Object>) value);

        final Map<String, String> inputToJson = new HashMap<>();
        for (final StructToJsonDebugConfig.FieldMapping mapping : config.fieldMappings()) {
            final String inputField = mapping.inputField();
            final String outputField = mapping.outputField();

            if (!valueMap.containsKey(inputField)) {
                if (config.skipMissingOrNull()) {
                    LOG.info(
                            "StructToJsonDebug skipping mapping (missing): {} -> {}",
                            inputField,
                            outputField);
                    continue;
                }
                throw new DataException("Field '" + inputField + "' does not exist in the record");
            }

            final Object fieldValue = valueMap.get(inputField);
            if (fieldValue == null) {
                if (config.skipMissingOrNull()) {
                    LOG.info(
                            "StructToJsonDebug skipping mapping (null): {} -> {}",
                            inputField,
                            outputField);
                    continue;
                }
                throw new DataException("Field '" + inputField + "' is null");
            }

            final String jsonString =
                    inputToJson.computeIfAbsent(
                            inputField, k -> convertToJson(convertSchemalessValue(fieldValue)));
            valueMap.put(outputField, jsonString);
        }

        return newRecord(record, null, valueMap);
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
        final Map<String, String> inputToJson = new HashMap<>();

        for (final StructToJsonDebugConfig.FieldMapping mapping : config.fieldMappings()) {
            final String inputField = mapping.inputField();
            final String outputField = mapping.outputField();
            final Field field = schema.field(inputField);

            if (field == null) {
                if (config.skipMissingOrNull()) {
                    LOG.info(
                            "StructToJsonDebug skipping mapping (not in schema): {} -> {}",
                            inputField,
                            outputField);
                    continue;
                }
                throw new DataException("Field '" + inputField + "' does not exist in schema");
            }

            final Object fieldValue = struct.get(field);
            if (fieldValue == null) {
                if (config.skipMissingOrNull()) {
                    LOG.info(
                            "StructToJsonDebug skipping mapping (null): {} -> {}",
                            inputField,
                            outputField);
                    continue;
                }
                throw new DataException("Field '" + inputField + "' is null");
            }

            inputToJson.put(
                    inputField,
                    inputToJson.computeIfAbsent(
                            inputField,
                            k -> convertToJson(structToMap(fieldValue, field.schema()))));
        }

        if (inputToJson.isEmpty()) {
            LOG.info(
                    "StructToJsonDebug no mappings applied (all skipped): topic={}, partition={}",
                    record.topic(),
                    record.kafkaPartition());
            return record;
        }

        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(schema);
            schemaUpdateCache.put(schema, updatedSchema);
        }

        final Set<String> outputFieldNames = new HashSet<>();
        for (final StructToJsonDebugConfig.FieldMapping m : config.fieldMappings()) {
            if (inputToJson.containsKey(m.inputField())) {
                outputFieldNames.add(m.outputField());
            }
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (final Field f : updatedSchema.fields()) {
            if (outputFieldNames.contains(f.name())) {
                for (final StructToJsonDebugConfig.FieldMapping m : config.fieldMappings()) {
                    if (m.outputField().equals(f.name())
                            && inputToJson.containsKey(m.inputField())) {
                        updatedValue.put(f.name(), inputToJson.get(m.inputField()));
                        break;
                    }
                }
            } else {
                final Field origField = schema.field(f.name());
                if (origField != null) {
                    updatedValue.put(f.name(), struct.get(origField));
                }
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(final Schema schema) {
        final Set<String> inputsToReplace = new HashSet<>();
        final Set<String> outputsToAdd = new HashSet<>();
        for (final StructToJsonDebugConfig.FieldMapping m : config.fieldMappings()) {
            if (schema.field(m.inputField()) != null) {
                if (m.inputField().equals(m.outputField())) {
                    inputsToReplace.add(m.inputField());
                } else {
                    outputsToAdd.add(m.outputField());
                }
            }
        }

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (final Field field : schema.fields()) {
            if (inputsToReplace.contains(field.name())) {
                builder.field(
                        field.name(),
                        buildOptionalStringSchemaWithParams(field.schema().parameters()));
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        int maxFieldNumber = findMaxProtobufFieldNumber(schema);
        for (final String outputField : outputsToAdd) {
            if (schema.field(outputField) == null) {
                maxFieldNumber++;
                builder.field(
                        outputField, buildOptionalStringSchemaWithFieldNumber(maxFieldNumber));
            }
        }

        return builder.build();
    }

    /**
     * Finds the maximum protobuf field number (tag) in the schema. Protobuf field numbers are
     * stored in schema parameters with key "io.confluent.connect.protobuf.Tag".
     */
    private int findMaxProtobufFieldNumber(final Schema schema) {
        int maxFieldNumber = 0;
        for (final Field field : schema.fields()) {
            final Map<String, String> params = field.schema().parameters();
            if (params != null && params.containsKey("io.confluent.connect.protobuf.Tag")) {
                try {
                    final int tag =
                            Integer.parseInt(params.get("io.confluent.connect.protobuf.Tag"));
                    maxFieldNumber = Math.max(maxFieldNumber, tag);
                } catch (final NumberFormatException e) {
                    LOG.warn(
                            "Invalid io.confluent.connect.protobuf.Tag value for field {}: {}",
                            field.name(),
                            params.get("io.confluent.connect.protobuf.Tag"));
                }
            }
        }
        return maxFieldNumber;
    }

    /**
     * Builds an optional string schema preserving the given parameters (e.g.,
     * io.confluent.connect.protobuf.Tag).
     */
    private Schema buildOptionalStringSchemaWithParams(final Map<String, String> params) {
        final SchemaBuilder fieldBuilder = SchemaBuilder.string().optional();
        if (params != null && !params.isEmpty()) {
            fieldBuilder.parameters(params);
        }
        return fieldBuilder.build();
    }

    /** Builds an optional string schema with a specific protobuf field number. */
    private Schema buildOptionalStringSchemaWithFieldNumber(final int fieldNumber) {
        return SchemaBuilder.string()
                .optional()
                .parameter("io.confluent.connect.protobuf.Tag", String.valueOf(fieldNumber))
                .build();
    }

    /**
     * Returns the approximate size in bytes of the full Connect record (key + value) when
     * serialized as JSON (UTF-8). Returns -1 if serialization fails for either part.
     */
    private int recordSizeInBytes(final R record) {
        final int keySize = sizeInBytes(record.key());
        final int valueSize = sizeInBytes(record.value());
        if (keySize < 0 || valueSize < 0) {
            return -1;
        }
        return keySize + valueSize;
    }

    /**
     * Returns the approximate size in bytes of the value when serialized as JSON (UTF-8). Returns 0
     * for null, -1 if serialization fails.
     */
    private int sizeInBytes(final Object value) {
        if (value == null) {
            return 0;
        }
        try {
            final Object toSerialize =
                    value instanceof Struct ? structToMap((Struct) value) : value;
            return objectMapper.writeValueAsBytes(toSerialize).length;
        } catch (final JsonProcessingException e) {
            LOG.debug("Could not compute size for value: {}", e.getMessage());
            return -1;
        }
    }

    private String convertToJson(final Object value) {
        LOG.info("Converting value to JSON");
        try {
            return objectMapper.writeValueAsString(value);
        } catch (final JsonProcessingException e) {
            LOG.error("Failed to convert value to JSON: {}", e.getMessage(), e);
            throw new DataException("Failed to convert value to JSON", e);
        }
    }

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
        return StructToJsonDebugConfig.CONFIG_DEF;
    }

    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    /** Implementation for transforming record keys. */
    public static class Key<R extends ConnectRecord<R>> extends StructToJsonDebug<R> {

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
    public static class Value<R extends ConnectRecord<R>> extends StructToJsonDebug<R> {

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
            LOG.info(
                    "StructToJsonDebug.Value.newRecord: topic={}, partition={}, updatedSchemaFields={}",
                    record.topic(),
                    record.kafkaPartition(),
                    updatedSchema != null ? updatedSchema.fields().size() : 0);
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
