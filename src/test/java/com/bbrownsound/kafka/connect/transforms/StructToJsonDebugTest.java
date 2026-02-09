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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StructToJsonDebugTest {

    private static final String TOPIC = "test-topic";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() {};

    private StructToJsonDebug<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new StructToJsonDebug.Value<>();
    }

    @AfterEach
    void tearDown() {
        if (transform != null) {
            transform.close();
        }
    }

    private SourceRecord createValueRecord(final Schema schema, final Object value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                TOPIC,
                0,
                null,
                null,
                schema,
                value,
                null);
    }

    private SourceRecord createValueRecordWithKey(
            final Schema keySchema,
            final Object key,
            final Schema valueSchema,
            final Object value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                TOPIC,
                0,
                keySchema,
                key,
                valueSchema,
                value,
                null);
    }

    private void configureTransform(final String fieldMappings) {
        final Map<String, Object> config = new HashMap<>();
        config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, fieldMappings);
        transform.configure(config);
    }

    private void configureTransform(final String fieldMappings, final boolean skipMissingOrNull) {
        final Map<String, Object> config = new HashMap<>();
        config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, fieldMappings);
        config.put(StructToJsonDebugConfig.SKIP_MISSING_OR_NULL_CONFIG, skipMissingOrNull);
        transform.configure(config);
    }

    private Map<String, Object> parseJson(final String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, MAP_TYPE_REF);
    }

    @Nested
    @DisplayName("Configuration")
    class Configuration {

        @Test
        @DisplayName("Should return config definition with field.mappings and skip.missing.or.null")
        void shouldReturnConfigDef() {
            configureTransform("a:b");
            assertNotNull(transform.config());
            assertTrue(
                    transform
                            .config()
                            .names()
                            .contains(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG));
            assertTrue(
                    transform
                            .config()
                            .names()
                            .contains(StructToJsonDebugConfig.SKIP_MISSING_OR_NULL_CONFIG));
        }
    }

    @Nested
    @DisplayName("Schema-based transformation")
    class SchemaBasedTransformation {

        @Test
        @DisplayName("Should transform single field in place (input:output same)")
        void shouldTransformSingleFieldInPlace() throws JsonProcessingException {
            configureTransform("data:data");

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT32_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .build();
            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();
            final Struct innerStruct = new Struct(innerSchema).put("id", 1).put("name", "alice");
            final Struct value = new Struct(schema).put("data", innerStruct);

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            final String json = (String) resultValue.get("data");
            assertNotNull(json);
            final Map<String, Object> parsed = parseJson(json);
            assertEquals(1, parsed.get("id"));
            assertEquals("alice", parsed.get("name"));
        }

        @Test
        @DisplayName("Should transform single field to different output field")
        void shouldTransformSingleFieldToDifferentOutput() throws JsonProcessingException {
            configureTransform("payload:payload_json");

            final Schema innerSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("payload", innerSchema)
                            .field("other", Schema.STRING_SCHEMA)
                            .build();
            final Struct innerStruct = new Struct(innerSchema).put("x", "hello");
            final Struct value =
                    new Struct(schema).put("payload", innerStruct).put("other", "keep");

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            assertEquals(innerStruct, resultValue.get("payload"));
            assertEquals("keep", resultValue.get("other"));
            final String json = (String) resultValue.get("payload_json");
            assertNotNull(json);
            final Map<String, Object> parsed = parseJson(json);
            assertEquals("hello", parsed.get("x"));
        }

        @Test
        @DisplayName("Should transform multiple fields")
        void shouldTransformMultipleFields() throws JsonProcessingException {
            configureTransform("payload:payload_json,meta:meta_json");

            final Schema payloadSchema =
                    SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
            final Schema metaSchema =
                    SchemaBuilder.struct().field("source", Schema.STRING_SCHEMA).build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("payload", payloadSchema)
                            .field("meta", metaSchema)
                            .build();
            final Struct payloadStruct = new Struct(payloadSchema).put("id", 42);
            final Struct metaStruct = new Struct(metaSchema).put("source", "kafka");
            final Struct value =
                    new Struct(schema).put("payload", payloadStruct).put("meta", metaStruct);

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            final String payloadJson = (String) resultValue.get("payload_json");
            final String metaJson = (String) resultValue.get("meta_json");
            assertNotNull(payloadJson);
            assertNotNull(metaJson);
            assertEquals(42, parseJson(payloadJson).get("id"));
            assertEquals("kafka", parseJson(metaJson).get("source"));
        }

        @Test
        @DisplayName("Should preserve non-mapped fields")
        void shouldPreserveNonMappedFields() {
            configureTransform("payload:payload_json");

            final Schema payloadSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("payload", payloadSchema)
                            .field("id", Schema.INT32_SCHEMA)
                            .build();
            final Struct payloadStruct = new Struct(payloadSchema).put("x", "v");
            final Struct value = new Struct(schema).put("payload", payloadStruct).put("id", 100);

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            assertEquals(100, resultValue.get("id"));
        }

        @Test
        @DisplayName("Should throw when field missing and skip.missing.or.null is false")
        void shouldThrowWhenFieldMissingAndSkipFalse() {
            configureTransform("missing:missing_json", false);

            final Schema schema =
                    SchemaBuilder.struct().field("present", Schema.STRING_SCHEMA).build();
            final Struct value = new Struct(schema).put("present", "x");

            assertThrows(
                    DataException.class, () -> transform.apply(createValueRecord(schema, value)));
        }

        @Test
        @DisplayName("Should skip missing mapping when skip.missing.or.null is true")
        void shouldSkipMissingMappingWhenSkipTrue() {
            configureTransform("missing:missing_json,present:present_json", true);

            final Schema presentSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
            final Schema schema = SchemaBuilder.struct().field("present", presentSchema).build();
            final Struct presentStruct = new Struct(presentSchema).put("x", "v");
            final Struct value = new Struct(schema).put("present", presentStruct);

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            assertNotNull(resultValue.get("present_json"));
            assertTrue(((String) resultValue.get("present_json")).contains("v"));
        }

        @Test
        @DisplayName("Should throw when field is null and skip.missing.or.null is false")
        void shouldThrowWhenFieldNullAndSkipFalse() {
            configureTransform("data:data", false);

            final Schema dataSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).optional().build();
            final Schema schema = SchemaBuilder.struct().field("data", dataSchema).build();
            final Struct value = new Struct(schema).put("data", null);

            assertThrows(
                    DataException.class, () -> transform.apply(createValueRecord(schema, value)));
        }

        @Test
        @DisplayName("Should skip null field when skip.missing.or.null is true")
        void shouldSkipNullFieldWhenSkipTrue() {
            configureTransform("data:data_json", true);

            final Schema dataSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).optional().build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("data", dataSchema)
                            .field("other", Schema.STRING_SCHEMA)
                            .build();
            final Struct value = new Struct(schema).put("data", null).put("other", "keep");

            final SourceRecord result = transform.apply(createValueRecord(schema, value));
            final Struct resultValue = (Struct) result.value();

            // "data" was null and skipped; "other" is not in mappings so unchanged
            assertEquals("keep", resultValue.get("other"));
        }
    }

    @Nested
    @DisplayName("Schemaless transformation")
    class SchemalessTransformation {

        @Test
        @DisplayName("Should transform single field in schemaless map")
        void shouldTransformSingleFieldInSchemalessMap() throws JsonProcessingException {
            configureTransform("payload:payload_json");

            final Map<String, Object> payload = new HashMap<>();
            payload.put("id", 1);
            payload.put("name", "alice");
            final Map<String, Object> value = new HashMap<>();
            value.put("payload", payload);
            value.put("other", "keep");

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            null,
                            null,
                            null,
                            value,
                            null);
            final SourceRecord result = transform.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            assertEquals("keep", resultValue.get("other"));
            final String json = (String) resultValue.get("payload_json");
            assertNotNull(json);
            final Map<String, Object> parsed = parseJson(json);
            assertEquals(1, parsed.get("id"));
            assertEquals("alice", parsed.get("name"));
        }

        @Test
        @DisplayName("Should throw when value is not Map for schemaless")
        void shouldThrowWhenSchemalessValueNotMap() {
            configureTransform("a:b");

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            null,
                            null,
                            null,
                            "not a map",
                            null);

            assertThrows(DataException.class, () -> transform.apply(record));
        }
    }

    @Nested
    @DisplayName("Record size logging")
    class RecordSizeLogging {

        @Test
        @DisplayName("Should apply and compute record size for before and after")
        void shouldApplyAndComputeRecordSize() {
            configureTransform("data:data_json");

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT32_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .build();
            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();
            final Struct innerStruct = new Struct(innerSchema).put("id", 1).put("name", "test");
            final Struct value = new Struct(schema).put("data", innerStruct);

            final SourceRecord result = transform.apply(createValueRecord(schema, value));

            assertNotNull(result);
            assertNotNull(result.value());
            final Struct resultValue = (Struct) result.value();
            assertNotNull(resultValue.get("data_json"));
        }
    }

    @Nested
    @DisplayName("Key transformation")
    class KeyTransformation {

        @Test
        @DisplayName("Should transform key when using StructToJsonDebug.Key")
        void shouldTransformKey() throws JsonProcessingException {
            final StructToJsonDebug<SourceRecord> keyTransform = new StructToJsonDebug.Key<>();
            try {
                keyTransform.configure(
                        Map.of(
                                StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG,
                                "keyData:keyData_json"));

                final Schema keyDataSchema =
                        SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
                final Schema keySchema =
                        SchemaBuilder.struct().field("keyData", keyDataSchema).build();
                final Schema valueSchema =
                        SchemaBuilder.struct().field("v", Schema.STRING_SCHEMA).build();
                final Struct keyData = new Struct(keyDataSchema).put("id", 99);
                final Struct key = new Struct(keySchema).put("keyData", keyData);
                final Struct value = new Struct(valueSchema).put("v", "value");

                final SourceRecord record =
                        createValueRecordWithKey(keySchema, key, valueSchema, value);
                final SourceRecord result = keyTransform.apply(record);

                final Struct resultKey = (Struct) result.key();
                assertNotNull(resultKey.get("keyData_json"));
                final String json = (String) resultKey.get("keyData_json");
                assertEquals(99, parseJson(json).get("id"));
                assertEquals(value, result.value());
            } finally {
                keyTransform.close();
            }
        }
    }

    @Nested
    @DisplayName("Null value handling")
    class NullValueHandling {

        @Test
        @DisplayName("Should throw when record value is null and skip is false")
        void shouldThrowWhenValueNullAndSkipFalse() {
            configureTransform("a:b", false);

            final SourceRecord record = createValueRecord(null, null);

            assertThrows(DataException.class, () -> transform.apply(record));
        }

        @Test
        @DisplayName("Should return record unchanged when value is null and skip is true")
        void shouldReturnUnchangedWhenValueNullAndSkipTrue() {
            configureTransform("a:b", true);

            final SourceRecord record = createValueRecord(null, null);
            final SourceRecord result = transform.apply(record);

            assertEquals(record, result);
        }
    }
}
