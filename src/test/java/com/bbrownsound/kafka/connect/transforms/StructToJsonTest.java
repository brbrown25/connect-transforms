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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class StructToJsonTest extends BaseTransformTest {

    private static final String TOPIC = "test-topic";

    private StructToJson<SourceRecord> transformValue;
    private StructToJson<SourceRecord> transformKey;

    @Override
    protected String getTopicName() {
        return TOPIC;
    }

    @Override
    @BeforeEach
    void setUp() {
        super.setUp();
        transformValue = transform;
        transformKey = new StructToJson.Key<>();
    }

    @Override
    @AfterEach
    void tearDown() {
        super.tearDown();
        if (transformKey != null) {
            transformKey.close();
        }
    }

    @Nested
    @DisplayName("Configuration Tests")
    class ConfigurationTests {

        @Test
        @DisplayName("Should return valid config definition")
        void shouldReturnValidConfigDef() {
            assertNotNull(transformValue.config());
            assertTrue(
                    transformValue.config().names().contains(StructToJsonConfig.FIELD_NAME_CONFIG));
            assertTrue(
                    transformValue
                            .config()
                            .names()
                            .contains(StructToJsonConfig.OUTPUT_FIELD_NAME_CONFIG));
            assertTrue(
                    transformValue
                            .config()
                            .names()
                            .contains(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG));
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"   ", "\t", "\n"})
        @DisplayName("Should throw exception for invalid field name")
        void shouldThrowExceptionForInvalidFieldName(final String fieldName) {
            final Map<String, Object> config = new HashMap<>();
            if (fieldName != null) {
                config.put(StructToJsonConfig.FIELD_NAME_CONFIG, fieldName);
            }
            assertThrows(ConfigException.class, () -> transformValue.configure(config));
        }

        @Test
        @DisplayName("Should use field name as output field name when not specified")
        void shouldUseFieldNameAsOutputFieldNameWhenNotSpecified() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "testField");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("testField", innerSchema).build();

            final Struct innerStruct = new Struct(innerSchema).put("name", "test");

            final Struct value = new Struct(schema).put("testField", innerStruct);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            assertNotNull(resultValue.get("testField"));
            assertTrue(resultValue.get("testField") instanceof String);
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "   ", "\t", "\n"})
        @DisplayName(
                "Should use field name as output field name when output field name is empty or whitespace")
        void shouldUseFieldNameAsOutputFieldNameWhenOutputFieldNameIsEmptyOrWhitespace(
                final String outputFieldName) {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "testField");
            config.put(StructToJsonConfig.OUTPUT_FIELD_NAME_CONFIG, outputFieldName);
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("testField", innerSchema).build();

            final Struct innerStruct = new Struct(innerSchema).put("name", "test");

            final Struct value = new Struct(schema).put("testField", innerStruct);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();

            assertNotNull(resultValue.get("testField"));
            assertTrue(resultValue.get("testField") instanceof String);
        }

        @Test
        @DisplayName("Should use different output field name when specified")
        void shouldUseDifferentOutputFieldNameWhenSpecified() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "sourceField");
            config.put(StructToJsonConfig.OUTPUT_FIELD_NAME_CONFIG, "targetField");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

            final Schema schema =
                    SchemaBuilder.struct()
                            .field("sourceField", innerSchema)
                            .field("otherField", Schema.STRING_SCHEMA)
                            .build();

            final Struct innerStruct = new Struct(innerSchema).put("name", "test");

            final Struct value =
                    new Struct(schema).put("sourceField", innerStruct).put("otherField", "other");

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();

            assertNotNull(resultValue.get("sourceField"));
            assertNotNull(resultValue.get("targetField"));
            assertTrue(resultValue.get("targetField") instanceof String);
        }
    }

    @Nested
    @DisplayName("Schema-based Transformation Tests")
    class SchemaBasedTransformationTests {

        @Test
        @DisplayName("Should transform simple struct to JSON")
        void shouldTransformSimpleStructToJson() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT32_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .field("active", Schema.BOOLEAN_SCHEMA)
                            .build();

            final Schema schema =
                    SchemaBuilder.struct()
                            .field("key", Schema.STRING_SCHEMA)
                            .field("data", innerSchema)
                            .build();

            final Struct innerStruct =
                    new Struct(innerSchema)
                            .put("id", 123)
                            .put("name", "Test Name")
                            .put("active", true);

            final Struct value = new Struct(schema).put("key", "key1").put("data", innerStruct);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            assertEquals("key1", resultValue.get("key"));

            final String jsonString = (String) resultValue.get("data");
            assertNotNull(jsonString);

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals(123, parsed.get("id"));
            assertEquals("Test Name", parsed.get("name"));
            assertEquals(true, parsed.get("active"));
        }

        @Test
        @DisplayName("Should transform nested struct to JSON")
        void shouldTransformNestedStructToJson() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "payload");
            transformValue.configure(config);

            final Schema addressSchema =
                    SchemaBuilder.struct()
                            .field("street", Schema.STRING_SCHEMA)
                            .field("city", Schema.STRING_SCHEMA)
                            .field("zipCode", Schema.STRING_SCHEMA)
                            .build();

            final Schema personSchema =
                    SchemaBuilder.struct()
                            .field("name", Schema.STRING_SCHEMA)
                            .field("age", Schema.INT32_SCHEMA)
                            .field("address", addressSchema)
                            .build();

            final Schema schema =
                    SchemaBuilder.struct()
                            .field("id", Schema.STRING_SCHEMA)
                            .field("payload", personSchema)
                            .build();

            final Struct address =
                    new Struct(addressSchema)
                            .put("street", "123 Main St")
                            .put("city", "Springfield")
                            .put("zipCode", "12345");

            final Struct person =
                    new Struct(personSchema)
                            .put("name", "John Doe")
                            .put("age", 30)
                            .put("address", address);

            final Struct value = new Struct(schema).put("id", "record-1").put("payload", person);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("payload");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals("John Doe", parsed.get("name"));
            assertEquals(30, parsed.get("age"));

            final Map<String, Object> parsedAddress = getMap(parsed, "address");
            assertEquals("123 Main St", parsedAddress.get("street"));
            assertEquals("Springfield", parsedAddress.get("city"));
            assertEquals("12345", parsedAddress.get("zipCode"));
        }

        @Test
        @DisplayName("Should transform struct with array to JSON")
        void shouldTransformStructWithArrayToJson() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema itemSchema =
                    SchemaBuilder.struct()
                            .field("sku", Schema.STRING_SCHEMA)
                            .field("quantity", Schema.INT32_SCHEMA)
                            .build();

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .field("orderId", Schema.STRING_SCHEMA)
                            .field("items", SchemaBuilder.array(itemSchema).build())
                            .build();

            final Schema schema = SchemaBuilder.struct().field("data", dataSchema).build();

            final Struct item1 = new Struct(itemSchema).put("sku", "SKU-001").put("quantity", 2);

            final Struct item2 = new Struct(itemSchema).put("sku", "SKU-002").put("quantity", 5);

            final Struct data =
                    new Struct(dataSchema)
                            .put("orderId", "ORD-123")
                            .put("items", Arrays.asList(item1, item2));

            final Struct value = new Struct(schema).put("data", data);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals("ORD-123", parsed.get("orderId"));

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(2, items.size());
            assertEquals("SKU-001", items.get(0).get("sku"));
            assertEquals(2, items.get(0).get("quantity"));
            assertEquals("SKU-002", items.get(1).get("sku"));
            assertEquals(5, items.get(1).get("quantity"));
        }

        @Test
        @DisplayName("Should transform struct with map to JSON")
        void shouldTransformStructWithMapToJson() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .field("name", Schema.STRING_SCHEMA)
                            .field(
                                    "properties",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                            .build())
                            .build();

            final Schema schema = SchemaBuilder.struct().field("data", dataSchema).build();

            final Map<String, String> properties = new HashMap<>();
            properties.put("color", "blue");
            properties.put("size", "large");

            final Struct data =
                    new Struct(dataSchema).put("name", "Product").put("properties", properties);

            final Struct value = new Struct(schema).put("data", data);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals("Product", parsed.get("name"));

            final Map<String, String> parsedProps = getStringMap(parsed, "properties");
            assertEquals("blue", parsedProps.get("color"));
            assertEquals("large", parsedProps.get("size"));
        }

        @Test
        @DisplayName("Should handle all primitive types")
        void shouldHandleAllPrimitiveTypes() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .field("int8Val", Schema.INT8_SCHEMA)
                            .field("int16Val", Schema.INT16_SCHEMA)
                            .field("int32Val", Schema.INT32_SCHEMA)
                            .field("int64Val", Schema.INT64_SCHEMA)
                            .field("float32Val", Schema.FLOAT32_SCHEMA)
                            .field("float64Val", Schema.FLOAT64_SCHEMA)
                            .field("boolVal", Schema.BOOLEAN_SCHEMA)
                            .field("stringVal", Schema.STRING_SCHEMA)
                            .build();

            final Schema schema = SchemaBuilder.struct().field("data", dataSchema).build();

            final Struct data =
                    new Struct(dataSchema)
                            .put("int8Val", (byte) 8)
                            .put("int16Val", (short) 16)
                            .put("int32Val", 32)
                            .put("int64Val", 64L)
                            .put("float32Val", 3.14f)
                            .put("float64Val", 2.718281828)
                            .put("boolVal", true)
                            .put("stringVal", "hello");

            final Struct value = new Struct(schema).put("data", data);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals(8, parsed.get("int8Val"));
            assertEquals(16, parsed.get("int16Val"));
            assertEquals(32, parsed.get("int32Val"));
            assertEquals(64, parsed.get("int64Val"));
            assertEquals(3.14f, ((Number) parsed.get("float32Val")).floatValue(), 0.001);
            assertEquals(2.718281828, ((Number) parsed.get("float64Val")).doubleValue(), 0.000001);
            assertEquals(true, parsed.get("boolVal"));
            assertEquals("hello", parsed.get("stringVal"));
        }

        @Test
        @DisplayName("Should preserve schema for other fields")
        void shouldPreserveSchemaForOtherFields() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("value", Schema.STRING_SCHEMA).build();

            final Schema schema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT64_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .field("data", innerSchema)
                            .build();

            final Struct inner = new Struct(innerSchema).put("value", "test");

            final Struct value =
                    new Struct(schema)
                            .put("id", 100L)
                            .put("name", "Test Record")
                            .put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Schema resultSchema = result.valueSchema();
            assertEquals(Schema.Type.INT64, resultSchema.field("id").schema().type());
            assertEquals(Schema.Type.STRING, resultSchema.field("name").schema().type());
            assertEquals(Schema.Type.STRING, resultSchema.field("data").schema().type());

            final Struct resultValue = (Struct) result.value();
            assertEquals(100L, resultValue.get("id"));
            assertEquals("Test Record", resultValue.get("name"));
        }
    }

    @Nested
    @DisplayName("Schemaless Transformation Tests")
    class SchemalessTransformationTests {

        @Test
        @DisplayName("Should transform schemaless record")
        void shouldTransformSchemalessRecord() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Map<String, Object> innerData = new HashMap<>();
            innerData.put("id", 1);
            innerData.put("name", "Test");

            final Map<String, Object> value = new HashMap<>();
            value.put("key", "key1");
            value.put("data", innerData);

            final SourceRecord record = createSchemalessValueRecord(value);
            final SourceRecord result = transformValue.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            assertEquals("key1", resultValue.get("key"));

            final String jsonString = (String) resultValue.get("data");
            assertNotNull(jsonString);

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals(1, parsed.get("id"));
            assertEquals("Test", parsed.get("name"));
        }

        @Test
        @DisplayName("Should transform nested schemaless record")
        void shouldTransformNestedSchemalessRecord() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "payload");
            transformValue.configure(config);

            final Map<String, Object> address = new HashMap<>();
            address.put("street", "456 Oak Ave");
            address.put("city", "Metropolis");

            final Map<String, Object> person = new HashMap<>();
            person.put("name", "Jane Doe");
            person.put("address", address);

            final Map<String, Object> value = new HashMap<>();
            value.put("payload", person);

            final SourceRecord record = createSchemalessValueRecord(value);
            final SourceRecord result = transformValue.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            final String jsonString = (String) resultValue.get("payload");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals("Jane Doe", parsed.get("name"));

            final Map<String, Object> parsedAddress = getMap(parsed, "address");
            assertEquals("456 Oak Ave", parsedAddress.get("street"));
        }

        @Test
        @DisplayName("Should transform schemaless record with array")
        void shouldTransformSchemalessRecordWithArray() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Map<String, Object> item1 = new HashMap<>();
            item1.put("name", "Item 1");

            final Map<String, Object> item2 = new HashMap<>();
            item2.put("name", "Item 2");

            final Map<String, Object> data = new HashMap<>();
            data.put("items", Arrays.asList(item1, item2));

            final Map<String, Object> value = new HashMap<>();
            value.put("data", data);

            final SourceRecord record = createSchemalessValueRecord(value);
            final SourceRecord result = transformValue.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(2, items.size());
            assertEquals("Item 1", items.get(0).get("name"));
            assertEquals("Item 2", items.get(1).get("name"));
        }

        @Test
        @DisplayName("Should use different output field name for schemaless record")
        void shouldUseDifferentOutputFieldNameForSchemaless() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "source");
            config.put(StructToJsonConfig.OUTPUT_FIELD_NAME_CONFIG, "target");
            transformValue.configure(config);

            final Map<String, Object> sourceData = new HashMap<>();
            sourceData.put("value", "test");

            final Map<String, Object> value = new HashMap<>();
            value.put("source", sourceData);
            value.put("other", "data");

            final SourceRecord record = createSchemalessValueRecord(value);
            final SourceRecord result = transformValue.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            assertEquals("data", resultValue.get("other"));
            assertNotNull(resultValue.get("target"));
            assertTrue(resultValue.get("target") instanceof String);
        }
    }

    @Nested
    @DisplayName("Null and Missing Field Handling Tests")
    class NullAndMissingFieldHandlingTests {

        @Test
        @DisplayName("Should throw exception for null value when skip is false")
        void shouldThrowExceptionForNullValueWhenSkipIsFalse() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, false);
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(null, null);
            assertThrows(DataException.class, () -> transformValue.apply(record));
        }

        @Test
        @DisplayName("Should pass through null value when skip is true")
        void shouldPassThroughNullValueWhenSkipIsTrue() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, true);
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(null, null);
            final SourceRecord result = transformValue.apply(record);
            assertNull(result.value());
        }

        @Test
        @DisplayName("Should throw exception for missing field when skip is false")
        void shouldThrowExceptionForMissingFieldWhenSkipIsFalse() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "nonexistent");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, false);
            transformValue.configure(config);

            final Schema schema =
                    SchemaBuilder.struct().field("other", Schema.STRING_SCHEMA).build();

            final Struct value = new Struct(schema).put("other", "value");

            final SourceRecord record = createValueRecord(schema, value);
            assertThrows(DataException.class, () -> transformValue.apply(record));
        }

        @Test
        @DisplayName("Should pass through missing field when skip is true")
        void shouldPassThroughMissingFieldWhenSkipIsTrue() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "nonexistent");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, true);
            transformValue.configure(config);

            final Schema schema =
                    SchemaBuilder.struct().field("other", Schema.STRING_SCHEMA).build();

            final Struct value = new Struct(schema).put("other", "value");

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            assertEquals("value", resultValue.get("other"));
        }

        @Test
        @DisplayName("Should throw exception for null field value when skip is false")
        void shouldThrowExceptionForNullFieldValueWhenSkipIsFalse() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, false);
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build();

            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();

            final Struct value = new Struct(schema).put("data", null);

            final SourceRecord record = createValueRecord(schema, value);
            assertThrows(DataException.class, () -> transformValue.apply(record));
        }

        @Test
        @DisplayName("Should pass through null field value when skip is true")
        void shouldPassThroughNullFieldValueWhenSkipIsTrue() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, true);
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build();

            final Schema schema =
                    SchemaBuilder.struct()
                            .field("id", Schema.STRING_SCHEMA)
                            .field("data", innerSchema)
                            .build();

            final Struct value = new Struct(schema).put("id", "123").put("data", null);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            assertEquals("123", resultValue.get("id"));
        }

        @Test
        @DisplayName("Should throw exception for missing field in schemaless when skip is false")
        void shouldThrowExceptionForMissingFieldInSchemalessWhenSkipIsFalse() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "nonexistent");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, false);
            transformValue.configure(config);

            final Map<String, Object> value = new HashMap<>();
            value.put("other", "value");

            final SourceRecord record = createSchemalessValueRecord(value);
            assertThrows(DataException.class, () -> transformValue.apply(record));
        }

        @Test
        @DisplayName("Should pass through missing field in schemaless when skip is true")
        void shouldPassThroughMissingFieldInSchemalessWhenSkipIsTrue() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "nonexistent");
            config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, true);
            transformValue.configure(config);

            final Map<String, Object> value = new HashMap<>();
            value.put("other", "value");

            final SourceRecord record = createSchemalessValueRecord(value);
            final SourceRecord result = transformValue.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultValue = (Map<String, Object>) result.value();
            assertEquals("value", resultValue.get("other"));
        }
    }

    @Nested
    @DisplayName("Key Transformation Tests")
    class KeyTransformationTests {

        @Test
        @DisplayName("Should transform key with schema")
        void shouldTransformKeyWithSchema() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "keyData");
            transformKey.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT32_SCHEMA)
                            .field("type", Schema.STRING_SCHEMA)
                            .build();

            final Schema keySchema = SchemaBuilder.struct().field("keyData", innerSchema).build();

            final Struct innerStruct = new Struct(innerSchema).put("id", 42).put("type", "primary");

            final Struct key = new Struct(keySchema).put("keyData", innerStruct);

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            keySchema,
                            key,
                            Schema.STRING_SCHEMA,
                            "value");

            final SourceRecord result = transformKey.apply(record);

            final Struct resultKey = (Struct) result.key();
            final String jsonString = (String) resultKey.get("keyData");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals(42, parsed.get("id"));
            assertEquals("primary", parsed.get("type"));
            assertEquals("value", result.value());
        }

        @Test
        @DisplayName("Should transform schemaless key")
        void shouldTransformSchemalessKey() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "keyData");
            transformKey.configure(config);

            final Map<String, Object> keyData = new HashMap<>();
            keyData.put("id", 99);
            keyData.put("region", "west");

            final Map<String, Object> key = new HashMap<>();
            key.put("keyData", keyData);

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            null,
                            key,
                            null,
                            "value");

            final SourceRecord result = transformKey.apply(record);

            @SuppressWarnings("unchecked")
            final Map<String, Object> resultKey = (Map<String, Object>) result.key();
            final String jsonString = (String) resultKey.get("keyData");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});
            assertEquals(99, parsed.get("id"));
            assertEquals("west", parsed.get("region"));
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should throw exception for non-struct value with schema")
        void shouldThrowExceptionForNonStructValueWithSchema() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            null,
                            null,
                            Schema.STRING_SCHEMA,
                            "not a struct");

            assertThrows(DataException.class, () -> transformValue.apply(record));
        }

        @Test
        @DisplayName("Should throw exception for non-map value without schema")
        void shouldThrowExceptionForNonMapValueWithoutSchema() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.emptyMap(),
                            TOPIC,
                            0,
                            null,
                            null,
                            null,
                            "not a map");

            assertThrows(DataException.class, () -> transformValue.apply(record));
        }
    }

    @Nested
    @DisplayName("JSON Validity and Round-Trip Tests")
    class JsonValidityTests {

        @Test
        @DisplayName("Should produce valid JSON without escape characters for simple strings")
        void shouldProduceValidJsonWithoutExtraEscapeCharacters() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();

            final Struct inner = new Struct(innerSchema).put("text", "Hello, World!");

            final Struct value = new Struct(schema).put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("Hello, World!", parsed.get("text"));
            assertTrue(jsonString.contains("\"text\":\"Hello, World!\""));
        }

        @Test
        @DisplayName("Should handle special characters in JSON properly")
        void shouldHandleSpecialCharactersInJsonProperly() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();

            // String with characters that need escaping in JSON
            final String specialText = "Line1\nLine2\tTabbed\r\nNewLine\"Quoted\"";

            final Struct inner = new Struct(innerSchema).put("text", specialText);

            final Struct value = new Struct(schema).put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals(specialText, parsed.get("text"));
        }

        @Test
        @DisplayName("Should handle Unicode characters properly")
        void shouldHandleUnicodeCharactersProperly() throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();

            final String unicodeText = "Hello 世界 🌍 Привет мир";

            final Struct inner = new Struct(innerSchema).put("text", unicodeText);

            final Struct value = new Struct(schema).put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("data");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals(unicodeText, parsed.get("text"));
        }

        @Test
        @DisplayName("Should produce JSON that can be parsed back to original structure")
        void shouldProduceJsonThatCanBeParsedBackToOriginalStructure()
                throws JsonProcessingException {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "payload");
            transformValue.configure(config);

            final Schema metadataSchema =
                    SchemaBuilder.struct()
                            .field("version", Schema.INT32_SCHEMA)
                            .field("timestamp", Schema.INT64_SCHEMA)
                            .build();

            final Schema itemSchema =
                    SchemaBuilder.struct()
                            .field("name", Schema.STRING_SCHEMA)
                            .field("price", Schema.FLOAT64_SCHEMA)
                            .build();

            final Schema orderSchema =
                    SchemaBuilder.struct()
                            .field("orderId", Schema.STRING_SCHEMA)
                            .field("items", SchemaBuilder.array(itemSchema).build())
                            .field("metadata", metadataSchema)
                            .build();

            final Schema schema = SchemaBuilder.struct().field("payload", orderSchema).build();

            final Struct metadata =
                    new Struct(metadataSchema).put("version", 1).put("timestamp", 1234567890L);

            final Struct item1 = new Struct(itemSchema).put("name", "Widget").put("price", 9.99);

            final Struct item2 = new Struct(itemSchema).put("name", "Gadget").put("price", 19.99);

            final Struct order =
                    new Struct(orderSchema)
                            .put("orderId", "ORD-001")
                            .put("items", Arrays.asList(item1, item2))
                            .put("metadata", metadata);

            final Struct value = new Struct(schema).put("payload", order);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("payload");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("ORD-001", parsed.get("orderId"));

            final List<Map<String, Object>> parsedItems = getListOfMaps(parsed, "items");
            assertEquals(2, parsedItems.size());
            assertEquals("Widget", parsedItems.get(0).get("name"));
            assertEquals(9.99, ((Number) parsedItems.get(0).get("price")).doubleValue(), 0.001);
            assertEquals("Gadget", parsedItems.get(1).get("name"));
            assertEquals(19.99, ((Number) parsedItems.get(1).get("price")).doubleValue(), 0.001);

            final Map<String, Object> parsedMetadata = getMap(parsed, "metadata");
            assertEquals(1, parsedMetadata.get("version"));
            assertEquals(1234567890L, ((Number) parsedMetadata.get("timestamp")).longValue());

            final String reserializedJson = OBJECT_MAPPER.writeValueAsString(parsed);
            final Map<String, Object> reparsed =
                    OBJECT_MAPPER.readValue(
                            reserializedJson, new TypeReference<Map<String, Object>>() {});
            assertEquals(parsed, reparsed);
        }
    }

    @Nested
    @DisplayName("Schema Caching Tests")
    class SchemaCachingTests {

        @Test
        @DisplayName("Should reuse cached schema for same input schema")
        void shouldReuseCachedSchemaForSameInputSchema() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "data");
            transformValue.configure(config);

            final Schema innerSchema =
                    SchemaBuilder.struct().field("value", Schema.STRING_SCHEMA).build();

            final Schema schema = SchemaBuilder.struct().field("data", innerSchema).build();

            final Struct inner1 = new Struct(innerSchema).put("value", "test1");
            final Struct value1 = new Struct(schema).put("data", inner1);

            final Struct inner2 = new Struct(innerSchema).put("value", "test2");
            final Struct value2 = new Struct(schema).put("data", inner2);

            final SourceRecord record1 = createValueRecord(schema, value1);
            final SourceRecord record2 = createValueRecord(schema, value2);

            final SourceRecord result1 = transformValue.apply(record1);
            final SourceRecord result2 = transformValue.apply(record2);

            assertTrue(
                    result1.valueSchema() == result2.valueSchema(),
                    "Schema should be reused from cache");
        }
    }

    @Nested
    @DisplayName("Schema evolution tests")
    class SchemaEvolutionTests {

        private static final String PROTOBUF_TAG_PARAM = "io.confluent.connect.protobuf.Tag";

        @Test
        @DisplayName("Should change transformed field from STRUCT to OPTIONAL_STRING in schema")
        void shouldChangeTransformedFieldTypeInSchema() {
            configureTransform("data");

            final Schema innerSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("id", Schema.INT32_SCHEMA)
                            .field("data", innerSchema)
                            .build();
            final Struct inner = new Struct(innerSchema).put("x", "value");
            final Struct value = new Struct(schema).put("id", 1).put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Schema beforeSchema = record.valueSchema();
            final Schema afterSchema = result.valueSchema();

            assertEquals(2, beforeSchema.fields().size());
            assertEquals(2, afterSchema.fields().size());
            assertEquals(Schema.Type.STRUCT, beforeSchema.field("data").schema().type());
            assertEquals(Schema.Type.STRING, afterSchema.field("data").schema().type());
            assertTrue(afterSchema.field("data").schema().isOptional());
        }

        @Test
        @DisplayName("Should add new output field when input != output")
        void shouldAddNewOutputFieldWhenInputDifferentFromOutput() {
            configureTransform("payload", "payload_json");

            final Schema payloadSchema =
                    SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("payload", payloadSchema)
                            .field("id", Schema.INT32_SCHEMA)
                            .build();
            final Struct payload = new Struct(payloadSchema).put("x", "v");
            final Struct value = new Struct(schema).put("payload", payload).put("id", 1);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Schema beforeSchema = record.valueSchema();
            final Schema afterSchema = result.valueSchema();

            assertEquals(2, beforeSchema.fields().size());
            assertEquals(3, afterSchema.fields().size());
            assertNotNull(afterSchema.field("payload"));
            assertNotNull(afterSchema.field("payload_json"));
            assertNotNull(afterSchema.field("id"));
            assertEquals(Schema.Type.STRING, afterSchema.field("payload_json").schema().type());
        }

        @Test
        @DisplayName("Should preserve Protobuf Tag when replacing field in-place")
        void shouldPreserveProtobufTagWhenReplacingInPlace() {
            configureTransform("data");

            final Schema innerSchema =
                    SchemaBuilder.struct()
                            .field("x", Schema.STRING_SCHEMA)
                            .parameter(PROTOBUF_TAG_PARAM, "2")
                            .build();
            final Schema idSchema =
                    SchemaBuilder.int32().parameter(PROTOBUF_TAG_PARAM, "1").build();
            final Schema schema =
                    SchemaBuilder.struct().field("id", idSchema).field("data", innerSchema).build();
            final Struct inner = new Struct(innerSchema).put("x", "v");
            final Struct value = new Struct(schema).put("id", 1).put("data", inner);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Schema afterSchema = result.valueSchema();
            final Schema transformedFieldSchema = afterSchema.field("data").schema();
            assertNotNull(
                    transformedFieldSchema.parameters(),
                    "Transformed field schema should have parameters");
            assertEquals(
                    "2",
                    transformedFieldSchema.parameters().get(PROTOBUF_TAG_PARAM),
                    "Protobuf Tag should be preserved when replacing in-place");
        }

        @Test
        @DisplayName("Should assign unique Protobuf Tag when adding new output field")
        void shouldAssignUniqueProtobufTagWhenAddingNewField() {
            configureTransform("payload", "payload_json");

            final Schema payloadSchema =
                    SchemaBuilder.struct()
                            .field("x", Schema.STRING_SCHEMA)
                            .parameter(PROTOBUF_TAG_PARAM, "2")
                            .build();
            final Schema idSchema =
                    SchemaBuilder.int32().parameter(PROTOBUF_TAG_PARAM, "3").build();
            final Schema schema =
                    SchemaBuilder.struct()
                            .field("payload", payloadSchema)
                            .field("id", idSchema)
                            .build();
            final Struct payload = new Struct(payloadSchema).put("x", "v");
            final Struct value = new Struct(schema).put("payload", payload).put("id", 1);

            final SourceRecord record = createValueRecord(schema, value);
            final SourceRecord result = transformValue.apply(record);

            final Schema afterSchema = result.valueSchema();
            final Schema newFieldSchema = afterSchema.field("payload_json").schema();
            assertNotNull(newFieldSchema.parameters());
            final String newTag = newFieldSchema.parameters().get(PROTOBUF_TAG_PARAM);
            assertNotNull(newTag, "New field should have Protobuf Tag assigned");
            int tagNum = Integer.parseInt(newTag);
            assertTrue(tagNum > 3, "New field Tag should be greater than max existing (3)");
        }
    }

    @Nested
    @DisplayName("Protobuf-like Structure Tests")
    class ProtobufLikeStructureTests {

        @Test
        @DisplayName("Should transform deeply nested protobuf-like structure")
        void shouldTransformDeeplyNestedProtobufLikeStructure() throws JsonProcessingException {
            final Schema metadataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Metadata")
                            .field("event_id", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                            .build();

            final Schema addressSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Address")
                            .field("street", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("city", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("country", Schema.OPTIONAL_STRING_SCHEMA)
                            .build();

            final Schema userSchema =
                    SchemaBuilder.struct()
                            .name("com.example.User")
                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("email", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("address", addressSchema)
                            .build();

            final Schema eventSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Event")
                            .field("metadata", metadataSchema)
                            .field("user", userSchema)
                            .build();

            final Schema wrapperSchema = SchemaBuilder.struct().field("event", eventSchema).build();

            final Struct metadata =
                    new Struct(metadataSchema)
                            .put("event_id", "evt-12345")
                            .put("timestamp", 1704067200000L);

            final Struct address =
                    new Struct(addressSchema)
                            .put("street", "123 Proto Street")
                            .put("city", "Buffertown")
                            .put("country", "Serializia");

            final Struct user =
                    new Struct(userSchema)
                            .put("name", "Proto User")
                            .put("email", "proto@example.com")
                            .put("address", address);

            final Struct event =
                    new Struct(eventSchema).put("metadata", metadata).put("user", user);

            final Struct wrapper = new Struct(wrapperSchema).put("event", event);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "event");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("event");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            final Map<String, Object> parsedMetadata = getMap(parsed, "metadata");
            assertEquals("evt-12345", parsedMetadata.get("event_id"));
            assertEquals(1704067200000L, ((Number) parsedMetadata.get("timestamp")).longValue());

            final Map<String, Object> parsedUser = getMap(parsed, "user");
            assertEquals("Proto User", parsedUser.get("name"));

            final Map<String, Object> parsedAddress = getMap(parsedUser, "address");
            assertEquals("123 Proto Street", parsedAddress.get("street"));
            assertEquals("Buffertown", parsedAddress.get("city"));
        }

        @Test
        @DisplayName("Should transform protobuf-like repeated message field")
        void shouldTransformProtobufLikeRepeatedMessageField() throws JsonProcessingException {
            final Schema itemSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Item")
                            .field("sku", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("quantity", Schema.OPTIONAL_INT32_SCHEMA)
                            .field("price", Schema.OPTIONAL_FLOAT64_SCHEMA)
                            .build();

            final Schema orderSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Order")
                            .field("order_id", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("items", SchemaBuilder.array(itemSchema).optional().build())
                            .field("total", Schema.OPTIONAL_FLOAT64_SCHEMA)
                            .build();

            final Schema wrapperSchema = SchemaBuilder.struct().field("order", orderSchema).build();

            final Struct item1 =
                    new Struct(itemSchema)
                            .put("sku", "PROTO-001")
                            .put("quantity", 3)
                            .put("price", 15.99);

            final Struct item2 =
                    new Struct(itemSchema)
                            .put("sku", "PROTO-002")
                            .put("quantity", 1)
                            .put("price", 99.99);

            final Struct order =
                    new Struct(orderSchema)
                            .put("order_id", "ORD-PROTO-123")
                            .put("items", Arrays.asList(item1, item2))
                            .put("total", 147.96);

            final Struct wrapper = new Struct(wrapperSchema).put("order", order);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "order");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("order");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("ORD-PROTO-123", parsed.get("order_id"));

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(2, items.size());
            assertEquals("PROTO-001", items.get(0).get("sku"));
            assertEquals(3, items.get(0).get("quantity"));
            assertEquals("PROTO-002", items.get(1).get("sku"));
        }

        @Test
        @DisplayName("Should transform protobuf-like map field")
        void shouldTransformProtobufLikeMapField() throws JsonProcessingException {
            final Schema configSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Config")
                            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                            .field(
                                    "settings",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                            .optional()
                                            .build())
                            .field(
                                    "counts",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                                            .optional()
                                            .build())
                            .build();

            final Schema wrapperSchema =
                    SchemaBuilder.struct().field("config", configSchema).build();

            final Map<String, String> settings = new HashMap<>();
            settings.put("theme", "dark");
            settings.put("language", "en");
            settings.put("region", "us-west");

            final Map<String, Integer> counts = new HashMap<>();
            counts.put("errors", 0);
            counts.put("warnings", 5);
            counts.put("infos", 100);

            final Struct configStruct =
                    new Struct(configSchema)
                            .put("name", "app-config")
                            .put("settings", settings)
                            .put("counts", counts);

            final Struct wrapper = new Struct(wrapperSchema).put("config", configStruct);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "config");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("config");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("app-config", parsed.get("name"));

            final Map<String, String> parsedSettings = getStringMap(parsed, "settings");
            assertEquals("dark", parsedSettings.get("theme"));
            assertEquals("en", parsedSettings.get("language"));

            final Map<String, Integer> parsedCounts = getIntegerMap(parsed, "counts");
            assertEquals(0, parsedCounts.get("errors"));
            assertEquals(5, parsedCounts.get("warnings"));
        }

        @Test
        @DisplayName("Should transform protobuf-like enum as string")
        void shouldTransformProtobufLikeEnumAsString() throws JsonProcessingException {
            final Schema statusSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Status")
                            .field("code", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("message", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("severity", Schema.OPTIONAL_STRING_SCHEMA)
                            .build();

            final Schema wrapperSchema =
                    SchemaBuilder.struct().field("status", statusSchema).build();

            final Struct status =
                    new Struct(statusSchema)
                            .put("code", "STATUS_OK")
                            .put("message", "Operation completed successfully")
                            .put("severity", "SEVERITY_INFO");

            final Struct wrapper = new Struct(wrapperSchema).put("status", status);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "status");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("status");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("STATUS_OK", parsed.get("code"));
            assertEquals("Operation completed successfully", parsed.get("message"));
            assertEquals("SEVERITY_INFO", parsed.get("severity"));
        }

        @Test
        @DisplayName("Should handle protobuf-like optional fields with null values")
        void shouldHandleProtobufLikeOptionalFieldsWithNullValues() throws JsonProcessingException {
            final Schema messageSchema =
                    SchemaBuilder.struct()
                            .name("com.example.OptionalFieldsMessage")
                            .field("required_field", Schema.STRING_SCHEMA)
                            .field("optional_string", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("optional_int", Schema.OPTIONAL_INT32_SCHEMA)
                            .field(
                                    "optional_nested",
                                    SchemaBuilder.struct()
                                            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
                                            .optional()
                                            .build())
                            .build();

            final Schema wrapperSchema =
                    SchemaBuilder.struct().field("message", messageSchema).build();

            final Struct message =
                    new Struct(messageSchema)
                            .put("required_field", "I am required")
                            .put("optional_string", null)
                            .put("optional_int", null)
                            .put("optional_nested", null);

            final Struct wrapper = new Struct(wrapperSchema).put("message", message);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "message");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("message");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("I am required", parsed.get("required_field"));
            assertTrue(parsed.containsKey("optional_string"));
            assertTrue(parsed.containsKey("optional_int"));
        }

        @Test
        @DisplayName("Should handle protobuf-like bytes field")
        void shouldHandleProtobufLikeBytesField() throws JsonProcessingException {
            final Schema messageSchema =
                    SchemaBuilder.struct()
                            .name("com.example.BytesMessage")
                            .field("id", Schema.STRING_SCHEMA)
                            .field("data", Schema.OPTIONAL_BYTES_SCHEMA)
                            .build();

            final Schema wrapperSchema =
                    SchemaBuilder.struct().field("message", messageSchema).build();

            final byte[] binaryData = "Hello Binary World!".getBytes();

            final Struct message =
                    new Struct(messageSchema).put("id", "bytes-123").put("data", binaryData);

            final Struct wrapper = new Struct(wrapperSchema).put("message", message);

            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "message");
            transformValue.configure(config);

            final SourceRecord record = createValueRecord(wrapperSchema, wrapper);
            final SourceRecord result = transformValue.apply(record);

            final Struct resultValue = (Struct) result.value();
            final String jsonString = (String) resultValue.get("message");

            final Map<String, Object> parsed =
                    OBJECT_MAPPER.readValue(
                            jsonString, new TypeReference<Map<String, Object>>() {});

            assertEquals("bytes-123", parsed.get("id"));
            assertNotNull(parsed.get("data"));
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

    private SourceRecord createSchemalessValueRecord(final Object value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                TOPIC,
                0,
                null,
                null,
                null,
                value,
                null);
    }
}
