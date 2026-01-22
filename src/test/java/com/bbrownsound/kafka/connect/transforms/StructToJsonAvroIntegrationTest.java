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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.connect.avro.AvroData;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StructToJsonAvroIntegrationTest extends BaseTransformTest {

    private static final String TOPIC = "avro-test-topic";
    private static final AvroData AVRO_DATA = new AvroData(100);

    @Override
    protected String getTopicName() {
        return TOPIC;
    }

    @Nested
    @DisplayName("Simple Avro Record Tests")
    class SimpleAvroRecordTests {

        @Test
        @DisplayName("Should transform simple Avro record to JSON")
        void shouldTransformSimpleAvroRecordToJson() throws Exception {
            final org.apache.avro.Schema avroSchema = loadAvroSchema("avro/simple_record.avsc");

            final GenericRecord avroRecord = new GenericData.Record(avroSchema);
            avroRecord.put("id", 42);
            avroRecord.put("name", "Test Product");
            avroRecord.put("active", true);
            avroRecord.put("score", 95.5);

            final Struct wrapper =
                    wrapStruct(
                            "data",
                            toConnectStruct(avroRecord),
                            "topic",
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            TOPIC);

            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(42, parsed.get("id"));
            assertEquals("Test Product", parsed.get("name"));
            assertEquals(true, parsed.get("active"));
            assertEquals(95.5, ((Number) parsed.get("score")).doubleValue(), 0.001);
        }
    }

    @Nested
    @DisplayName("Nested Avro Record Tests")
    class NestedAvroRecordTests {

        @Test
        @DisplayName("Should transform Address field within Person to JSON")
        void shouldTransformAddressFieldWithinPersonToJson() throws Exception {
            final org.apache.avro.Schema personSchema = loadAvroSchema("avro/person.avsc");
            final org.apache.avro.Schema addressSchema = personSchema.getField("address").schema();

            final GenericRecord address = new GenericData.Record(addressSchema);
            address.put("street", "123 Avro Lane");
            address.put("city", "Schemaville");
            address.put("state", "CA");
            address.put("zip_code", "94105");
            address.put("country", "USA");

            final GenericRecord person = new GenericData.Record(personSchema);
            person.put("first_name", "Jane");
            person.put("last_name", "Avro");
            person.put("age", 28);
            person.put("email", "jane@avro.io");
            person.put("address", address);
            person.put("phone_numbers", java.util.Arrays.asList("415-555-1234", "415-555-5678"));

            final String expectedAddressJson =
                    "{\"street\":\"123 Avro Lane\","
                            + "\"city\":\"Schemaville\","
                            + "\"state\":\"CA\","
                            + "\"zip_code\":\"94105\","
                            + "\"country\":\"USA\"}";

            final Struct personStruct = toConnectStruct(person);
            final TransformResult result = transformWithResult("address", personStruct);

            assertEquals("Jane", result.struct().get("first_name"));
            assertEquals("Avro", result.struct().get("last_name"));
            assertEquals(28, result.struct().get("age"));
            assertEquals("jane@avro.io", result.struct().get("email"));

            final List<String> phoneNumbers = getStringList(result.struct(), "phone_numbers");
            assertEquals(2, phoneNumbers.size());
            assertEquals("415-555-1234", phoneNumbers.get(0));
            assertEquals("415-555-5678", phoneNumbers.get(1));

            assertNotNull(result.json());
            assertJsonEquals(expectedAddressJson, result.json());
        }
    }

    @Nested
    @DisplayName("Avro Array and Enum Tests")
    class AvroArrayAndEnumTests {

        @Test
        @DisplayName("Should transform Order record with array of items and enum status")
        void shouldTransformOrderRecordWithArrayOfItemsAndEnumStatus() throws Exception {
            final org.apache.avro.Schema orderSchema = loadAvroSchema("avro/order.avsc");
            final org.apache.avro.Schema itemSchema =
                    orderSchema.getField("items").schema().getElementType();
            final org.apache.avro.Schema statusSchema = orderSchema.getField("status").schema();

            final GenericRecord item1 = new GenericData.Record(itemSchema);
            item1.put("sku", "AVRO-001");
            item1.put("product_name", "Avro Widget");
            item1.put("quantity", 3);
            item1.put("unit_price", 19.99);

            final GenericRecord item2 = new GenericData.Record(itemSchema);
            item2.put("sku", "AVRO-002");
            item2.put("product_name", "Schema Gadget");
            item2.put("quantity", 1);
            item2.put("unit_price", 49.99);

            final GenericRecord order = new GenericData.Record(orderSchema);
            order.put("order_id", "ORD-AVRO-001");
            order.put("customer_id", "CUST-AVRO-123");
            order.put("items", java.util.Arrays.asList(item1, item2));
            order.put("total_amount", 109.96);
            order.put("status", new GenericData.EnumSymbol(statusSchema, "SHIPPED"));
            order.put("created_at", 1704067200000L);

            final Struct wrapper = wrapStruct("order", toConnectStruct(order));
            final String jsonString = transformAndGetJson("order", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("ORD-AVRO-001", parsed.get("order_id"));
            assertEquals("SHIPPED", parsed.get("status"));
            assertEquals(109.96, ((Number) parsed.get("total_amount")).doubleValue(), 0.001);

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(2, items.size());
            assertEquals("AVRO-001", items.get(0).get("sku"));
            assertEquals("Avro Widget", items.get(0).get("product_name"));
            assertEquals(3, items.get(0).get("quantity"));
        }
    }

    @Nested
    @DisplayName("Avro Map Tests")
    class AvroMapTests {

        @Test
        @DisplayName("Should transform UserPreferences record with map fields")
        void shouldTransformUserPreferencesRecordWithMapFields() throws Exception {
            final org.apache.avro.Schema prefsSchema = loadAvroSchema("avro/user_preferences.avsc");

            final Map<String, String> settings = new HashMap<>();
            settings.put("theme", "dark");
            settings.put("language", "en-US");
            settings.put("notifications", "enabled");

            final Map<String, Integer> featureFlags = new HashMap<>();
            featureFlags.put("new_ui", 1);
            featureFlags.put("beta_features", 0);
            featureFlags.put("analytics", 1);

            final GenericRecord prefs = new GenericData.Record(prefsSchema);
            prefs.put("user_id", "user-avro-456");
            prefs.put("settings", settings);
            prefs.put("feature_flags", featureFlags);

            final Struct wrapper = wrapStruct("preferences", toConnectStruct(prefs));
            final String jsonString = transformAndGetJson("preferences", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("user-avro-456", parsed.get("user_id"));

            final Map<String, String> parsedSettings = getStringMap(parsed, "settings");
            assertEquals("dark", parsedSettings.get("theme"));
            assertEquals("en-US", parsedSettings.get("language"));

            final Map<String, Integer> parsedFlags = getIntegerMap(parsed, "feature_flags");
            assertEquals(1, parsedFlags.get("new_ui"));
            assertEquals(0, parsedFlags.get("beta_features"));
        }
    }

    @Nested
    @DisplayName("Avro Union (Nullable) Tests")
    class AvroUnionTests {

        @Test
        @DisplayName("Should handle Avro nullable fields with values")
        void shouldHandleAvroNullableFieldsWithValues() throws Exception {
            final org.apache.avro.Schema nullableSchema =
                    loadAvroSchema("avro/nullable_record.avsc");
            final org.apache.avro.Schema nestedSchema =
                    nullableSchema.getField("optional_nested").schema().getTypes().get(1);

            final GenericRecord nested = new GenericData.Record(nestedSchema);
            nested.put("value", "nested value");

            final GenericRecord record = new GenericData.Record(nullableSchema);
            record.put("required_field", "I am required");
            record.put("optional_string", "I am optional");
            record.put("optional_int", 42);
            record.put("optional_long", 9876543210L);
            record.put("optional_double", 3.14159);
            record.put("optional_boolean", true);
            record.put("optional_nested", nested);

            final Struct wrapper = wrapStruct("record", toConnectStruct(record));
            final String jsonString = transformAndGetJson("record", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("I am required", parsed.get("required_field"));
            assertEquals("I am optional", parsed.get("optional_string"));
            assertEquals(42, parsed.get("optional_int"));
            assertEquals(9876543210L, ((Number) parsed.get("optional_long")).longValue());
            assertEquals(3.14159, ((Number) parsed.get("optional_double")).doubleValue(), 0.00001);
            assertEquals(true, parsed.get("optional_boolean"));

            final Map<String, Object> parsedNested = getMap(parsed, "optional_nested");
            assertEquals("nested value", parsedNested.get("value"));
        }

        @Test
        @DisplayName("Should handle Avro nullable fields with null values")
        void shouldHandleAvroNullableFieldsWithNullValues() throws Exception {
            final org.apache.avro.Schema nullableSchema =
                    loadAvroSchema("avro/nullable_record.avsc");

            final GenericRecord record = new GenericData.Record(nullableSchema);
            record.put("required_field", "Only required");
            record.put("optional_string", null);
            record.put("optional_int", null);
            record.put("optional_long", null);
            record.put("optional_double", null);
            record.put("optional_boolean", null);
            record.put("optional_nested", null);

            final Struct wrapper = wrapStruct("record", toConnectStruct(record));
            final String jsonString = transformAndGetJson("record", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Only required", parsed.get("required_field"));
            assertTrue(parsed.containsKey("optional_string"));
        }
    }

    @Nested
    @DisplayName("Avro All Types Tests")
    class AvroAllTypesTests {

        @Test
        @DisplayName("Should handle all Avro primitive types")
        void shouldHandleAllAvroPrimitiveTypes() throws Exception {
            final org.apache.avro.Schema allTypesSchema =
                    loadAvroSchema("avro/all_types_record.avsc");
            final org.apache.avro.Schema enumSchema = allTypesSchema.getField("enum_val").schema();
            final org.apache.avro.Schema fixedSchema =
                    allTypesSchema.getField("fixed_val").schema();
            final org.apache.avro.Schema nestedSchema =
                    allTypesSchema.getField("nested_record").schema();

            final GenericRecord nested = new GenericData.Record(nestedSchema);
            nested.put("inner_value", "nested content");

            final GenericRecord record = new GenericData.Record(allTypesSchema);
            record.put("nullable_string", "I can be null");
            record.put("boolean_val", true);
            record.put("int_val", Integer.MAX_VALUE);
            record.put("long_val", Long.MAX_VALUE);
            record.put("float_val", 3.14f);
            record.put("double_val", 2.718281828);
            record.put("bytes_val", ByteBuffer.wrap("hello bytes".getBytes()));
            record.put("string_val", "hello string");
            record.put("array_val", java.util.Arrays.asList("one", "two", "three"));

            final Map<String, Integer> mapVal = new HashMap<>();
            mapVal.put("a", 1);
            mapVal.put("b", 2);
            record.put("map_val", mapVal);

            record.put("enum_val", new GenericData.EnumSymbol(enumSchema, "GREEN"));
            record.put("fixed_val", new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3, 4}));
            record.put("nested_record", nested);

            final Struct wrapper = wrapStruct("data", toConnectStruct(record));
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(true, parsed.get("boolean_val"));
            assertEquals(Integer.MAX_VALUE, ((Number) parsed.get("int_val")).intValue());
            assertEquals(Long.MAX_VALUE, ((Number) parsed.get("long_val")).longValue());
            assertEquals(3.14f, ((Number) parsed.get("float_val")).floatValue(), 0.001);
            assertEquals(2.718281828, ((Number) parsed.get("double_val")).doubleValue(), 0.000001);
            assertEquals("hello string", parsed.get("string_val"));
            assertEquals("GREEN", parsed.get("enum_val"));

            final List<String> arrayVal = getStringList(parsed, "array_val");
            assertEquals(3, arrayVal.size());
            assertEquals("one", arrayVal.get(0));

            final Map<String, Integer> parsedMap = getIntegerMap(parsed, "map_val");
            assertEquals(1, parsedMap.get("a"));

            final Map<String, Object> parsedNested = getMap(parsed, "nested_record");
            assertEquals("nested content", parsedNested.get("inner_value"));
        }
    }

    @Nested
    @DisplayName("Standalone Address Schema Tests")
    class StandaloneAddressSchemaTests {

        @Test
        @DisplayName("Should transform standalone Address record to JSON")
        void shouldTransformStandaloneAddressRecordToJson() throws Exception {
            final org.apache.avro.Schema addressSchema = loadAvroSchema("avro/address.avsc");

            final GenericRecord address = new GenericData.Record(addressSchema);
            address.put("street", "742 Evergreen Terrace");
            address.put("city", "Springfield");
            address.put("state", "OR");
            address.put("zip_code", "97403");
            address.put("country", "USA");

            final Struct wrapper =
                    wrapStruct(
                            "shipping_address",
                            toConnectStruct(address),
                            "order_id",
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            "ORD-12345");

            final TransformResult result = transformWithResult("shipping_address", wrapper);

            assertEquals("ORD-12345", result.struct().get("order_id"));
            assertNotNull(result.json());

            final Map<String, Object> parsed = result.parsedJson();
            assertEquals("742 Evergreen Terrace", parsed.get("street"));
            assertEquals("Springfield", parsed.get("city"));
            assertEquals("OR", parsed.get("state"));
            assertEquals("97403", parsed.get("zip_code"));
            assertEquals("USA", parsed.get("country"));
        }

        @Test
        @DisplayName("Should transform Address to different output field")
        void shouldTransformAddressToDifferentOutputField() throws Exception {
            final org.apache.avro.Schema addressSchema = loadAvroSchema("avro/address.avsc");

            final GenericRecord address = new GenericData.Record(addressSchema);
            address.put("street", "1600 Pennsylvania Avenue");
            address.put("city", "Washington");
            address.put("state", "DC");
            address.put("zip_code", "20500");
            address.put("country", "USA");

            final Struct wrapper =
                    wrapStruct(
                            "address",
                            toConnectStruct(address),
                            "name",
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            "White House");

            final String jsonString = transformAndGetJson("address", "address_json", wrapper);

            final Struct result = applyTransform(wrapper.schema(), wrapper);
            assertNotNull(result.get("address"));
            assertNotNull(jsonString);

            final Map<String, Object> parsed = parseJson(jsonString);
            assertEquals("1600 Pennsylvania Avenue", parsed.get("street"));
            assertEquals("Washington", parsed.get("city"));
        }
    }

    @Nested
    @DisplayName("Avro Logical Types Tests")
    class AvroLogicalTypesTests {

        @Test
        @DisplayName("Should transform record with Avro logical types to JSON")
        void shouldTransformRecordWithAvroLogicalTypesToJson() throws Exception {
            final org.apache.avro.Schema logicalTypesSchema =
                    loadAvroSchema("avro/logical_types_record.avsc");

            final GenericRecord record = new GenericData.Record(logicalTypesSchema);
            record.put("id", "lt-001");
            record.put("created_date", 19737);
            record.put("created_timestamp_millis", 1705312800000L);
            record.put("created_timestamp_micros", 1705312800000000L);
            record.put("duration_millis", 36000000L);

            final java.math.BigDecimal amount = new java.math.BigDecimal("1234.56");
            record.put("amount", ByteBuffer.wrap(amount.unscaledValue().toByteArray()));
            record.put("unique_id", "550e8400-e29b-41d4-a716-446655440000");

            final Struct wrapper = wrapStruct("transaction", toConnectStruct(record));
            final String jsonString = transformAndGetJson("transaction", wrapper);

            assertNotNull(jsonString);

            final Map<String, Object> parsed = parseJson(jsonString);
            assertEquals("lt-001", parsed.get("id"));
            assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed.get("unique_id"));
            assertTrue(parsed.containsKey("created_date"));
            assertTrue(parsed.containsKey("created_timestamp_millis"));
            assertTrue(parsed.containsKey("created_timestamp_micros"));
            assertTrue(parsed.containsKey("duration_millis"));
            assertTrue(parsed.containsKey("amount"));
        }

        @Test
        @DisplayName("Should handle logical types with typical values")
        void shouldHandleLogicalTypesWithTypicalValues() throws Exception {
            final org.apache.avro.Schema logicalTypesSchema =
                    loadAvroSchema("avro/logical_types_record.avsc");

            final GenericRecord record = new GenericData.Record(logicalTypesSchema);
            record.put("id", "lt-002");
            record.put("created_date", 0);
            record.put("created_timestamp_millis", 0L);
            record.put("created_timestamp_micros", 0L);
            record.put("duration_millis", 0L);
            record.put(
                    "amount",
                    ByteBuffer.wrap(java.math.BigDecimal.ZERO.unscaledValue().toByteArray()));
            record.put("unique_id", "00000000-0000-0000-0000-000000000000");

            final Struct wrapper = wrapStruct("data", toConnectStruct(record));
            final String jsonString = transformAndGetJson("data", wrapper);

            assertNotNull(jsonString);

            final Map<String, Object> parsed = parseJson(jsonString);
            assertEquals("lt-002", parsed.get("id"));
            assertEquals("00000000-0000-0000-0000-000000000000", parsed.get("unique_id"));
        }
    }

    @Nested
    @DisplayName("Avro Round-Trip Tests")
    class AvroRoundTripTests {

        @Test
        @DisplayName("Should produce JSON that can be re-serialized identically")
        void shouldProduceJsonThatCanBeReserializedIdentically() throws Exception {
            final org.apache.avro.Schema orderSchema = loadAvroSchema("avro/order.avsc");
            final org.apache.avro.Schema itemSchema =
                    orderSchema.getField("items").schema().getElementType();
            final org.apache.avro.Schema statusSchema = orderSchema.getField("status").schema();

            final GenericRecord item = new GenericData.Record(itemSchema);
            item.put("sku", "RT-001");
            item.put("product_name", "Round Trip Item");
            item.put("quantity", 5);
            item.put("unit_price", 99.99);

            final GenericRecord order = new GenericData.Record(orderSchema);
            order.put("order_id", "ORD-RT-AVRO");
            order.put("customer_id", "CUST-RT");
            order.put("items", java.util.Arrays.asList(item));
            order.put("total_amount", 499.95);
            order.put("status", new GenericData.EnumSymbol(statusSchema, "DELIVERED"));
            order.put("created_at", 1704153600000L);

            final Struct wrapper = wrapStruct("order", toConnectStruct(order));
            final String jsonString = transformAndGetJson("order", wrapper);

            final Map<String, Object> firstParse = parseJson(jsonString);
            final String reserialized = OBJECT_MAPPER.writeValueAsString(firstParse);
            final Map<String, Object> secondParse = parseJson(reserialized);

            assertEquals(firstParse, secondParse);
        }

        @Test
        @DisplayName("Should handle special characters in Avro strings")
        void shouldHandleSpecialCharactersInAvroStrings() throws Exception {
            final org.apache.avro.Schema simpleSchema = loadAvroSchema("avro/simple_record.avsc");

            final GenericRecord record = new GenericData.Record(simpleSchema);
            record.put("id", 1);
            record.put("name", "Special chars: \"quotes\", \n newlines, \t tabs, \\ backslashes");
            record.put("active", true);
            record.put("score", 100.0);

            final Struct wrapper = wrapStruct("data", toConnectStruct(record));
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(
                    "Special chars: \"quotes\", \n newlines, \t tabs, \\ backslashes",
                    parsed.get("name"));
        }

        @Test
        @DisplayName("Should handle Unicode in Avro strings")
        void shouldHandleUnicodeInAvroStrings() throws Exception {
            final org.apache.avro.Schema simpleSchema = loadAvroSchema("avro/simple_record.avsc");

            final GenericRecord record = new GenericData.Record(simpleSchema);
            record.put("id", 2);
            record.put("name", "Unicode: 你好世界 🌍 Привет мир مرحبا");
            record.put("active", true);
            record.put("score", 42.0);

            final Struct wrapper = wrapStruct("data", toConnectStruct(record));
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Unicode: 你好世界 🌍 Привет мир مرحبا", parsed.get("name"));
        }
    }

    private org.apache.avro.Schema loadAvroSchema(final String resourcePath) throws IOException {
        final Parser parser = new Parser();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return parser.parse(is);
        }
    }

    /** Converts an Avro GenericRecord to a Kafka Connect Struct using Confluent's AvroData. */
    private Struct toConnectStruct(final GenericRecord record) {
        final SchemaAndValue schemaAndValue = AVRO_DATA.toConnectData(record.getSchema(), record);
        return (Struct) schemaAndValue.value();
    }
}
