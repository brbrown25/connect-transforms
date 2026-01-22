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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StructToJsonSchemaIntegrationTest extends BaseTransformTest {

    private static final String TOPIC = "json-schema-test-topic";

    @Override
    protected String getTopicName() {
        return TOPIC;
    }

    @Nested
    @DisplayName("Simple JSON Schema Tests")
    class SimpleJsonSchemaTests {

        @Test
        @DisplayName("Should transform simple JSON Schema record")
        void shouldTransformSimpleJsonSchemaRecord() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Product")
                            .field("id", Schema.INT32_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .field("active", Schema.BOOLEAN_SCHEMA)
                            .field("price", Schema.FLOAT64_SCHEMA)
                            .build();

            final Struct data =
                    new Struct(dataSchema)
                            .put("id", 101)
                            .put("name", "JSON Schema Product")
                            .put("active", true)
                            .put("price", 29.99);

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(101, parsed.get("id"));
            assertEquals("JSON Schema Product", parsed.get("name"));
            assertEquals(true, parsed.get("active"));
            assertEquals(29.99, ((Number) parsed.get("price")).doubleValue(), 0.001);
        }
    }

    @Nested
    @DisplayName("JSON Schema with $ref (Nested Objects) Tests")
    class JsonSchemaRefTests {

        @Test
        @DisplayName("Should transform shipping_address field within Order to JSON")
        void shouldTransformShippingAddressFieldToJson() throws Exception {
            final Schema addressSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Address")
                            .field("street", Schema.STRING_SCHEMA)
                            .field("city", Schema.STRING_SCHEMA)
                            .field("postal_code", Schema.STRING_SCHEMA)
                            .field("country", Schema.STRING_SCHEMA)
                            .build();

            final Schema userSchema =
                    SchemaBuilder.struct()
                            .name("com.example.User")
                            .field("id", Schema.INT64_SCHEMA)
                            .field("username", Schema.STRING_SCHEMA)
                            .field("email", Schema.STRING_SCHEMA)
                            .build();

            final Schema orderSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Order")
                            .field("order_id", Schema.STRING_SCHEMA)
                            .field("user", userSchema)
                            .field("shipping_address", addressSchema)
                            .field("billing_address", addressSchema)
                            .build();

            final Struct shippingAddress =
                    new Struct(addressSchema)
                            .put("street", "123 Ship Street")
                            .put("city", "Shipville")
                            .put("postal_code", "12345")
                            .put("country", "US");

            final Struct billingAddress =
                    new Struct(addressSchema)
                            .put("street", "456 Bill Avenue")
                            .put("city", "Billtown")
                            .put("postal_code", "67890")
                            .put("country", "US");

            final Struct user =
                    new Struct(userSchema)
                            .put("id", 1001L)
                            .put("username", "jsonuser")
                            .put("email", "json@schema.io");

            final Struct order =
                    new Struct(orderSchema)
                            .put("order_id", "ORD-JSON-001")
                            .put("user", user)
                            .put("shipping_address", shippingAddress)
                            .put("billing_address", billingAddress);

            final String expectedShippingJson =
                    "{\"street\":\"123 Ship Street\","
                            + "\"city\":\"Shipville\","
                            + "\"postal_code\":\"12345\","
                            + "\"country\":\"US\"}";

            final TransformResult result = transformWithResult("shipping_address", order);

            assertEquals("ORD-JSON-001", result.struct().get("order_id"));

            final Struct resultUser = (Struct) result.struct().get("user");
            assertEquals(1001L, resultUser.get("id"));
            assertEquals("jsonuser", resultUser.get("username"));

            final Struct resultBilling = (Struct) result.struct().get("billing_address");
            assertEquals("456 Bill Avenue", resultBilling.get("street"));

            assertNotNull(result.json());
            assertJsonEquals(expectedShippingJson, result.json());
        }
    }

    @Nested
    @DisplayName("JSON Schema Array Tests")
    class JsonSchemaArrayTests {

        @Test
        @DisplayName("Should transform record with array of objects")
        void shouldTransformRecordWithArrayOfObjects() throws Exception {
            final Schema lineItemSchema =
                    SchemaBuilder.struct()
                            .name("com.example.LineItem")
                            .field("product_id", Schema.STRING_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .field("quantity", Schema.INT32_SCHEMA)
                            .field("unit_price", Schema.FLOAT64_SCHEMA)
                            .build();

            final Schema cartSchema =
                    SchemaBuilder.struct()
                            .name("com.example.ShoppingCart")
                            .field("cart_id", Schema.STRING_SCHEMA)
                            .field("items", SchemaBuilder.array(lineItemSchema).build())
                            .field("total", Schema.FLOAT64_SCHEMA)
                            .build();

            final Struct item1 =
                    new Struct(lineItemSchema)
                            .put("product_id", "PROD-001")
                            .put("name", "Widget")
                            .put("quantity", 2)
                            .put("unit_price", 15.99);

            final Struct item2 =
                    new Struct(lineItemSchema)
                            .put("product_id", "PROD-002")
                            .put("name", "Gadget")
                            .put("quantity", 1)
                            .put("unit_price", 49.99);

            final Struct cart =
                    new Struct(cartSchema)
                            .put("cart_id", "CART-JSON-001")
                            .put("items", Arrays.asList(item1, item2))
                            .put("total", 81.97);

            final Struct wrapper = wrapStruct("cart", cart);
            final String jsonString = transformAndGetJson("cart", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("CART-JSON-001", parsed.get("cart_id"));

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(2, items.size());
            assertEquals("PROD-001", items.get(0).get("product_id"));
            assertEquals("Widget", items.get(0).get("name"));
            assertEquals(2, items.get(0).get("quantity"));
        }

        @Test
        @DisplayName("Should transform record with array of primitives")
        void shouldTransformRecordWithArrayOfPrimitives() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.TaggedItem")
                            .field("id", Schema.STRING_SCHEMA)
                            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .field("scores", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                            .build();

            final Struct data =
                    new Struct(dataSchema)
                            .put("id", "item-123")
                            .put("tags", Arrays.asList("electronics", "sale", "featured"))
                            .put("scores", Arrays.asList(95, 88, 92, 100));

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            final List<String> tags = getStringList(parsed, "tags");
            assertEquals(3, tags.size());
            assertTrue(tags.contains("electronics"));

            final List<Integer> scores = getIntegerList(parsed, "scores");
            assertEquals(4, scores.size());
            assertEquals(95, scores.get(0));
        }
    }

    @Nested
    @DisplayName("JSON Schema additionalProperties Tests")
    class JsonSchemaAdditionalPropertiesTests {

        @Test
        @DisplayName("Should transform record with additionalProperties (map)")
        void shouldTransformRecordWithAdditionalPropertiesMap() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.MetadataContainer")
                            .field("name", Schema.STRING_SCHEMA)
                            .field(
                                    "metadata",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                            .build())
                            .field(
                                    "counts",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)
                                            .build())
                            .build();

            final java.util.Map<String, String> metadata = new java.util.HashMap<>();
            metadata.put("created_by", "system");
            metadata.put("version", "1.0.0");
            metadata.put("environment", "production");

            final java.util.Map<String, Integer> counts = new java.util.HashMap<>();
            counts.put("views", 1500);
            counts.put("likes", 42);
            counts.put("shares", 7);

            final Struct data =
                    new Struct(dataSchema)
                            .put("name", "Configurable Item")
                            .put("metadata", metadata)
                            .put("counts", counts);

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Configurable Item", parsed.get("name"));

            final Map<String, String> parsedMetadata = getStringMap(parsed, "metadata");
            assertEquals("system", parsedMetadata.get("created_by"));
            assertEquals("1.0.0", parsedMetadata.get("version"));

            final Map<String, Integer> parsedCounts = getIntegerMap(parsed, "counts");
            assertEquals(1500, parsedCounts.get("views"));
            assertEquals(42, parsedCounts.get("likes"));
        }
    }

    @Nested
    @DisplayName("JSON Schema Nullable Types Tests")
    class JsonSchemaNullableTypesTests {

        @Test
        @DisplayName("Should handle nullable types with values")
        void shouldHandleNullableTypesWithValues() throws Exception {
            final Schema detailsSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Details")
                            .field("info", Schema.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build();

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.NullableTypes")
                            .field("required_field", Schema.STRING_SCHEMA)
                            .field("nullable_string", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("nullable_number", Schema.OPTIONAL_FLOAT64_SCHEMA)
                            .field("nullable_boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                            .field("nullable_object", detailsSchema)
                            .build();

            final Struct details = new Struct(detailsSchema).put("info", "Some details here");

            final Struct data =
                    new Struct(dataSchema)
                            .put("required_field", "I am required")
                            .put("nullable_string", "I have a value")
                            .put("nullable_number", 3.14159)
                            .put("nullable_boolean", true)
                            .put("nullable_object", details);

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("I am required", parsed.get("required_field"));
            assertEquals("I have a value", parsed.get("nullable_string"));
            assertEquals(3.14159, ((Number) parsed.get("nullable_number")).doubleValue(), 0.00001);
            assertEquals(true, parsed.get("nullable_boolean"));

            final Map<String, Object> parsedDetails = getMap(parsed, "nullable_object");
            assertEquals("Some details here", parsedDetails.get("info"));
        }

        @Test
        @DisplayName("Should handle nullable types with null values")
        void shouldHandleNullableTypesWithNullValues() throws Exception {
            final Schema detailsSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Details")
                            .field("info", Schema.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build();

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.NullableTypes")
                            .field("required_field", Schema.STRING_SCHEMA)
                            .field("nullable_string", Schema.OPTIONAL_STRING_SCHEMA)
                            .field("nullable_number", Schema.OPTIONAL_FLOAT64_SCHEMA)
                            .field("nullable_boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                            .field("nullable_object", detailsSchema)
                            .build();

            final Struct data =
                    new Struct(dataSchema)
                            .put("required_field", "Only required is set")
                            .put("nullable_string", null)
                            .put("nullable_number", null)
                            .put("nullable_boolean", null)
                            .put("nullable_object", null);

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Only required is set", parsed.get("required_field"));
            assertNull(parsed.get("nullable_string"));
            assertNull(parsed.get("nullable_number"));
            assertNull(parsed.get("nullable_boolean"));
            assertNull(parsed.get("nullable_object"));
        }
    }

    @Nested
    @DisplayName("JSON Schema Format Annotations Tests")
    class JsonSchemaFormatAnnotationsTests {

        @Test
        @DisplayName("Should handle format annotations (email, uri, date-time)")
        void shouldHandleFormatAnnotations() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .name("com.example.FormattedData")
                            .field("email", Schema.STRING_SCHEMA)
                            .field("website", Schema.STRING_SCHEMA)
                            .field("created_at", Schema.STRING_SCHEMA)
                            .field("date_of_birth", Schema.STRING_SCHEMA)
                            .field("uuid", Schema.STRING_SCHEMA)
                            .build();

            final Struct data =
                    new Struct(dataSchema)
                            .put("email", "user@example.com")
                            .put("website", "https://www.example.com/path?query=value")
                            .put("created_at", "2024-01-15T10:30:00Z")
                            .put("date_of_birth", "1990-05-20")
                            .put("uuid", "550e8400-e29b-41d4-a716-446655440000");

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("user@example.com", parsed.get("email"));
            assertEquals("https://www.example.com/path?query=value", parsed.get("website"));
            assertEquals("2024-01-15T10:30:00Z", parsed.get("created_at"));
            assertEquals("1990-05-20", parsed.get("date_of_birth"));
            assertEquals("550e8400-e29b-41d4-a716-446655440000", parsed.get("uuid"));
        }
    }

    @Nested
    @DisplayName("JSON Schema Complex Nested Structure Tests")
    class JsonSchemaComplexNestedStructureTests {

        @Test
        @DisplayName("Should transform deeply nested JSON Schema structure")
        void shouldTransformDeeplyNestedJsonSchemaStructure() throws Exception {
            final Schema geoLocationSchema =
                    SchemaBuilder.struct()
                            .name("com.example.GeoLocation")
                            .field("latitude", Schema.FLOAT64_SCHEMA)
                            .field("longitude", Schema.FLOAT64_SCHEMA)
                            .build();

            final Schema venueSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Venue")
                            .field("name", Schema.STRING_SCHEMA)
                            .field("address", Schema.STRING_SCHEMA)
                            .field("location", geoLocationSchema)
                            .build();

            final Schema speakerSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Speaker")
                            .field("name", Schema.STRING_SCHEMA)
                            .field("bio", Schema.STRING_SCHEMA)
                            .field("topics", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                            .build();

            final Schema sessionSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Session")
                            .field("title", Schema.STRING_SCHEMA)
                            .field("description", Schema.STRING_SCHEMA)
                            .field("speakers", SchemaBuilder.array(speakerSchema).build())
                            .field("duration_minutes", Schema.INT32_SCHEMA)
                            .build();

            final Schema conferenceSchema =
                    SchemaBuilder.struct()
                            .name("com.example.Conference")
                            .field("conference_id", Schema.STRING_SCHEMA)
                            .field("name", Schema.STRING_SCHEMA)
                            .field("venue", venueSchema)
                            .field("sessions", SchemaBuilder.array(sessionSchema).build())
                            .field(
                                    "tags",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                            .build())
                            .build();

            final Struct geoLocation =
                    new Struct(geoLocationSchema)
                            .put("latitude", 37.7749)
                            .put("longitude", -122.4194);

            final Struct venue =
                    new Struct(venueSchema)
                            .put("name", "Tech Conference Center")
                            .put("address", "100 Tech Blvd, San Francisco, CA")
                            .put("location", geoLocation);

            final Struct speaker1 =
                    new Struct(speakerSchema)
                            .put("name", "Alice Smith")
                            .put("bio", "Expert in distributed systems")
                            .put(
                                    "topics",
                                    Arrays.asList("Kafka", "Microservices", "Event Streaming"));

            final Struct speaker2 =
                    new Struct(speakerSchema)
                            .put("name", "Bob Johnson")
                            .put("bio", "Data engineering specialist")
                            .put(
                                    "topics",
                                    Arrays.asList("Data Pipelines", "ETL", "Schema Evolution"));

            final Struct session =
                    new Struct(sessionSchema)
                            .put("title", "Building Scalable Data Pipelines")
                            .put(
                                    "description",
                                    "Learn how to build robust data pipelines with Kafka Connect")
                            .put("speakers", Arrays.asList(speaker1, speaker2))
                            .put("duration_minutes", 60);

            final Map<String, String> tags = new HashMap<>();
            tags.put("track", "Data Engineering");
            tags.put("level", "Intermediate");
            tags.put("format", "Workshop");

            final Struct conference =
                    new Struct(conferenceSchema)
                            .put("conference_id", "CONF-2024-001")
                            .put("name", "DataCon 2024")
                            .put("venue", venue)
                            .put("sessions", Arrays.asList(session))
                            .put("tags", tags);

            final Struct wrapper = wrapStruct("conference", conference);
            final String jsonString = transformAndGetJson("conference", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("CONF-2024-001", parsed.get("conference_id"));
            assertEquals("DataCon 2024", parsed.get("name"));

            final Map<String, Object> parsedVenue = getMap(parsed, "venue");
            assertEquals("Tech Conference Center", parsedVenue.get("name"));

            final Map<String, Object> parsedLocation = getMap(parsedVenue, "location");
            assertEquals(37.7749, ((Number) parsedLocation.get("latitude")).doubleValue(), 0.0001);

            final List<Map<String, Object>> sessions = getListOfMaps(parsed, "sessions");
            assertEquals(1, sessions.size());
            assertEquals("Building Scalable Data Pipelines", sessions.get(0).get("title"));

            final List<Map<String, Object>> speakers = getListOfMaps(sessions.get(0), "speakers");
            assertEquals(2, speakers.size());
            assertEquals("Alice Smith", speakers.get(0).get("name"));

            final List<String> topics = getStringList(speakers.get(0), "topics");
            assertTrue(topics.contains("Kafka"));

            final Map<String, String> parsedTags = getStringMap(parsed, "tags");
            assertEquals("Data Engineering", parsedTags.get("track"));
        }
    }

    @Nested
    @DisplayName("JSON Schema Round-Trip Tests")
    class JsonSchemaRoundTripTests {

        @Test
        @DisplayName("Should produce JSON that can be re-serialized identically")
        void shouldProduceJsonThatCanBeReserializedIdentically() throws Exception {
            final Schema itemSchema =
                    SchemaBuilder.struct()
                            .field("sku", Schema.STRING_SCHEMA)
                            .field("price", Schema.FLOAT64_SCHEMA)
                            .build();

            final Schema dataSchema =
                    SchemaBuilder.struct()
                            .field("id", Schema.STRING_SCHEMA)
                            .field("items", SchemaBuilder.array(itemSchema).build())
                            .field(
                                    "metadata",
                                    SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                            .build())
                            .build();

            final Struct item =
                    new Struct(itemSchema).put("sku", "RT-JSON-001").put("price", 99.99);

            final Map<String, String> metadata = new HashMap<>();
            metadata.put("source", "json-schema");
            metadata.put("version", "2.0");

            final Struct data =
                    new Struct(dataSchema)
                            .put("id", "round-trip-test")
                            .put("items", Arrays.asList(item))
                            .put("metadata", metadata);

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);

            final Map<String, Object> firstParse = parseJson(jsonString);
            final String reserialized = OBJECT_MAPPER.writeValueAsString(firstParse);
            final Map<String, Object> secondParse = parseJson(reserialized);

            assertEquals(firstParse, secondParse);
        }

        @Test
        @DisplayName("Should handle special characters")
        void shouldHandleSpecialCharacters() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();

            final Struct data =
                    new Struct(dataSchema)
                            .put(
                                    "text",
                                    "Special: \"quotes\", \n newlines, \t tabs, \\ backslash, / slash");

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(
                    "Special: \"quotes\", \n newlines, \t tabs, \\ backslash, / slash",
                    parsed.get("text"));
        }

        @Test
        @DisplayName("Should handle Unicode characters")
        void shouldHandleUnicodeCharacters() throws Exception {
            final Schema dataSchema =
                    SchemaBuilder.struct().field("text", Schema.STRING_SCHEMA).build();

            final Struct data =
                    new Struct(dataSchema).put("text", "Unicode: 日本語 한국어 العربية 🎉🚀💻");

            final Struct wrapper = wrapStruct("data", data);
            final String jsonString = transformAndGetJson("data", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Unicode: 日本語 한국어 العربية 🎉🚀💻", parsed.get("text"));
        }
    }
}
