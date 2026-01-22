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

import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.Address;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.AllTypesMessage;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.EventMetadata;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.Order;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.OrderItem;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.OrderStatus;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.Person;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.SimpleMessage;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.UserCreated;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.UserEvent;
import com.bbrownsound.kafka.connect.transforms.proto.TestMessages.UserPreferences;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StructToJsonProtobufIntegrationTest extends BaseTransformTest {

    private static final String TOPIC = "test-topic";
    private static final ProtobufData PROTOBUF_DATA =
            new ProtobufData(new ProtobufDataConfig(Map.of()));

    @Override
    protected String getTopicName() {
        return TOPIC;
    }

    @Nested
    @DisplayName("Simple Protobuf Message Tests")
    class SimpleProtobufMessageTests {

        @Test
        @DisplayName("Should transform simple protobuf message to JSON")
        void shouldTransformSimpleProtobufMessageToJson() throws Exception {
            final SimpleMessage protoMessage =
                    SimpleMessage.newBuilder()
                            .setId(42)
                            .setName("Test Product")
                            .setActive(true)
                            .setScore(95.5)
                            .build();

            final Struct wrapper =
                    wrapStruct(
                            "data",
                            toConnectStruct(protoMessage),
                            "topic",
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            TOPIC);

            final TransformResult result = transformWithResult("data", wrapper);
            assertEquals(TOPIC, result.struct().get("topic"));
            assertNotNull(result.json());

            final Map<String, Object> parsed = result.parsedJson();
            assertEquals(42, parsed.get("id"));
            assertEquals("Test Product", parsed.get("name"));
            assertEquals(true, parsed.get("active"));
            assertEquals(95.5, ((Number) parsed.get("score")).doubleValue(), 0.001);

            final String protoJson = JsonFormat.printer().print(protoMessage);
            final Map<String, Object> protoJsonParsed = parseJson(protoJson);

            assertEquals(protoJsonParsed.get("id"), parsed.get("id"));
            assertEquals(protoJsonParsed.get("name"), parsed.get("name"));
            assertEquals(protoJsonParsed.get("active"), parsed.get("active"));
        }
    }

    @Nested
    @DisplayName("Nested Protobuf Message Tests")
    class NestedProtobufMessageTests {

        @Test
        @DisplayName("Should transform Address field within Person to JSON")
        void shouldTransformAddressFieldWithinPersonToJson() throws Exception {
            final Address address =
                    Address.newBuilder()
                            .setStreet("123 Main Street")
                            .setCity("Springfield")
                            .setState("IL")
                            .setZipCode("62701")
                            .setCountry("USA")
                            .build();

            final Person person =
                    Person.newBuilder()
                            .setFirstName("John")
                            .setLastName("Doe")
                            .setAge(35)
                            .setEmail("john.doe@example.com")
                            .setAddress(address)
                            .addPhoneNumbers("555-1234")
                            .addPhoneNumbers("555-5678")
                            .build();

            final String expectedAddressJson =
                    "{\"street\":\"123 Main Street\","
                            + "\"city\":\"Springfield\","
                            + "\"state\":\"IL\","
                            + "\"zip_code\":\"62701\","
                            + "\"country\":\"USA\"}";

            final Struct personStruct = toConnectStruct(person);
            final TransformResult result = transformWithResult("address", personStruct);

            assertEquals("John", result.struct().get("first_name"));
            assertEquals("Doe", result.struct().get("last_name"));
            assertEquals(35, result.struct().get("age"));
            assertEquals("john.doe@example.com", result.struct().get("email"));

            final List<String> phoneNumbers = getStringList(result.struct(), "phone_numbers");
            assertEquals(2, phoneNumbers.size());
            assertEquals("555-1234", phoneNumbers.get(0));
            assertEquals("555-5678", phoneNumbers.get(1));

            assertNotNull(result.json());
            assertJsonEquals(expectedAddressJson, result.json());
        }

        @Test
        @DisplayName("Should transform nested struct to JSON with different output field name")
        void shouldTransformNestedStructWithDifferentOutputFieldName() throws Exception {
            final Address address =
                    Address.newBuilder()
                            .setStreet("456 Oak Avenue")
                            .setCity("Portland")
                            .setState("OR")
                            .setZipCode("97201")
                            .setCountry("USA")
                            .build();

            final Person person =
                    Person.newBuilder()
                            .setFirstName("Jane")
                            .setLastName("Smith")
                            .setAge(28)
                            .setEmail("jane.smith@example.com")
                            .setAddress(address)
                            .build();

            final String expectedAddressJson =
                    "{\"street\":\"456 Oak Avenue\","
                            + "\"city\":\"Portland\","
                            + "\"state\":\"OR\","
                            + "\"zip_code\":\"97201\","
                            + "\"country\":\"USA\"}";

            final Struct personStruct = toConnectStruct(person);
            final String actualAddressJson =
                    transformAndGetJson("address", "address_json", personStruct);

            final Struct resultAfterTransform = applyTransform(personStruct.schema(), personStruct);
            assertNotNull(resultAfterTransform.get("address"));
            assertNotNull(actualAddressJson);
            assertJsonEquals(expectedAddressJson, actualAddressJson);
        }
    }

    @Nested
    @DisplayName("Repeated Fields (Arrays) Tests")
    class RepeatedFieldsTests {

        @Test
        @DisplayName("Should transform Order message with repeated OrderItems")
        void shouldTransformOrderMessageWithRepeatedOrderItems() throws Exception {
            final OrderItem item1 =
                    OrderItem.newBuilder()
                            .setSku("SKU-001")
                            .setProductName("Widget A")
                            .setQuantity(2)
                            .setUnitPrice(19.99)
                            .build();

            final OrderItem item2 =
                    OrderItem.newBuilder()
                            .setSku("SKU-002")
                            .setProductName("Gadget B")
                            .setQuantity(1)
                            .setUnitPrice(49.99)
                            .build();

            final OrderItem item3 =
                    OrderItem.newBuilder()
                            .setSku("SKU-003")
                            .setProductName("Accessory C")
                            .setQuantity(5)
                            .setUnitPrice(9.99)
                            .build();

            final Order order =
                    Order.newBuilder()
                            .setOrderId("ORD-2024-001")
                            .setCustomerId("CUST-12345")
                            .addItems(item1)
                            .addItems(item2)
                            .addItems(item3)
                            .setTotalAmount(139.92)
                            .setStatus(OrderStatus.ORDER_STATUS_CONFIRMED)
                            .setCreatedAt(1704067200000L)
                            .build();

            final Struct wrapper = wrapStruct("order", toConnectStruct(order));
            final String jsonString = transformAndGetJson("order", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("ORD-2024-001", parsed.get("order_id"));
            assertEquals("CUST-12345", parsed.get("customer_id"));
            assertEquals(139.92, ((Number) parsed.get("total_amount")).doubleValue(), 0.001);
            assertEquals("ORDER_STATUS_CONFIRMED", parsed.get("status"));
            assertEquals(1704067200000L, ((Number) parsed.get("created_at")).longValue());

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertEquals(3, items.size());

            assertEquals("SKU-001", items.get(0).get("sku"));
            assertEquals("Widget A", items.get(0).get("product_name"));
            assertEquals(2, items.get(0).get("quantity"));
            assertEquals(19.99, ((Number) items.get(0).get("unit_price")).doubleValue(), 0.001);

            assertEquals("SKU-002", items.get(1).get("sku"));
            assertEquals("SKU-003", items.get(2).get("sku"));
        }
    }

    @Nested
    @DisplayName("Map Fields Tests")
    class MapFieldsTests {

        @Test
        @DisplayName("Should transform UserPreferences message with map fields")
        void shouldTransformUserPreferencesMessageWithMapFields() throws Exception {
            final UserPreferences prefs =
                    UserPreferences.newBuilder()
                            .setUserId("user-123")
                            .putSettings("theme", "dark")
                            .putSettings("language", "en-US")
                            .putSettings("timezone", "America/New_York")
                            .putFeatureFlags("beta_features", 1)
                            .putFeatureFlags("new_dashboard", 0)
                            .putFeatureFlags("ai_suggestions", 1)
                            .build();

            final Struct wrapper = wrapStruct("preferences", toConnectStruct(prefs));
            final String jsonString = transformAndGetJson("preferences", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("user-123", parsed.get("user_id"));

            final Map<String, String> settings = getStringMap(parsed, "settings");
            assertEquals("dark", settings.get("theme"));
            assertEquals("en-US", settings.get("language"));
            assertEquals("America/New_York", settings.get("timezone"));

            final Map<String, Integer> featureFlags = getIntegerMap(parsed, "feature_flags");
            assertEquals(1, featureFlags.get("beta_features"));
            assertEquals(0, featureFlags.get("new_dashboard"));
            assertEquals(1, featureFlags.get("ai_suggestions"));
        }
    }

    @Nested
    @DisplayName("Complex Event Message Tests")
    class ComplexEventMessageTests {

        @Test
        @DisplayName("Should transform UserEvent with nested metadata and profile")
        void shouldTransformUserEventWithNestedMetadataAndProfile() throws Exception {
            final Address address =
                    Address.newBuilder()
                            .setStreet("456 Oak Avenue")
                            .setCity("Portland")
                            .setState("OR")
                            .setZipCode("97201")
                            .setCountry("USA")
                            .build();

            final Person profile =
                    Person.newBuilder()
                            .setFirstName("Jane")
                            .setLastName("Smith")
                            .setAge(28)
                            .setEmail("jane.smith@example.com")
                            .setAddress(address)
                            .addPhoneNumbers("503-555-0100")
                            .build();

            final UserCreated userCreated =
                    UserCreated.newBuilder()
                            .setUsername("janesmith")
                            .setEmail("jane.smith@example.com")
                            .setProfile(profile)
                            .build();

            final EventMetadata metadata =
                    EventMetadata.newBuilder()
                            .setEventId("evt-abc-123")
                            .setEventType("USER_CREATED")
                            .setTimestamp(1704153600000L)
                            .setSource("user-service")
                            .putHeaders("correlation-id", "corr-xyz-789")
                            .putHeaders("trace-id", "trace-123-456")
                            .build();

            final UserEvent event =
                    UserEvent.newBuilder()
                            .setMetadata(metadata)
                            .setUserId("user-456")
                            .setUserCreated(userCreated)
                            .build();

            final Struct wrapper = wrapStruct("event", toConnectStruct(event));
            final String jsonString = transformAndGetJson("event", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("user-456", parsed.get("user_id"));

            final Map<String, Object> parsedMetadata = getMap(parsed, "metadata");
            assertEquals("evt-abc-123", parsedMetadata.get("event_id"));
            assertEquals("USER_CREATED", parsedMetadata.get("event_type"));
            assertEquals("user-service", parsedMetadata.get("source"));

            final Map<String, String> headers = getStringMap(parsedMetadata, "headers");
            assertEquals("corr-xyz-789", headers.get("correlation-id"));

            final String jsonForSearch = jsonString.toLowerCase();
            assertTrue(
                    jsonForSearch.contains("janesmith"),
                    "JSON should contain username 'janesmith': " + jsonString);
            assertTrue(
                    jsonForSearch.contains("jane.smith@example.com"), "JSON should contain email");
        }
    }

    @Nested
    @DisplayName("All Scalar Types Tests")
    class AllScalarTypesTests {

        @Test
        @DisplayName("Should correctly handle all protobuf scalar types")
        void shouldCorrectlyHandleAllProtobufScalarTypes() throws Exception {
            final AllTypesMessage message =
                    AllTypesMessage.newBuilder()
                            .setDoubleVal(3.14159265359)
                            .setFloatVal(2.71828f)
                            .setInt32Val(Integer.MAX_VALUE)
                            .setInt64Val(Long.MAX_VALUE)
                            .setUint32Val(123456789)
                            .setUint64Val(987654321L)
                            .setSint32Val(-12345)
                            .setSint64Val(-9876543210L)
                            .setFixed32Val(11111)
                            .setFixed64Val(22222L)
                            .setSfixed32Val(-33333)
                            .setSfixed64Val(-44444L)
                            .setBoolVal(true)
                            .setStringVal("Hello, Protobuf!")
                            .setBytesVal(ByteString.copyFromUtf8("binary data"))
                            .build();

            final Struct wrapper = wrapStruct("message", toConnectStruct(message));
            final String jsonString = transformAndGetJson("message", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(
                    3.14159265359,
                    ((Number) parsed.get("double_val")).doubleValue(),
                    0.00000000001);
            assertEquals(2.71828f, ((Number) parsed.get("float_val")).floatValue(), 0.00001);
            assertEquals(Integer.MAX_VALUE, ((Number) parsed.get("int32_val")).intValue());
            assertEquals(Long.MAX_VALUE, ((Number) parsed.get("int64_val")).longValue());
            assertEquals(123456789, ((Number) parsed.get("uint32_val")).intValue());
            assertEquals(987654321L, ((Number) parsed.get("uint64_val")).longValue());
            assertEquals(-12345, ((Number) parsed.get("sint32_val")).intValue());
            assertEquals(-9876543210L, ((Number) parsed.get("sint64_val")).longValue());
            assertEquals(11111, ((Number) parsed.get("fixed32_val")).intValue());
            assertEquals(22222L, ((Number) parsed.get("fixed64_val")).longValue());
            assertEquals(-33333, ((Number) parsed.get("sfixed32_val")).intValue());
            assertEquals(-44444L, ((Number) parsed.get("sfixed64_val")).longValue());

            assertEquals(true, parsed.get("bool_val"));
            assertEquals("Hello, Protobuf!", parsed.get("string_val"));
            assertNotNull(parsed.get("bytes_val"));
        }
    }

    @Nested
    @DisplayName("Round-Trip Validation Tests")
    class RoundTripValidationTests {

        @Test
        @DisplayName("Should produce JSON that can be re-serialized identically")
        void shouldProduceJsonThatCanBeReserializedIdentically() throws Exception {
            final Order order =
                    Order.newBuilder()
                            .setOrderId("ORD-RT-001")
                            .setCustomerId("CUST-RT-001")
                            .addItems(
                                    OrderItem.newBuilder()
                                            .setSku("SKU-RT-001")
                                            .setProductName("Round Trip Widget")
                                            .setQuantity(3)
                                            .setUnitPrice(29.99)
                                            .build())
                            .setTotalAmount(89.97)
                            .setStatus(OrderStatus.ORDER_STATUS_SHIPPED)
                            .setCreatedAt(1704240000000L)
                            .build();

            final Struct wrapper = wrapStruct("order", toConnectStruct(order));
            final String jsonString = transformAndGetJson("order", wrapper);

            final Map<String, Object> firstParse = parseJson(jsonString);
            final String reserializedJson = OBJECT_MAPPER.writeValueAsString(firstParse);
            final Map<String, Object> secondParse = parseJson(reserializedJson);

            assertEquals(firstParse, secondParse);
            assertEquals("ORD-RT-001", secondParse.get("order_id"));
            assertEquals("ORDER_STATUS_SHIPPED", secondParse.get("status"));

            final List<Map<String, Object>> items = getListOfMaps(secondParse, "items");
            assertEquals("Round Trip Widget", items.get(0).get("product_name"));
        }

        @Test
        @DisplayName("Should handle special characters in protobuf strings")
        void shouldHandleSpecialCharactersInProtobufStrings() throws Exception {
            final SimpleMessage message =
                    SimpleMessage.newBuilder()
                            .setId(1)
                            .setName(
                                    "Test with \"quotes\" and \n newlines \t tabs and \\ backslashes")
                            .setActive(true)
                            .setScore(100.0)
                            .build();

            final Struct wrapper = wrapStruct("message", toConnectStruct(message));
            final String jsonString = transformAndGetJson("message", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            final String expectedName =
                    "Test with \"quotes\" and \n newlines \t tabs and \\ backslashes";
            assertEquals(expectedName, parsed.get("name"));
        }

        @Test
        @DisplayName("Should handle Unicode in protobuf strings")
        void shouldHandleUnicodeInProtobufStrings() throws Exception {
            final SimpleMessage message =
                    SimpleMessage.newBuilder()
                            .setId(2)
                            .setName("Héllo Wörld! 你好世界 🌍 Привет мир")
                            .setActive(true)
                            .setScore(42.0)
                            .build();

            final Struct wrapper = wrapStruct("message", toConnectStruct(message));
            final String jsonString = transformAndGetJson("message", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("Héllo Wörld! 你好世界 🌍 Привет мир", parsed.get("name"));
        }
    }

    @Nested
    @DisplayName("Output Field Name Configuration Tests")
    class OutputFieldNameConfigurationTests {

        @Test
        @DisplayName("Should create separate JSON field when output field name differs")
        void shouldCreateSeparateJsonFieldWhenOutputFieldNameDiffers() throws Exception {
            final SimpleMessage message =
                    SimpleMessage.newBuilder()
                            .setId(100)
                            .setName("Separate Output")
                            .setActive(true)
                            .setScore(50.0)
                            .build();

            final Struct wrapper =
                    wrapStruct(
                            "original_data",
                            toConnectStruct(message),
                            "other_field",
                            org.apache.kafka.connect.data.Schema.STRING_SCHEMA,
                            "preserved");

            final String jsonString = transformAndGetJson("original_data", "json_data", wrapper);
            final Struct resultValue = applyTransform(wrapper.schema(), wrapper);

            assertNotNull(resultValue.get("original_data"));
            assertNotNull(jsonString);
            assertEquals("preserved", resultValue.get("other_field"));

            final Map<String, Object> parsed = parseJson(jsonString);
            assertEquals(100, parsed.get("id"));
            assertEquals("Separate Output", parsed.get("name"));
        }
    }

    @Nested
    @DisplayName("Empty and Default Value Tests")
    class EmptyAndDefaultValueTests {

        @Test
        @DisplayName("Should handle protobuf message with default values")
        void shouldHandleProtobufMessageWithDefaultValues() throws Exception {
            final SimpleMessage message = SimpleMessage.newBuilder().build();

            final Struct wrapper = wrapStruct("message", toConnectStruct(message));
            final String jsonString = transformAndGetJson("message", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals(0, parsed.get("id"));
            assertEquals("", parsed.get("name"));
            assertEquals(false, parsed.get("active"));
            assertEquals(0.0, ((Number) parsed.get("score")).doubleValue(), 0.001);
        }

        @Test
        @DisplayName("Should handle protobuf message with empty repeated fields")
        void shouldHandleProtobufMessageWithEmptyRepeatedFields() throws Exception {
            final Order order =
                    Order.newBuilder()
                            .setOrderId("EMPTY-001")
                            .setCustomerId("CUST-EMPTY")
                            .setTotalAmount(0.0)
                            .setStatus(OrderStatus.ORDER_STATUS_PENDING)
                            .setCreatedAt(1704326400000L)
                            .build();

            final Struct wrapper = wrapStruct("order", toConnectStruct(order));
            final String jsonString = transformAndGetJson("order", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("EMPTY-001", parsed.get("order_id"));

            final List<Map<String, Object>> items = getListOfMaps(parsed, "items");
            assertTrue(items.isEmpty());
        }

        @Test
        @DisplayName("Should handle protobuf message with empty map fields")
        void shouldHandleProtobufMessageWithEmptyMapFields() throws Exception {
            final UserPreferences prefs =
                    UserPreferences.newBuilder().setUserId("user-empty-maps").build();

            final Struct wrapper = wrapStruct("preferences", toConnectStruct(prefs));
            final String jsonString = transformAndGetJson("preferences", wrapper);
            final Map<String, Object> parsed = parseJson(jsonString);

            assertEquals("user-empty-maps", parsed.get("user_id"));

            final Map<String, String> settings = getStringMap(parsed, "settings");
            assertTrue(settings.isEmpty());

            final Map<String, Integer> featureFlags = getIntegerMap(parsed, "feature_flags");
            assertTrue(featureFlags.isEmpty());
        }
    }

    private Struct toConnectStruct(final Message message) {
        final ProtobufSchema protobufSchema = new ProtobufSchema(message.getDescriptorForType());
        final SchemaAndValue schemaAndValue = PROTOBUF_DATA.toConnectData(protobufSchema, message);
        return (Struct) schemaAndValue.value();
    }
}
