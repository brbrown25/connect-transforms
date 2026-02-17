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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StructToJsonEmbeddedConnectTest {

    private static final Logger LOG = LogManager.getLogger(StructToJsonEmbeddedConnectTest.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final AvroData AVRO_DATA = new AvroData(100);
    private static final String CONFLUENT_VERSION = "7.5.3";

    private static Network network;
    private static KafkaContainer kafka;
    private static GenericContainer<?> schemaRegistry;

    private static String bootstrapServers;
    private static String schemaRegistryUrl;

    @BeforeAll
    static void setUp() throws Exception {
        network = Network.newNetwork();

        kafka =
                new KafkaContainer(
                                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                        .withNetwork(network)
                        .withNetworkAliases("kafka");
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
        LOG.info("Kafka started at {}", bootstrapServers);

        schemaRegistry =
                new GenericContainer<>(
                                DockerImageName.parse(
                                        "confluentinc/cp-schema-registry:" + CONFLUENT_VERSION))
                        .withNetwork(network)
                        .withNetworkAliases("schema-registry")
                        .withExposedPorts(8081)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                        .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200));
        schemaRegistry.start();
        schemaRegistryUrl =
                String.format(
                        "http://%s:%d",
                        schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));
        LOG.info("Schema Registry started at {}", schemaRegistryUrl);
    }

    @AfterAll
    static void tearDown() {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    @DisplayName("Should transform Avro record through full serialization cycle")
    void shouldTransformAvroRecordThroughFullSerializationCycle() throws Exception {
        final String testId = UUID.randomUUID().toString().substring(0, 8);
        final String topic = "test-avro-transform-" + testId;

        final org.apache.avro.Schema avroSchema = loadAvroSchema("avro/event.avsc");
        final org.apache.avro.Schema payloadSchema = avroSchema.getField("payload").schema();

        LOG.info("Producing Avro messages to topic {}", topic);
        try (KafkaProducer<String, GenericRecord> producer = createAvroProducer()) {
            for (int i = 0; i < 3; i++) {
                Map<String, String> details = new HashMap<>();
                details.put("page", "/checkout");
                details.put("item_count", String.valueOf(i + 1));

                GenericRecord payload = new GenericData.Record(payloadSchema);
                payload.put("user_id", "user-" + i);
                payload.put("action", "purchase");
                payload.put("details", details);

                GenericRecord event = new GenericData.Record(avroSchema);
                event.put("event_id", "evt-" + i);
                event.put("timestamp", System.currentTimeMillis());
                event.put("payload", payload);

                producer.send(new ProducerRecord<>(topic, "key-" + i, event)).get();
            }
            producer.flush();
        }

        LOG.info("Consuming and transforming messages");
        try (KafkaConsumer<String, GenericRecord> consumer =
                createAvroConsumer("test-group-" + testId)) {
            consumer.subscribe(Collections.singletonList(topic));

            StructToJson<SourceRecord> transform = new StructToJson.Value<>();
            Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "payload");
            transform.configure(config);

            int processedCount = 0;
            long deadline = System.currentTimeMillis() + 30000;

            while (processedCount < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, GenericRecord> records =
                        consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    LOG.info("Consumed record: {}", record.value());

                    Struct connectStruct = toConnectStruct(record.value());

                    SourceRecord sourceRecord =
                            new SourceRecord(
                                    Collections.emptyMap(),
                                    Collections.emptyMap(),
                                    topic,
                                    0,
                                    null,
                                    record.key(),
                                    connectStruct.schema(),
                                    connectStruct,
                                    record.timestamp());

                    SourceRecord transformedRecord = transform.apply(sourceRecord);

                    Struct transformedValue = (Struct) transformedRecord.value();
                    assertNotNull(transformedValue);

                    String eventId = transformedValue.getString("event_id");
                    assertNotNull(eventId);
                    assertTrue(eventId.startsWith("evt-"));

                    Object payloadObj = transformedValue.get("payload");
                    assertTrue(
                            payloadObj instanceof String,
                            "Expected payload to be String but was " + payloadObj.getClass());

                    String payloadJson = (String) payloadObj;
                    LOG.info("Transformed payload JSON: {}", payloadJson);

                    Map<String, Object> parsedPayload =
                            OBJECT_MAPPER.readValue(
                                    payloadJson, new TypeReference<Map<String, Object>>() {});

                    assertNotNull(parsedPayload.get("user_id"));
                    assertEquals("purchase", parsedPayload.get("action"));
                    assertTrue(parsedPayload.get("details") instanceof Map);

                    @SuppressWarnings("unchecked")
                    Map<String, String> details =
                            (Map<String, String>) parsedPayload.get("details");
                    assertEquals("/checkout", details.get("page"));

                    processedCount++;
                }
            }

            assertEquals(3, processedCount, "Should have processed 3 records");
            transform.close();
        }
    }

    @Test
    @DisplayName("Should handle deeply nested Avro structures")
    void shouldHandleDeeplyNestedAvroStructures() throws Exception {
        final String testId = UUID.randomUUID().toString().substring(0, 8);
        final String topic = "test-nested-" + testId;

        final org.apache.avro.Schema avroSchema = loadAvroSchema("avro/nested_order.avsc");
        final org.apache.avro.Schema orderDetailsSchema =
                avroSchema.getField("order_details").schema();
        final org.apache.avro.Schema customerSchema =
                orderDetailsSchema.getField("customer").schema();
        final org.apache.avro.Schema addressSchema = customerSchema.getField("address").schema();
        final org.apache.avro.Schema itemSchema =
                orderDetailsSchema.getField("items").schema().getElementType();

        GenericRecord address = new GenericData.Record(addressSchema);
        address.put("street", "123 Main St");
        address.put("city", "San Francisco");
        address.put("country", "USA");

        GenericRecord customer = new GenericData.Record(customerSchema);
        customer.put("name", "John Doe");
        customer.put("email", "john@example.com");
        customer.put("address", address);

        GenericRecord item1 = new GenericData.Record(itemSchema);
        item1.put("sku", "SKU-001");
        item1.put("name", "Widget");
        item1.put("price", 29.99);
        item1.put("quantity", 2);

        GenericRecord item2 = new GenericData.Record(itemSchema);
        item2.put("sku", "SKU-002");
        item2.put("name", "Gadget");
        item2.put("price", 49.99);
        item2.put("quantity", 1);

        GenericRecord orderDetails = new GenericData.Record(orderDetailsSchema);
        orderDetails.put("customer", customer);
        orderDetails.put("items", java.util.Arrays.asList(item1, item2));
        orderDetails.put("total", 109.97);

        GenericRecord order = new GenericData.Record(avroSchema);
        order.put("order_id", "ORD-001");
        order.put("order_details", orderDetails);

        try (KafkaProducer<String, GenericRecord> producer = createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, "order-key", order)).get();
            producer.flush();
        }

        try (KafkaConsumer<String, GenericRecord> consumer =
                createAvroConsumer("test-nested-group-" + testId)) {
            consumer.subscribe(Collections.singletonList(topic));

            StructToJson<SourceRecord> transform = new StructToJson.Value<>();
            Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "order_details");
            transform.configure(config);

            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                ConsumerRecords<String, GenericRecord> records =
                                        consumer.poll(Duration.ofSeconds(1));
                                assertTrue(
                                        records.count() > 0, "Should receive at least one record");

                                for (ConsumerRecord<String, GenericRecord> record : records) {
                                    Struct connectStruct = toConnectStruct(record.value());

                                    SourceRecord sourceRecord =
                                            new SourceRecord(
                                                    Collections.emptyMap(),
                                                    Collections.emptyMap(),
                                                    topic,
                                                    0,
                                                    null,
                                                    record.key(),
                                                    connectStruct.schema(),
                                                    connectStruct,
                                                    record.timestamp());

                                    SourceRecord transformed = transform.apply(sourceRecord);
                                    Struct transformedValue = (Struct) transformed.value();

                                    assertEquals("ORD-001", transformedValue.getString("order_id"));

                                    String orderDetailsJson =
                                            (String) transformedValue.get("order_details");
                                    assertNotNull(orderDetailsJson);
                                    LOG.info(
                                            "Transformed order_details JSON: {}", orderDetailsJson);

                                    Map<String, Object> parsed =
                                            OBJECT_MAPPER.readValue(
                                                    orderDetailsJson,
                                                    new TypeReference<Map<String, Object>>() {});

                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> parsedCustomer =
                                            (Map<String, Object>) parsed.get("customer");
                                    assertEquals("John Doe", parsedCustomer.get("name"));

                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> parsedAddress =
                                            (Map<String, Object>) parsedCustomer.get("address");
                                    assertEquals("San Francisco", parsedAddress.get("city"));

                                    @SuppressWarnings("unchecked")
                                    List<Map<String, Object>> parsedItems =
                                            (List<Map<String, Object>>) parsed.get("items");
                                    assertEquals(2, parsedItems.size());
                                    assertEquals("SKU-001", parsedItems.get(0).get("sku"));
                                    assertEquals(
                                            29.99,
                                            ((Number) parsedItems.get(0).get("price"))
                                                    .doubleValue(),
                                            0.001);
                                }
                            });

            transform.close();
        }
    }

    @Test
    @DisplayName("Should preserve JSON round-trip integrity with special characters")
    void shouldPreserveJsonRoundTripIntegrityWithSpecialCharacters() throws Exception {
        final String testId = UUID.randomUUID().toString().substring(0, 8);
        final String topic = "test-special-chars-" + testId;

        final org.apache.avro.Schema avroSchema = loadAvroSchema("avro/message.avsc");
        final org.apache.avro.Schema contentSchema = avroSchema.getField("content").schema();

        String specialText = "Line1\\nLine2\\tTabbed\\r\\nWindows\\\\Backslash\\\"Quoted\\\"";
        String unicodeText = "Hello 世界 🌍 مرحبا Привет";

        GenericRecord content = new GenericData.Record(contentSchema);
        content.put("text", specialText);
        content.put("unicode_text", unicodeText);

        GenericRecord message = new GenericData.Record(avroSchema);
        message.put("id", "msg-special");
        message.put("content", content);

        try (KafkaProducer<String, GenericRecord> producer = createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, "key", message)).get();
            producer.flush();
        }

        try (KafkaConsumer<String, GenericRecord> consumer =
                createAvroConsumer("test-special-group-" + testId)) {
            consumer.subscribe(Collections.singletonList(topic));

            StructToJson<SourceRecord> transform = new StructToJson.Value<>();
            Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonConfig.FIELD_NAME_CONFIG, "content");
            transform.configure(config);

            await().atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                ConsumerRecords<String, GenericRecord> records =
                                        consumer.poll(Duration.ofSeconds(1));
                                assertTrue(records.count() > 0);

                                for (ConsumerRecord<String, GenericRecord> record : records) {
                                    Struct connectStruct = toConnectStruct(record.value());

                                    SourceRecord sourceRecord =
                                            new SourceRecord(
                                                    Collections.emptyMap(),
                                                    Collections.emptyMap(),
                                                    topic,
                                                    0,
                                                    null,
                                                    record.key(),
                                                    connectStruct.schema(),
                                                    connectStruct,
                                                    record.timestamp());

                                    SourceRecord transformed = transform.apply(sourceRecord);
                                    Struct transformedValue = (Struct) transformed.value();

                                    String contentJson = (String) transformedValue.get("content");
                                    LOG.info("Content JSON: {}", contentJson);

                                    Map<String, Object> parsed =
                                            OBJECT_MAPPER.readValue(
                                                    contentJson,
                                                    new TypeReference<Map<String, Object>>() {});

                                    assertEquals(specialText, parsed.get("text"));
                                    assertEquals(unicodeText, parsed.get("unicode_text"));

                                    String reserialized = OBJECT_MAPPER.writeValueAsString(parsed);
                                    Map<String, Object> reparsed =
                                            OBJECT_MAPPER.readValue(
                                                    reserialized,
                                                    new TypeReference<Map<String, Object>>() {});
                                    assertEquals(parsed, reparsed);
                                }
                            });

            transform.close();
        }
    }

    private KafkaProducer<String, GenericRecord> createAvroProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, GenericRecord> createAvroConsumer(final String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    private org.apache.avro.Schema loadAvroSchema(final String resourcePath) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }
            return new org.apache.avro.Schema.Parser().parse(is);
        }
    }

    private Struct toConnectStruct(final GenericRecord record) {
        final SchemaAndValue schemaAndValue = AVRO_DATA.toConnectData(record.getSchema(), record);
        return (Struct) schemaAndValue.value();
    }
}
