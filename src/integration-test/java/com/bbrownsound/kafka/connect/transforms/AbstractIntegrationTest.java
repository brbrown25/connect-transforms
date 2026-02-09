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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Abstract base class for integration tests that sets up Kafka, Schema Registry, and Kafka Connect
 * containers using Testcontainers.
 */
public abstract class AbstractIntegrationTest {

    protected static final Logger LOG = LogManager.getLogger(AbstractIntegrationTest.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    protected static final String CONFLUENT_VERSION = "7.5.3";
    protected static final int SCHEMA_REGISTRY_PORT = 8081;
    protected static final int CONNECT_PORT = 8083;

    protected static Network network;
    protected static KafkaContainer kafka;
    protected static GenericContainer<?> schemaRegistry;
    protected static GenericContainer<?> connect;

    protected static String bootstrapServers;
    protected static String schemaRegistryUrl;
    protected static String connectUrl;

    @BeforeAll
    static void startContainers() throws Exception {
        network = Network.newNetwork();

        kafka =
                new KafkaContainer(
                                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_VERSION))
                        .withNetwork(network)
                        .withNetworkAliases("kafka")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        kafka.start();
        bootstrapServers = kafka.getBootstrapServers();
        LOG.info("Kafka started at {}", bootstrapServers);

        schemaRegistry =
                new GenericContainer<>(
                                DockerImageName.parse(
                                        "confluentinc/cp-schema-registry:" + CONFLUENT_VERSION))
                        .withNetwork(network)
                        .withNetworkAliases("schema-registry")
                        .withExposedPorts(SCHEMA_REGISTRY_PORT)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                        .withEnv(
                                "SCHEMA_REGISTRY_LISTENERS",
                                "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
                        .waitingFor(
                                Wait.forHttp("/subjects")
                                        .forPort(SCHEMA_REGISTRY_PORT)
                                        .forStatusCode(200));
        schemaRegistry.start();
        schemaRegistryUrl =
                String.format(
                        "http://%s:%d",
                        schemaRegistry.getHost(),
                        schemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT));
        LOG.info("Schema Registry started at {}", schemaRegistryUrl);

        connect =
                new GenericContainer<>(
                                DockerImageName.parse(
                                        "confluentinc/cp-kafka-connect:" + CONFLUENT_VERSION))
                        .withNetwork(network)
                        .withNetworkAliases("connect")
                        .withExposedPorts(CONNECT_PORT)
                        .withEnv("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092")
                        .withEnv("CONNECT_REST_PORT", String.valueOf(CONNECT_PORT))
                        .withEnv("CONNECT_GROUP_ID", "connect-cluster")
                        .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs")
                        .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets")
                        .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect-status")
                        .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv(
                                "CONNECT_KEY_CONVERTER",
                                "org.apache.kafka.connect.storage.StringConverter")
                        .withEnv(
                                "CONNECT_VALUE_CONVERTER",
                                "io.confluent.connect.avro.AvroConverter")
                        .withEnv(
                                "CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                                "http://schema-registry:8081")
                        .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
                        .withEnv(
                                "CONNECT_PLUGIN_PATH",
                                "/usr/share/java,/usr/share/confluent-hub-components,/connect-plugins")
                        .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
                        .withFileSystemBind(
                                System.getProperty("user.dir") + "/build/libs",
                                "/connect-plugins/connect-transforms",
                                BindMode.READ_ONLY)
                        .waitingFor(
                                Wait.forHttp("/connectors")
                                        .forPort(CONNECT_PORT)
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        connect.start();
        connectUrl =
                String.format(
                        "http://%s:%d", connect.getHost(), connect.getMappedPort(CONNECT_PORT));
        LOG.info("Kafka Connect started at {}", connectUrl);

        waitForConnectReady();
    }

    @AfterAll
    static void stopContainers() {
        if (connect != null) {
            connect.stop();
        }
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

    private static void waitForConnectReady() throws Exception {
        LOG.info("Waiting for Kafka Connect to be ready...");
        int maxAttempts = 30;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                HttpRequest request =
                        HttpRequest.newBuilder()
                                .uri(URI.create(connectUrl + "/connectors"))
                                .GET()
                                .build();
                HttpResponse<String> response =
                        HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    LOG.info("Kafka Connect is ready");
                    return;
                }
            } catch (Exception e) {
                LOG.debug("Connect not ready yet: {}", e.getMessage());
            }
            Thread.sleep(2000);
        }
        throw new RuntimeException("Kafka Connect failed to start within timeout");
    }

    protected void createTopic(final String topicName, final int partitions) throws Exception {
        try (AdminClient admin =
                AdminClient.create(
                        Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(
                            java.util.Collections.singletonList(
                                    new NewTopic(topicName, partitions, (short) 1)))
                    .all()
                    .get();
            LOG.info("Created topic: {}", topicName);
        }
    }

    protected KafkaProducer<String, String> createStringProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    protected KafkaConsumer<String, String> createStringConsumer(final String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    protected void createConnector(final String name, final Map<String, String> config)
            throws IOException, InterruptedException {
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("name", name);
        connectorConfig.put("config", config);

        String json = OBJECT_MAPPER.writeValueAsString(connectorConfig);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(connectUrl + "/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

        HttpResponse<String> response =
                HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 201 && response.statusCode() != 200) {
            throw new RuntimeException("Failed to create connector: " + response.body());
        }
        LOG.info("Created connector: {}", name);
    }

    protected void deleteConnector(final String name) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(connectUrl + "/connectors/" + name))
                        .DELETE()
                        .build();

        HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        LOG.info("Deleted connector: {}", name);
    }

    protected JsonNode getConnectorStatus(final String name)
            throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(connectUrl + "/connectors/" + name + "/status"))
                        .GET()
                        .build();

        HttpResponse<String> response =
                HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            return null;
        }
        return OBJECT_MAPPER.readTree(response.body());
    }

    protected void waitForConnectorRunning(final String name, final Duration timeout)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            JsonNode status = getConnectorStatus(name);
            if (status != null) {
                String state = status.path("connector").path("state").asText();
                if ("RUNNING".equals(state)) {
                    JsonNode tasks = status.path("tasks");
                    if (tasks.isArray() && tasks.size() > 0) {
                        boolean allRunning = true;
                        for (JsonNode task : tasks) {
                            if (!"RUNNING".equals(task.path("state").asText())) {
                                allRunning = false;
                                break;
                            }
                        }
                        if (allRunning) {
                            LOG.info("Connector {} is running", name);
                            return;
                        }
                    }
                }
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException("Connector " + name + " failed to start within timeout");
    }
}
