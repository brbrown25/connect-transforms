# Kafka Connect image with custom transforms plugin
# Build stage
FROM gradle:8.5-jdk17 AS builder

WORKDIR /app
COPY . .
RUN gradle connectPluginJar -x test --no-daemon

# Runtime stage
FROM confluentinc/cp-kafka-connect:7.5.3

# Copy the plugin JAR to the Connect plugins directory
COPY --from=builder /app/build/libs/*-connect-plugin.jar /usr/share/confluent-hub-components/connect-transforms/

# Set plugin path
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

# Labels
LABEL org.opencontainers.image.source="https://github.com/brandonbrown/connect-transforms"
LABEL org.opencontainers.image.description="Kafka Connect with StructToJson transformation"
LABEL org.opencontainers.image.licenses="Apache-2.0"
