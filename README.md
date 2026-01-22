# Custom Kafka Connect Transforms

[![CI](https://github.com/brandonbrown/connect-transforms/actions/workflows/ci.yml/badge.svg)](https://github.com/brandonbrown/connect-transforms/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/brandonbrown/connect-transforms/branch/main/graph/badge.svg)](https://codecov.io/gh/brandonbrown/connect-transforms)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub release](https://img.shields.io/github/v/release/brandonbrown/connect-transforms)](https://github.com/brandonbrown/connect-transforms/releases)

A collection of Single Message Transformations (SMTs) for Apache Kafka Connect.

## Transformations

### `StructToJson`

This transformation converts a Struct field into a JSON string. It works with all major schema formats supported by Kafka Connect:

- **Protobuf** (via Confluent ProtobufConverter)
- **Avro** (via Confluent AvroConverter)
- **JSON Schema** (via Confluent JsonSchemaConverter)

The transformation:

- Expects the record to have a `STRUCT` type field (when schema is present) or a `Map` field (schemaless)
- Converts the specified field to a valid JSON string
- Preserves the original structure of nested objects, arrays, and maps
- Produces JSON that can be round-tripped (parsed back to the original structure)
- Handles all primitive types, enums, unions/nullable fields, logical types, and complex nested structures

Exists in two variants:

- `com.bbrownsound.kafka.connect.transforms.StructToJson$Key` - works on keys
- `com.bbrownsound.kafka.connect.transforms.StructToJson$Value` - works on values

#### Configuration

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `field.name` | String | (required) | The name of the field containing the Struct to convert to JSON. |
| `output.field.name` | String | null | The name of the output field for the JSON string. If not specified, the original field is replaced. |
| `skip.missing.or.null` | Boolean | false | If true, records with missing or null field values are passed through without transformation. If false, a DataException is thrown. |

#### Example Configuration

**Basic usage - replace field with JSON string:**

```properties
transforms=StructToJson
transforms.StructToJson.type=com.bbrownsound.kafka.connect.transforms.StructToJson$Value
transforms.StructToJson.field.name=payload
```

**Convert to a different output field:**

```properties
transforms=StructToJson
transforms.StructToJson.type=com.bbrownsound.kafka.connect.transforms.StructToJson$Value
transforms.StructToJson.field.name=payload
transforms.StructToJson.output.field.name=payload_json
```

**Skip records with missing or null fields:**

```properties
transforms=StructToJson
transforms.StructToJson.type=com.bbrownsound.kafka.connect.transforms.StructToJson$Value
transforms.StructToJson.field.name=payload
transforms.StructToJson.skip.missing.or.null=true
```

### Schema Format Support

#### Protobuf

Works with protobuf messages converted by `ProtobufConverter`. Supports:
- All scalar types (int32, int64, float, double, bool, string, bytes, etc.)
- Nested messages
- Repeated fields (arrays)
- Map fields
- Enums (serialized as strings)
- Oneof fields

#### Avro

Works with Avro records converted by `AvroConverter`. Supports:
- All Avro primitive types (null, boolean, int, long, float, double, bytes, string)
- Records (nested structures)
- Arrays and Maps
- Enums
- Fixed types
- Union types (nullable fields)
- Logical types (date, time, timestamp, decimal, uuid)

#### JSON Schema

Works with JSON Schema validated records converted by `JsonSchemaConverter`. Supports:
- All JSON types (object, array, string, number, boolean, null)
- Nested objects with `$ref` references
- Arrays of objects and primitives
- `additionalProperties` (maps)
- Nullable types (`oneOf` with null)
- Format annotations (date-time, email, uri, uuid, etc.)

### Example: Protobuf to JSON

If you have a protobuf message like:

```protobuf
message Order {
  string order_id = 1;
  repeated Item items = 2;
  
  message Item {
    string sku = 1;
    int32 quantity = 2;
  }
}
```

And your Kafka Connect record value has the structure:

```json
{
  "metadata": { "version": 1 },
  "order": {
    "order_id": "ORD-001",
    "items": [
      { "sku": "SKU-001", "quantity": 2 },
      { "sku": "SKU-002", "quantity": 1 }
    ]
  }
}
```

Using this transformation with `field.name=order`, the output would be:

```json
{
  "metadata": { "version": 1 },
  "order": "{\"order_id\":\"ORD-001\",\"items\":[{\"sku\":\"SKU-001\",\"quantity\":2},{\"sku\":\"SKU-002\",\"quantity\":1}]}"
}
```

The JSON string in the `order` field is valid JSON that can be parsed by downstream systems.

## Building

```bash
./gradlew build
```

## Testing

### Unit Tests

```bash
./gradlew test
```

### Integration Tests

Integration tests use Testcontainers to spin up real Kafka, Schema Registry, and Kafka Connect instances. Docker must be running.

```bash
./gradlew integrationTest
```

### All Tests

```bash
./gradlew test integrationTest
```

### Test Coverage

The project includes comprehensive tests organized into:

**Unit Tests (`StructToJsonTest` - 41 tests):**
- Configuration validation
- Schema-based transformations (simple, nested, arrays, maps, all primitive types)
- Schemaless transformations
- Key and Value transformation variants
- Null/missing field handling
- JSON validity and round-trip verification
- Unicode and special character handling
- Schema caching
- Protobuf-like structure tests

**Protobuf Integration Tests (`StructToJsonProtobufIntegrationTest` - 13 tests):**
- Uses actual protobuf messages defined in `src/test/proto/test_messages.proto`
- Tests with `SimpleMessage`, `Person`, `Order`, `UserEvent`, `UserPreferences`, `AllTypesMessage`
- Verifies nested messages, repeated fields, map fields, enums
- All scalar protobuf types
- Round-trip validation
- Empty/default value handling

**Avro Integration Tests (`StructToJsonAvroIntegrationTest` - 10 tests):**
- Uses Avro schemas defined in `src/test/resources/avro/*.avsc`
- Tests with `SimpleRecord`, `Person`, `Order`, `UserPreferences`, `NullableRecord`, `AllTypesRecord`
- Verifies nested records, arrays, maps, enums, fixed types
- Union types (nullable fields)
- All Avro primitive types
- Round-trip validation with special characters and Unicode

**JSON Schema Integration Tests (`StructToJsonSchemaIntegrationTest` - 12 tests):**
- Simulates JSON Schema structures as produced by `JsonSchemaConverter`
- Tests with nested objects, `$ref` references, arrays, maps
- `additionalProperties` handling
- Nullable types
- Format annotations (email, uri, date-time, uuid)
- Complex deeply nested structures
- Round-trip validation

**End-to-End Integration Tests (Testcontainers):**

Located in `src/integration-test/java/`:

- `StructToJsonEmbeddedConnectTest` - Tests with real Kafka and Schema Registry
  - Full Avro serialization/deserialization cycle
  - Deeply nested structure transformation
  - Special characters and Unicode handling
  - Round-trip JSON integrity verification

## Installation

### From GitHub Packages

Add the GitHub Packages Maven repository to your build configuration:

**Gradle:**
```groovy
repositories {
    maven {
        url = uri("https://maven.pkg.github.com/brandonbrown/connect-transforms")
        credentials {
            username = project.findProperty("gpr.user") ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.key") ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation 'com.bbrownsound:connect-transforms:VERSION'
}
```

### From Source

1. Build the project:
   ```bash
   ./gradlew build
   ```

2. Copy the plugin JAR file to your Kafka Connect plugins directory:
   ```bash
   cp build/libs/connect-transforms-*-connect-plugin.jar /path/to/kafka/connect/plugins/
   ```

3. Restart Kafka Connect to load the new transformation.

### Docker Image

A pre-built Docker image with Kafka Connect and this transformation is available:

```bash
docker pull ghcr.io/brandonbrown/connect-transforms:latest
```

Or use a specific version:
```bash
docker pull ghcr.io/brandonbrown/connect-transforms:0.1.0
```

## CI/CD

This project uses GitHub Actions for continuous integration and deployment:

### Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **CI** | Push to main, PRs | Runs unit tests, integration tests, coverage reports |
| **PR Checks** | Pull requests | Checkstyle, dependency review, multi-JDK testing |
| **Release** | Tags (v*) | Publishes to GitHub Packages, creates GitHub Release |

### Creating a Release

1. Update `gradle.properties` with the new version
2. Create and push a tag:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
3. The release workflow will automatically:
   - Run all tests
   - Publish to GitHub Packages
   - Create a GitHub Release with artifacts
   - Build and push Docker images

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`./gradlew test integrationTest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the Apache License, Version 2.0.

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
