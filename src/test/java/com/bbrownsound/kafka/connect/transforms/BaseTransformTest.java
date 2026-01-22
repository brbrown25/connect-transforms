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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for StructToJson transformation tests. Provides common setup, teardown, and utility
 * methods.
 */
public abstract class BaseTransformTest {

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected static final TypeReference<Map<String, Object>> MAP_TYPE_REF =
            new TypeReference<Map<String, Object>>() {};

    protected StructToJson<SourceRecord> transform;

    protected abstract String getTopicName();

    @BeforeEach
    void setUp() {
        transform = new StructToJson.Value<>();
    }

    @AfterEach
    void tearDown() {
        if (transform != null) {
            transform.close();
        }
    }

    /**
     * Creates a SourceRecord with the given schema and value.
     *
     * @param schema the value schema
     * @param value the value
     * @return a new SourceRecord
     */
    protected SourceRecord createRecord(final Schema schema, final Object value) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                getTopicName(),
                0,
                null,
                null,
                schema,
                value);
    }

    /**
     * Configures the transform with the specified field name.
     *
     * @param fieldName the field to transform to JSON
     */
    protected void configureTransform(final String fieldName) {
        final Map<String, Object> config = new HashMap<>();
        config.put(StructToJsonConfig.FIELD_NAME_CONFIG, fieldName);
        transform.configure(config);
    }

    /**
     * Configures the transform with field name and output field name.
     *
     * @param fieldName the field to transform to JSON
     * @param outputFieldName the output field name for the JSON string
     */
    protected void configureTransform(final String fieldName, final String outputFieldName) {
        final Map<String, Object> config = new HashMap<>();
        config.put(StructToJsonConfig.FIELD_NAME_CONFIG, fieldName);
        config.put(StructToJsonConfig.OUTPUT_FIELD_NAME_CONFIG, outputFieldName);
        transform.configure(config);
    }

    /**
     * Configures the transform with field name and skip missing/null option.
     *
     * @param fieldName the field to transform to JSON
     * @param skipMissingOrNull whether to skip missing or null fields
     */
    protected void configureTransform(final String fieldName, final boolean skipMissingOrNull) {
        final Map<String, Object> config = new HashMap<>();
        config.put(StructToJsonConfig.FIELD_NAME_CONFIG, fieldName);
        config.put(StructToJsonConfig.SKIP_MISSING_OR_NULL_CONFIG, skipMissingOrNull);
        transform.configure(config);
    }

    /**
     * Applies the transform and returns the result struct.
     *
     * @param schema the input schema
     * @param value the input value
     * @return the transformed struct
     */
    protected Struct applyTransform(final Schema schema, final Object value) {
        final SourceRecord record = createRecord(schema, value);
        final SourceRecord result = transform.apply(record);
        return (Struct) result.value();
    }

    /**
     * Parses a JSON string into a Map.
     *
     * @param json the JSON string
     * @return the parsed map
     * @throws JsonProcessingException if parsing fails
     */
    protected Map<String, Object> parseJson(final String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, MAP_TYPE_REF);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getMap(final Map<String, Object> map, final String key) {
        return (Map<String, Object>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, String> getStringMap(final Map<String, Object> map, final String key) {
        return (Map<String, String>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Integer> getIntegerMap(final Map<String, Object> map, final String key) {
        return (Map<String, Integer>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected List<String> getStringList(final Map<String, Object> map, final String key) {
        return (List<String>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected List<Integer> getIntegerList(final Map<String, Object> map, final String key) {
        return (List<Integer>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected List<Map<String, Object>> getListOfMaps(
            final Map<String, Object> map, final String key) {
        return (List<Map<String, Object>>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    protected List<String> getStringList(final Struct struct, final String field) {
        return (List<String>) struct.get(field);
    }

    protected void assertJsonEquals(final String expectedJson, final String actualJson)
            throws JsonProcessingException {
        final Map<String, Object> expectedMap = parseJson(expectedJson);
        final Map<String, Object> actualMap = parseJson(actualJson);
        assertEquals(expectedMap, actualMap);
    }

    protected Struct wrapStruct(final String fieldName, final Struct struct) {
        final Schema wrapperSchema =
                SchemaBuilder.struct().field(fieldName, struct.schema()).build();
        return new Struct(wrapperSchema).put(fieldName, struct);
    }

    protected Struct wrapStruct(
            final String fieldName1,
            final Struct struct1,
            final String fieldName2,
            final Schema schema2,
            final Object value2) {
        final Schema wrapperSchema =
                SchemaBuilder.struct()
                        .field(fieldName1, struct1.schema())
                        .field(fieldName2, schema2)
                        .build();
        return new Struct(wrapperSchema).put(fieldName1, struct1).put(fieldName2, value2);
    }

    protected String transformAndGetJson(final String fieldName, final Struct value)
            throws JsonProcessingException {
        configureTransform(fieldName);
        final Struct result = applyTransform(value.schema(), value);
        return (String) result.get(fieldName);
    }

    protected String transformAndGetJson(
            final String fieldName, final String outputFieldName, final Struct value)
            throws JsonProcessingException {
        configureTransform(fieldName, outputFieldName);
        final Struct result = applyTransform(value.schema(), value);
        return (String) result.get(outputFieldName);
    }

    protected TransformResult transformWithResult(final String fieldName, final Struct value) {
        configureTransform(fieldName);
        final Struct result = applyTransform(value.schema(), value);
        return new TransformResult(result, (String) result.get(fieldName));
    }

    protected record TransformResult(Struct struct, String json) {
        public Map<String, Object> parsedJson() throws JsonProcessingException {
            return OBJECT_MAPPER.readValue(json, MAP_TYPE_REF);
        }
    }
}
