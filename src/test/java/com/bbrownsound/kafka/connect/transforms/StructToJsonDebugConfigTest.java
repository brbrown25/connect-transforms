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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class StructToJsonDebugConfigTest {

    @Nested
    @DisplayName("Field mappings parsing")
    class FieldMappingsParsing {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"   ", "\t", "\n"})
        @DisplayName("Should throw ConfigException for null or empty field.mappings")
        void shouldThrowForNullOrEmptyMappings(final String value) {
            final Map<String, Object> config = new HashMap<>();
            if (value != null) {
                config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, value);
            }
            assertThrows(ConfigException.class, () -> new StructToJsonDebugConfig(config));
        }

        @Test
        @DisplayName("Should throw when mapping has no colon")
        void shouldThrowWhenMappingHasNoColon() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "payloadpayload_json");
            final ConfigException e =
                    assertThrows(ConfigException.class, () -> new StructToJsonDebugConfig(config));
            assertTrue(e.getMessage().contains("input_field:output_field"));
        }

        @Test
        @DisplayName("Should throw when input field is empty")
        void shouldThrowWhenInputFieldEmpty() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, ":output");
            final ConfigException e =
                    assertThrows(ConfigException.class, () -> new StructToJsonDebugConfig(config));
            assertTrue(e.getMessage().contains("must not be empty"));
        }

        @Test
        @DisplayName("Should throw when output field is empty")
        void shouldThrowWhenOutputFieldEmpty() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "input:");
            final ConfigException e =
                    assertThrows(ConfigException.class, () -> new StructToJsonDebugConfig(config));
            assertTrue(e.getMessage().contains("must not be empty"));
        }

        @Test
        @DisplayName("Should parse single mapping")
        void shouldParseSingleMapping() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "payload:payload_json");
            config.put(StructToJsonDebugConfig.SKIP_MISSING_OR_NULL_CONFIG, false);

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            List<StructToJsonDebugConfig.FieldMapping> mappings = c.fieldMappings();
            assertNotNull(mappings);
            assertEquals(1, mappings.size());
            assertEquals("payload", mappings.get(0).inputField());
            assertEquals("payload_json", mappings.get(0).outputField());
        }

        @Test
        @DisplayName("Should parse multiple mappings")
        void shouldParseMultipleMappings() {
            final Map<String, Object> config = new HashMap<>();
            config.put(
                    StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG,
                    "payload:payload_json,meta:meta_json,status:status");
            config.put(StructToJsonDebugConfig.SKIP_MISSING_OR_NULL_CONFIG, true);

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            List<StructToJsonDebugConfig.FieldMapping> mappings = c.fieldMappings();
            assertNotNull(mappings);
            assertEquals(3, mappings.size());
            assertEquals("payload", mappings.get(0).inputField());
            assertEquals("payload_json", mappings.get(0).outputField());
            assertEquals("meta", mappings.get(1).inputField());
            assertEquals("meta_json", mappings.get(1).outputField());
            assertEquals("status", mappings.get(2).inputField());
            assertEquals("status", mappings.get(2).outputField());
        }

        @Test
        @DisplayName("Should trim whitespace around mappings")
        void shouldTrimWhitespace() {
            final Map<String, Object> config = new HashMap<>();
            config.put(
                    StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG,
                    "  payload : payload_json  ,  meta : meta_json  ");

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            List<StructToJsonDebugConfig.FieldMapping> mappings = c.fieldMappings();
            assertEquals(2, mappings.size());
            assertEquals("payload", mappings.get(0).inputField());
            assertEquals("payload_json", mappings.get(0).outputField());
            assertEquals("meta", mappings.get(1).inputField());
            assertEquals("meta_json", mappings.get(1).outputField());
        }

        @Test
        @DisplayName("Should allow output field with colon in name (split on first colon)")
        void shouldAllowColonInOutputField() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "a:b:c");

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            List<StructToJsonDebugConfig.FieldMapping> mappings = c.fieldMappings();
            assertEquals(1, mappings.size());
            assertEquals("a", mappings.get(0).inputField());
            assertEquals("b:c", mappings.get(0).outputField());
        }
    }

    @Nested
    @DisplayName("Skip missing or null")
    class SkipMissingOrNull {

        @Test
        @DisplayName("Should default skip.missing.or.null to false")
        void shouldDefaultSkipMissingOrNullToFalse() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "a:b");

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            assertFalse(c.skipMissingOrNull());
        }

        @Test
        @DisplayName("Should use configured skip.missing.or.null")
        void shouldUseConfiguredSkipMissingOrNull() {
            final Map<String, Object> config = new HashMap<>();
            config.put(StructToJsonDebugConfig.FIELD_MAPPINGS_CONFIG, "a:b");
            config.put(StructToJsonDebugConfig.SKIP_MISSING_OR_NULL_CONFIG, true);

            final StructToJsonDebugConfig c = new StructToJsonDebugConfig(config);

            assertTrue(c.skipMissingOrNull());
        }
    }
}
