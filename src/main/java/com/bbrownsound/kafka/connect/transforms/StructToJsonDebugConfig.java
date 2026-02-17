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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Configuration for the {@link StructToJsonDebug} transformation (multi-field mappings). */
public class StructToJsonDebugConfig extends AbstractConfig {

    /** Configuration key for comma-separated input_field:output_field mappings. */
    public static final String FIELD_MAPPINGS_CONFIG = "field.mappings";

    private static final String FIELD_MAPPINGS_DOC =
            "Comma-separated list of input_field:output_field. "
                    + "Each listed input field (Struct) is converted to a JSON string and written to the "
                    + "corresponding output field. Example: payload:payload_json,meta:meta_json. "
                    + "If output equals input, the field is replaced in place.";

    /** Configuration key for skip missing or null behavior. */
    public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";

    private static final String SKIP_MISSING_OR_NULL_DOC =
            "If true, records with missing or null field values for a mapping are skipped for that "
                    + "mapping only; other mappings still apply. If false, a DataException is thrown.";
    private static final boolean SKIP_MISSING_OR_NULL_DEFAULT = false;

    /** Configuration definition for this transformation. */
    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELD_MAPPINGS_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            new FieldMappingsValidator(),
                            ConfigDef.Importance.HIGH,
                            FIELD_MAPPINGS_DOC)
                    .define(
                            SKIP_MISSING_OR_NULL_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            SKIP_MISSING_OR_NULL_DEFAULT,
                            ConfigDef.Importance.MEDIUM,
                            SKIP_MISSING_OR_NULL_DOC);

    private final List<FieldMapping> fieldMappings;
    private final boolean skipMissingOrNull;

    /**
     * Creates a new configuration instance.
     *
     * @param originals the configuration properties
     */
    public StructToJsonDebugConfig(final Map<String, ?> originals) {
        super(CONFIG_DEF, originals);

        this.fieldMappings = parseFieldMappings(getString(FIELD_MAPPINGS_CONFIG));
        this.skipMissingOrNull = getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
    }

    private static List<FieldMapping> parseFieldMappings(final String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigException(FIELD_MAPPINGS_CONFIG, value, "Must not be empty");
        }
        final List<FieldMapping> result = new ArrayList<>();
        for (final String part : value.split(",")) {
            final String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final String[] pair = trimmed.split(":", 2);
            if (pair.length != 2) {
                throw new ConfigException(
                        FIELD_MAPPINGS_CONFIG,
                        value,
                        "Each mapping must be input_field:output_field, got: " + trimmed);
            }
            final String inputField = pair[0].trim();
            final String outputField = pair[1].trim();
            if (inputField.isEmpty() || outputField.isEmpty()) {
                throw new ConfigException(
                        FIELD_MAPPINGS_CONFIG,
                        value,
                        "Input and output field names must not be empty in: " + trimmed);
            }
            result.add(new FieldMapping(inputField, outputField));
        }
        if (result.isEmpty()) {
            throw new ConfigException(
                    FIELD_MAPPINGS_CONFIG, value, "At least one mapping required");
        }
        return result;
    }

    /**
     * Returns the list of input→output field mappings.
     *
     * @return the field mappings (never null or empty)
     */
    public List<FieldMapping> fieldMappings() {
        return fieldMappings;
    }

    /**
     * Returns whether to skip records (or individual mappings) with missing or null field values.
     *
     * @return true if missing/null should be skipped, false otherwise
     */
    public boolean skipMissingOrNull() {
        return skipMissingOrNull;
    }

    /** A single input field → output field mapping. */
    public static final class FieldMapping {
        private final String inputField;
        private final String outputField;

        FieldMapping(final String inputField, final String outputField) {
            this.inputField = inputField;
            this.outputField = outputField;
        }

        public String inputField() {
            return inputField;
        }

        public String outputField() {
            return outputField;
        }
    }

    private static final class FieldMappingsValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                throw new ConfigException(name, value, "Must not be null");
            }
            parseFieldMappings((String) value);
        }
    }
}
