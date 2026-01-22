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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/** Configuration for the {@link StructToJson} transformation. */
public class StructToJsonConfig extends AbstractConfig {

    /** Configuration key for the field name to transform. */
    public static final String FIELD_NAME_CONFIG = "field.name";

    private static final String FIELD_NAME_DOC =
            "The name of the field containing the Struct to convert to JSON.";

    /** Configuration key for the output field name. */
    public static final String OUTPUT_FIELD_NAME_CONFIG = "output.field.name";

    private static final String OUTPUT_FIELD_NAME_DOC =
            "The name of the output field for the JSON string. "
                    + "If not specified, the original field is replaced.";

    /** Configuration key for skip missing or null behavior. */
    public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";

    private static final String SKIP_MISSING_OR_NULL_DOC =
            "If true, records with missing or null field values are passed through without transformation. "
                    + "If false, a DataException is thrown.";
    private static final boolean SKIP_MISSING_OR_NULL_DEFAULT = false;

    /** Configuration definition for this transformation. */
    public static final ConfigDef CONFIG_DEF =
            new ConfigDef()
                    .define(
                            FIELD_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            ConfigDef.NO_DEFAULT_VALUE,
                            new ConfigDef.NonEmptyString(),
                            ConfigDef.Importance.HIGH,
                            FIELD_NAME_DOC)
                    .define(
                            OUTPUT_FIELD_NAME_CONFIG,
                            ConfigDef.Type.STRING,
                            null,
                            ConfigDef.Importance.MEDIUM,
                            OUTPUT_FIELD_NAME_DOC)
                    .define(
                            SKIP_MISSING_OR_NULL_CONFIG,
                            ConfigDef.Type.BOOLEAN,
                            SKIP_MISSING_OR_NULL_DEFAULT,
                            ConfigDef.Importance.MEDIUM,
                            SKIP_MISSING_OR_NULL_DOC);

    private final String fieldName;
    private final String outputFieldName;
    private final boolean skipMissingOrNull;

    /**
     * Creates a new configuration instance.
     *
     * @param originals the configuration properties
     */
    public StructToJsonConfig(final Map<String, ?> originals) {
        super(CONFIG_DEF, originals);

        this.fieldName = getString(FIELD_NAME_CONFIG);

        final String configuredOutputFieldName = getString(OUTPUT_FIELD_NAME_CONFIG);

        if (configuredOutputFieldName == null || configuredOutputFieldName.trim().isEmpty()) {
            this.outputFieldName = this.fieldName;
        } else {
            this.outputFieldName = configuredOutputFieldName;
        }

        this.skipMissingOrNull = getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
    }

    /**
     * Returns the name of the field to transform.
     *
     * @return the field name
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Returns the name of the output field for the JSON string.
     *
     * @return the output field name
     */
    public String outputFieldName() {
        return outputFieldName;
    }

    /**
     * Returns whether to skip records with missing or null field values.
     *
     * @return true if missing/null fields should be skipped, false otherwise
     */
    public boolean skipMissingOrNull() {
        return skipMissingOrNull;
    }
}
