/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.doris.config;

import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Locale;

@Getter
@AllArgsConstructor
public enum DorisFormatType {

    CSV("CSV"),
    JSON("JSON"),
    CSV_WITH_NAMES("CSV_WITH_NAMES"),
    CSV_WITH_NAMES_AND_TYPES("CSV_WITH_NAMES_AND_TYPES"),
    PARQUET("PARQUET"),
    ORC("ORC"),
    ;

    private final String name;

    public static DorisFormatType of(String typeName) {
        switch (typeName.toUpperCase(Locale.ROOT)) {
            case "CSV":
                return CSV;
            case "JSON":
                return JSON;
            case "CSV_WITH_NAMES":
                return CSV_WITH_NAMES;
            case "CSV_WITH_NAMES_AND_TYPES":
                return CSV_WITH_NAMES_AND_TYPES;
            case "PARQUET":
                return PARQUET;
            case "ORC":
                return ORC;
            default:
                throw new DorisConnectorException(DorisConnectorErrorCode.UNSUPPORTED_FORMAT_TYPE, "Doris connector doesn't support the format type of " + typeName);
        }
    }
}
