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

package org.apache.seatunnel.connectors.doris.client.load;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.HttpPut;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Slf4j
public abstract class DorisStreamLoad implements Serializable {

    public static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";

    public static final List<String> DROIS_SUCCESS_STATUS = Arrays.asList("Success", "Publish Timeout");

    public abstract HttpPut generateHttpPut(String url, String label, String mergeType, List<String> columnNames, List<String> values);
}
