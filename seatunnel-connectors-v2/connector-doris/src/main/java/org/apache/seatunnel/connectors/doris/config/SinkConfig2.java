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

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;
import java.util.TimeZone;

@Getter
@Setter
@Builder
@ToString
public class SinkConfig2 {

    public static final Option<List<String>> NODE_URLS =
            Options.key("node_urls")
            .listType(String.class)
            .noDefaultValue()
            .withDescription("node urls");

    public static final Option<String> USERNAME =
            Options.key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("username");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Database name from doris."
                    );

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table name from doris."
                    );

    public static final Option<String> LABEL_PREFIX =
            Options.key("label_prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The prefix of stream load label."
                    );

    public static final Option<String> COLUMN_SEPARATOR =
            Options.key("column_separator")
                    .stringType()
                    .defaultValue("\t")
                    .withDescription(
                            "Used to specify the column separator in the load file. The default is '\\t'. " +
                            "If it is an invisible character, you need to add '\\x' as a prefix and hexadecimal" +
                                    " to indicate the separator. You can use a combination of multiple characters as the column separator."
                    );

    public static final Option<String> LINE_DELIMITER =
            Options.key("line_delimiter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Used to specify the line delimiter in the load file. The default is '\\n'." +
                            "You can use a combination of multiple characters as the column separator."
                    );

    public static final Option<Integer> MAX_FILTER_RATIO =
            Options.key("max_filter_ratio")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The maximum tolerance rate of the import task is 0 by default, and the range of values is 0-1. When the import error rate exceeds this value, the import fails." +
                            "If the user wishes to ignore the wrong row, the import can be successful by setting this parameter greater than 0."
                    );

    public static final Option<String> WHERE =
            Options.key("where")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Import the filter conditions specified by the task. Stream load supports filtering of where statements specified for raw data. The filtered data will not be imported or participated in the calculation of filter ratio, but will be counted as num_rows_unselected."
                    );

    public static final Option<String> PARTITIONS =
            Options.key("partitions")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Partitions information for tables to be imported will not be imported if the data to be imported does not belong to the specified Partition. These data will be included in 'dpp.abnorm.ALL'."
                    );

    public static final Option<String> COLUMNS =
            Options.key("columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The function transformation configuration of data to be imported includes the sequence change of columns and the expression transformation, in which the expression transformation method is consistent with the query statement."
                    );

    public static final Option<Long> EXEC_MEM_LIMIT =
            Options.key("exec_mem_limit")
                    .longType()
                    .defaultValue(2 * 1024 * 1024 * 1024L)
                    .withDescription(
                            "Memory limit. Default is 2GB. Unit is Bytes."
                    );

    public static final Option<Boolean> TWO_PHASE_COMMIT =
            Options.key("two_phase_commit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Stream load import can enable two-stage transaction commit mode: in the stream load process, the data is written and the information is returned to the user. " +
                                    "At this time, the data is invisible and the transaction status is PRECOMMITTED. After the user manually triggers the commit operation, the data is visible. " +
                                    "The default two-phase bulk transaction commit is off."
                    );

    public static final Option<Integer> MAX_RETRIES =
            Options.key("max_retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            ""
                    );

    public static final Option<Long> WAIT_RETRY_INTERVALS_MS =
            Options.key("wait_retry_intervals_ms")
                    .longType()
                    .defaultValue(6000L)
                    .withDescription(
                            ""
                    );

    public static final Option<Boolean> STRICT_MODE =
            Options.key("strict_mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The user specifies whether to enable strict mode for this import. The default is off."
                    );

    public static final Option<String> MERGE_TYPE =
            Options.key("merge_type")
                    .stringType()
                    .defaultValue("APPEND")
                    .withDescription(
                            "The type of data merging supports three types: APPEND, DELETE, and MERGE." +
                                    " APPEND is the default value, which means that all this batch of data needs to be appended to the existing data." +
                                    " DELETE means to delete all rows with the same key as this batch of data." +
                                    " MERGE semantics Need to be used in conjunction with the delete condition," +
                                    " which means that the data that meets the delete condition is processed according to DELETE semantics and the rest is processed according to APPEND semantics"
                    );

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            ""
                    );

    public static final Option<Long> BATCH_BYTES =
            Options.key("batch_bytes")
                    .longType()
                    .defaultValue(1024 * 1024 * 1024L)
                    .withDescription(
                            ""
                    );

    public static final Option<Integer> BATCH_INTERVAL_MS =
            Options.key("batch_interval_ms")
                    .intType()
                    .defaultValue(10 * 1000)
                    .withDescription(
                            ""
                    );

    public static final Option<String> TIMEZONE =
            Options.key("timezone")
                    .stringType()
                    .defaultValue(TimeZone.getDefault().getID())
                    .withDescription(
                            "Specify the time zone used for this import. The default is Dongba District. " +
                                    "This parameter affects the results of all time zone-related functions involved in the import."
                    );

    public static final Option<DorisFormatType> FORMAT =
            Options.key("format")
                    .enumType(DorisFormatType.class)
                    .defaultValue(DorisFormatType.CSV)
                    .withDescription(
                            ""
                    );

    private List<String> nodeUrls;
    private String username;
    private String password;

    private String database;
    private String table;
    private String labelPrefix;

    private String columnSeparator;
    private String lineDelimiter;
    private Integer maxFilterRatio;
    private String where;
    private String partitions;
    private String columns;
    private Long execMemLimit;
    private Boolean twoPhaseCommit;

    private Integer maxRetries;
    private Long waitRetryIntervalsMs;
    private Boolean strictMode;
    private String mergeType;

    private Integer batchSize;
    private Long batchBytes;
    private Integer batchIntervalMs;

    private String timeZone;
    private DorisFormatType format;

    public static SinkConfig2 loadConfig(@NonNull Config config) {
        SinkConfig2Builder builder = SinkConfig2.builder();

        if (config.hasPath(NODE_URLS.key())) {
            builder.nodeUrls(config.getStringList(NODE_URLS.key()));
        }

        if (config.hasPath(USERNAME.key())) {
            builder.username(config.getString(USERNAME.key()));
        }

        if (config.hasPath(PASSWORD.key())) {
            builder.password(config.getString(PASSWORD.key()));
        }

        if (config.hasPath(DATABASE.key())) {
            builder.database(config.getString(DATABASE.key()));
        }

        if (config.hasPath(TABLE.key())) {
            builder.table(config.getString(TABLE.key()));
        }

        if (config.hasPath(LABEL_PREFIX.key())) {
            builder.labelPrefix(config.getString(LABEL_PREFIX.key()));
        }

        if (config.hasPath(COLUMN_SEPARATOR.key())) {
            builder.columnSeparator(config.getString(COLUMN_SEPARATOR.key()));
        }

        if (config.hasPath(LINE_DELIMITER.key())) {
            builder.lineDelimiter(config.getString(LINE_DELIMITER.key()));
        }

        if (config.hasPath(MAX_FILTER_RATIO.key())) {
            builder.maxFilterRatio(config.getInt(MAX_FILTER_RATIO.key()));
        }

        if (config.hasPath(WHERE.key())) {
            builder.where(config.getString(WHERE.key()));
        }

        if (config.hasPath(PARTITIONS.key())) {
            builder.partitions(config.getString(PARTITIONS.key()));
        }

        if (config.hasPath(COLUMNS.key())) {
            builder.columns(config.getString(COLUMNS.key()));
        }

        if (config.hasPath(EXEC_MEM_LIMIT.key())) {
            builder.execMemLimit(config.getLong(EXEC_MEM_LIMIT.key()));
        }

        if (config.hasPath(TWO_PHASE_COMMIT.key())) {
            builder.twoPhaseCommit(config.getBoolean(TWO_PHASE_COMMIT.key()));
        }

        if (config.hasPath(MAX_RETRIES.key())) {
            builder.maxRetries(config.getInt(MAX_RETRIES.key()));
        }

        if (config.hasPath(WAIT_RETRY_INTERVALS_MS.key())) {
            builder.waitRetryIntervalsMs(config.getLong(WAIT_RETRY_INTERVALS_MS.key()));
        }

        if (config.hasPath(STRICT_MODE.key())) {
            builder.strictMode(config.getBoolean(STRICT_MODE.key()));
        }

        if (config.hasPath(MERGE_TYPE.key())) {
            builder.mergeType(config.getString(MERGE_TYPE.key()));
        }

        if (config.hasPath(BATCH_SIZE.key())) {
            builder.batchSize(config.getInt(BATCH_SIZE.key()));
        }

        if (config.hasPath(BATCH_BYTES.key())) {
            builder.batchBytes(config.getLong(BATCH_BYTES.key()));
        }

        if (config.hasPath(BATCH_INTERVAL_MS.key())) {
           builder.batchIntervalMs(config.getInt(BATCH_INTERVAL_MS.key()));
        }

        if (config.hasPath(TIMEZONE.key())) {
            builder.timeZone(config.getString(TIMEZONE.key()));
        }

        if (config.hasPath(FORMAT.key())) {
            builder.format(config.getEnum(DorisFormatType.class, FORMAT.key()));
        }

        return builder.build();
    }
}