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

package org.apache.seatunnel.connectors.seatunnel.mysql.sink;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MySQLSinkWriter implements SinkWriter<SeaTunnelRow, MySQLCommitInfo, MySQLSinkState> {

    private final JdbcConnectionProvider connectionProvider;
    private final List<CatalogTable> catalogTables;
    private final Map<CatalogTable, MySQLOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>>> outputFormatMap;
    private final JdbcDialect dialect;
    private final JdbcSinkConfig jdbcSinkConfig;
    private final SeaTunnelRowType rowType;

    public MySQLSinkWriter(
            List<CatalogTable> catalogTables,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            SeaTunnelRowType rowType) {
        this.catalogTables = catalogTables;
        this.connectionProvider =
                new SimpleJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.dialect = dialect;
        outputFormatMap = new HashMap<>();
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.rowType = rowType;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        SeaTunnelRow copy = SerializationUtils.clone(element);
        String tableId = copy.getTableId();
        CatalogTable catalogTableOfRowTableId = null;
        for (CatalogTable catalogTable : catalogTables) {
            TablePath tablePath = catalogTable.getTableId().toTablePath();
            if (tableId.equals(tablePath.toString())) {
                catalogTableOfRowTableId = catalogTable;
            }
        }
        write(element, catalogTableOfRowTableId);
    }

    public void write(SeaTunnelRow element, CatalogTable catalogTable) throws IOException {
        if (outputFormatMap.containsKey(catalogTable)) {
            outputFormatMap.get(catalogTable).writeRecord(element);
        } else {
            MySQLOutputFormat<SeaTunnelRow, JdbcBatchStatementExecutor<SeaTunnelRow>> output = new MySQLOutputFormatBuilder(dialect, connectionProvider, jdbcSinkConfig, rowType)
                    .build(catalogTable);
            outputFormatMap.put(catalogTable, output);
            output.open();
            output.writeRecord(element);
        }
    }

    @Override
    public Optional<MySQLCommitInfo> prepareCommit() {
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
    }

    @Override
    public void close() {
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED, "unable to close JDBC sink write", e);
        }
    }
}
