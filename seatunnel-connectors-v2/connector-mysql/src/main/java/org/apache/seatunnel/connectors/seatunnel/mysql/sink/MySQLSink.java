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

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@AutoService(SeaTunnelSink.class)
@NoArgsConstructor
public class MySQLSink
        implements SeaTunnelSink<SeaTunnelRow, MySQLSinkState, MySQLCommitInfo, MySQLCommitInfo>,
        SupportDataSaveMode {

    private SeaTunnelRowType seaTunnelRowType;

    private JdbcSinkConfig jdbcSinkConfig;

    private JdbcDialect dialect;

    private ReadonlyConfig config;

    private DataSaveMode dataSaveMode;

    private List<String> catalogTables;

    public MySQLSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.dataSaveMode = dataSaveMode;
        this.seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    public String getPluginName() {
        return "MySQL";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
        this.jdbcSinkConfig = JdbcSinkConfig.of(config);
        this.dialect = JdbcDialectLoader.load(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        this.dataSaveMode = DataSaveMode.KEEP_SCHEMA_AND_DATA;
        try (Catalog catalog = new MySqlCatalogFactory().createCatalog("mysql", config)) {
            catalog.open();
            catalogTables = catalog.listTables(config.get(JdbcOptions.DATABASE));
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, MySQLCommitInfo, MySQLSinkState> createWriter(
            SinkWriter.Context context) {
        List<CatalogTable> tables = new ArrayList<>();
        try (Catalog catalog = new MySqlCatalogFactory().createCatalog("mysql", config)) {
            catalog.open();
            String database = config.get(JdbcOptions.DATABASE);
            catalogTables = catalog.listTables(database);
            catalogTables.forEach(
                   catalogTable -> {
                       CatalogTable table = catalog.getTable(TablePath.of(database, catalogTable));
                       tables.add(table);
                   }
            );
        }
        return new MySQLSinkWriter(tables, dialect, jdbcSinkConfig, seaTunnelRowType);
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return dataSaveMode;
    }

    @Override
    public List<DataSaveMode> supportedDataSaveModeValues() {
        return Collections.singletonList(DataSaveMode.KEEP_SCHEMA_AND_DATA);
    }

    @Override
    public void handleSaveMode(DataSaveMode saveMode) {
    }
}
