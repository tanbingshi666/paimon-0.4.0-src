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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.FlinkCdcSyncTableSinkBuilder;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.Action.getConfigMap;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple MySQL tables into one Paimon table.
 *
 * <p>You should specify MySQL source table in {@code mySqlConfig}. See <a
 * href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified MySQL tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified MySQL tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 */
public class MySqlSyncTableAction implements Action {

    private final Configuration mySqlConfig;
    private final String warehouse;
    private final String database;
    private final String table;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private final List<String> computedColumnArgs;
    private final Map<String, String> catalogConfig;
    private final Map<String, String> tableConfig;

    MySqlSyncTableAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this(
                mySqlConfig,
                warehouse,
                database,
                table,
                partitionKeys,
                primaryKeys,
                Collections.emptyList(),
                catalogConfig,
                tableConfig);
    }

    MySqlSyncTableAction(
            Map<String, String> mySqlConfig,
            String warehouse,
            String database,
            String table,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<String> computedColumnArgs,
            Map<String, String> catalogConfig,
            Map<String, String> tableConfig) {
        this.mySqlConfig = Configuration.fromMap(mySqlConfig);
        this.warehouse = warehouse;
        this.database = database;
        this.table = table;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.computedColumnArgs = computedColumnArgs;
        this.catalogConfig = catalogConfig;
        this.tableConfig = tableConfig;
    }

    public void build(StreamExecutionEnvironment env) throws Exception {
        /**
         * 构建 mysql cdc source
         */
        MySqlSource<String> source = MySqlActionUtils.buildMySqlSource(mySqlConfig);

        /**
         * 创建 paimon catalog
         * 根据 catalogConfig 创建对应的 catalog
         * 默认 paimon 有两种 catalog 类型
         * 1. FileSystemCatalog (default)
         * 2. HiveCatalog
         */
        Catalog catalog =
                FlinkCatalogFactory.createPaimonCatalog(
                        Options.fromMap(catalogConfig).set(CatalogOptions.WAREHOUSE, warehouse));
        boolean caseSensitive = catalog.caseSensitive();

        /**
         * HiveCatalog 大小写敏感
         */
        if (!caseSensitive) {
            validateCaseInsensitive();
        }

        /**
         * paimon 支持实时读取 mysql 多张表数据合并到 paimon 一张表
         * 因此需要将 mysql 多张表的字段 schema 进行合并
         */
        MySqlSchema mySqlSchema =
                getMySqlSchemaList().stream()
                        .reduce(MySqlSchema::merge)
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No table satisfies the given database name and table name"));

        /**
         * 创建 paimon 数据库
         */
        catalog.createDatabase(database, true);

        Identifier identifier = new Identifier(database, table);
        FileStoreTable table;
        /**
         * 如果命令行指定了 --computed-column 参数
         */
        List<ComputedColumn> computedColumns =
                MySqlActionUtils.buildComputedColumns(
                        computedColumnArgs, mySqlSchema.typeMapping());

        /**
         * 构建 paimon 表 schema
         */
        Schema fromMySql =
                MySqlActionUtils.buildPaimonSchema(
                        mySqlSchema,
                        partitionKeys,
                        primaryKeys,
                        computedColumns,
                        tableConfig,
                        caseSensitive);
        try {
            table = (FileStoreTable) catalog.getTable(identifier);
            checkArgument(
                    computedColumnArgs.isEmpty(),
                    "Cannot add computed column when table already exists.");
            MySqlActionUtils.assertSchemaCompatible(table.schema(), fromMySql);
        } catch (Catalog.TableNotExistException e) {
            /**
             * 创建 paimon 表
             */
            catalog.createTable(identifier, fromMySql, false);
            table = (FileStoreTable) catalog.getTable(identifier);
        }

        String serverTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        ZoneId zoneId = serverTimeZone == null ? ZoneId.systemDefault() : ZoneId.of(serverTimeZone);
        EventParser.Factory<String> parserFactory =
                () -> new MySqlDebeziumJsonEventParser(zoneId, caseSensitive, computedColumns);

        FlinkCdcSyncTableSinkBuilder<String> sinkBuilder =
                new FlinkCdcSyncTableSinkBuilder<String>()
                        .withInput(
                                env.fromSource(
                                        source, WatermarkStrategy.noWatermarks(), "MySQL Source"))
                        .withParserFactory(parserFactory)
                        .withTable(table);
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            sinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }
        sinkBuilder.build();
    }

    private void validateCaseInsensitive() {
        checkArgument(
                database.equals(database.toLowerCase()),
                String.format(
                        "Database name [%s] cannot contain upper case in case-insensitive catalog.",
                        database));
        checkArgument(
                table.equals(table.toLowerCase()),
                String.format(
                        "Table name [%s] cannot contain upper case in case-insensitive catalog.",
                        table));
        for (String part : partitionKeys) {
            checkArgument(
                    part.equals(part.toLowerCase()),
                    String.format(
                            "Partition keys [%s] cannot contain upper case in case-insensitive catalog.",
                            partitionKeys));
        }
        for (String pk : primaryKeys) {
            checkArgument(
                    pk.equals(pk.toLowerCase()),
                    String.format(
                            "Primary keys [%s] cannot contain upper case in case-insensitive catalog.",
                            primaryKeys));
        }
    }

    private List<MySqlSchema> getMySqlSchemaList() throws Exception {
        Pattern databasePattern =
                Pattern.compile(mySqlConfig.get(MySqlSourceOptions.DATABASE_NAME));
        Pattern tablePattern = Pattern.compile(mySqlConfig.get(MySqlSourceOptions.TABLE_NAME));

        List<MySqlSchema> mySqlSchemaList = new ArrayList<>();
        /**
         * 基于原生的 JDBC 方式创建 Connection
         */
        try (Connection conn = MySqlActionUtils.getConnection(mySqlConfig)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet schemas = metaData.getCatalogs()) {
                while (schemas.next()) {
                    String databaseName = schemas.getString("TABLE_CAT");
                    Matcher databaseMatcher = databasePattern.matcher(databaseName);
                    if (databaseMatcher.matches()) {
                        try (ResultSet tables = metaData.getTables(databaseName, null, "%", null)) {
                            while (tables.next()) {
                                String tableName = tables.getString("TABLE_NAME");
                                Matcher tableMatcher = tablePattern.matcher(tableName);
                                if (tableMatcher.matches()) {
                                    /**
                                     * 一张表对应一个 MySqlSchema
                                     * 里面会解析字段和字段类型
                                     */
                                    mySqlSchemaList.add(
                                            new MySqlSchema(metaData, databaseName, tableName));
                                }
                            }
                        }
                    }
                }
            }
        }
        return mySqlSchemaList;
    }

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    public static Optional<Action> create(String[] args) {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        if (params.has("help")) {
            printHelp();
            return Optional.empty();
        }

        /**
         * 根据命令行解析 warehouse、database、table 参数
         */
        Tuple3<String, String, String> tablePath = Action.getTablePath(params);
        if (tablePath == null) {
            return Optional.empty();
        }

        List<String> partitionKeys = Collections.emptyList();
        if (params.has("partition-keys")) {
            partitionKeys =
                    Arrays.stream(params.get("partition-keys").split(","))
                            .collect(Collectors.toList());
        }

        List<String> primaryKeys = Collections.emptyList();
        if (params.has("primary-keys")) {
            primaryKeys =
                    Arrays.stream(params.get("primary-keys").split(","))
                            .collect(Collectors.toList());
        }

        List<String> computedColumnArgs = Collections.emptyList();
        if (params.has("computed-column")) {
            computedColumnArgs = new ArrayList<>(params.getMultiParameter("computed-column"));
        }

        Optional<Map<String, String>> mySqlConfig = getConfigMap(params, "mysql-conf");
        Optional<Map<String, String>> catalogConfig = getConfigMap(params, "catalog-conf");
        Optional<Map<String, String>> tableConfig = getConfigMap(params, "table-conf");
        if (!mySqlConfig.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(
                new MySqlSyncTableAction(
                        // --mysql-conf 参数
                        mySqlConfig.get(),
                        // --warehouse 参数
                        tablePath.f0,
                        // --database 参数
                        tablePath.f1,
                        // --table 参数
                        tablePath.f2,
                        // --partition-keys 参数
                        partitionKeys,
                        // --primary-keys 参数
                        primaryKeys,
                        // --computed-column 参数
                        computedColumnArgs,
                        // --catalog-conf 参数
                        catalogConfig.orElse(Collections.emptyMap()),
                        // --table-conf 参数
                        tableConfig.orElse(Collections.emptyMap())));
    }

    private static void printHelp() {
        System.out.println(
                "Action \"mysql-sync-table\" creates a streaming job "
                        + "with a Flink MySQL CDC source and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql-sync-table --warehouse <warehouse-path> --database <database-name> "
                        + "--table <table-name> "
                        + "[--partition-keys <partition-keys>] "
                        + "[--primary-keys <primary-keys>] "
                        + "[--computed-column <'column-name=expr-name(args[, ...])'> [--computed-column ...]] "
                        + "[--mysql-conf <mysql-cdc-source-conf> [--mysql-conf <mysql-cdc-source-conf> ...]] "
                        + "[--catalog-conf <paimon-catalog-conf> [--catalog-conf <paimon-catalog-conf> ...]] "
                        + "[--table-conf <paimon-table-sink-conf> [--table-conf <paimon-table-sink-conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from MySQL tables if not specified.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed-column.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name' and 'table-name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql-sync-table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition-keys pt \\\n"
                        + "    --primary-keys pt,uid \\\n"
                        + "    --mysql-conf hostname=127.0.0.1 \\\n"
                        + "    --mysql-conf username=root \\\n"
                        + "    --mysql-conf password=123456 \\\n"
                        + "    --mysql-conf database-name=source_db \\\n"
                        + "    --mysql-conf table-name='source_table_.*' \\\n"
                        + "    --catalog-conf metastore=hive \\\n"
                        + "    --catalog-conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table-conf bucket=4 \\\n"
                        + "    --table-conf changelog-producer=input \\\n"
                        + "    --table-conf sink.parallelism=4");
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        env.execute(String.format("MySQL-Paimon Table Sync: %s.%s", database, table));
    }
}
