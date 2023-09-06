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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncDatabaseAction;
import org.apache.paimon.flink.action.cdc.mysql.MySqlSyncTableAction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract class for Flink actions.
 */
public interface Action {

    /**
     * The execution method of the action.
     */
    void run() throws Exception;

    @Nullable
    static Tuple3<String, String, String> getTablePath(MultipleParameterTool params) {
        String warehouse = params.get("warehouse");
        String database = params.get("database");
        String table = params.get("table");
        String path = params.get("path");

        Tuple3<String, String, String> tablePath = null;
        int count = 0;
        if (warehouse != null || database != null || table != null) {
            if (warehouse == null || database == null || table == null) {
                System.err.println(
                        "Warehouse, database and table must be specified all at once.\n"
                                + "Run <action> --help for help.");
                return null;
            }
            tablePath = Tuple3.of(warehouse, database, table);
            count++;
        }
        if (path != null) {
            tablePath =
                    Tuple3.of(
                            CatalogUtils.warehouse(path),
                            CatalogUtils.database(path),
                            CatalogUtils.table(path));
            count++;
        }

        if (count != 1) {
            System.err.println(
                    "Please specify either \"warehouse, database and table\" or \"path\".\n"
                            + "Run <action> --help for help.");
            return null;
        }

        return tablePath;
    }

    @Nullable
    static List<Map<String, String>> getPartitions(MultipleParameterTool params) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : params.getMultiParameter("partition")) {
            Map<String, String> kvs = parseKeyValues(partition);
            if (kvs == null) {
                return null;
            }
            partitions.add(kvs);
        }

        return partitions;
    }

    static Map<String, String> parseKeyValues(String keyValues) {
        Map<String, String> kvs = new HashMap<>();
        for (String kvString : keyValues.split(",")) {
            String[] kv = kvString.split("=");
            if (kv.length != 2) {
                System.err.print(
                        "Invalid key-value pair \""
                                + kvString
                                + "\".\n"
                                + "Run <action> --help for help.");
                return null;
            }
            kvs.put(kv[0].trim(), kv[1].trim());
        }

        return kvs;
    }

    /**
     * Factory to create {@link Action}.
     */
    class Factory {

        // supported actions
        private static final String COMPACT = "compact";
        private static final String DROP_PARTITION = "drop-partition";
        private static final String DELETE = "delete";
        private static final String MERGE_INTO = "merge-into";
        // cdc actions
        private static final String MYSQL_SYNC_TABLE = "mysql-sync-table";
        private static final String MYSQL_SYNC_DATABASE = "mysql-sync-database";

        public static Optional<Action> create(String[] args) {
            String action = args[0].toLowerCase();
            String[] actionArgs = Arrays.copyOfRange(args, 1, args.length);

            switch (action) {
                case COMPACT:
                    return CompactAction.create(actionArgs);
                case DROP_PARTITION:
                    return DropPartitionAction.create(actionArgs);
                case DELETE:
                    return DeleteAction.create(actionArgs);
                case MERGE_INTO:
                    return MergeIntoAction.create(actionArgs);
                case MYSQL_SYNC_TABLE:
                    // 同步 mysql 单表
                    return MySqlSyncTableAction.create(actionArgs);
                case MYSQL_SYNC_DATABASE:
                    // 同步 mysql 单库
                    return MySqlSyncDatabaseAction.create(actionArgs);
                default:
                    System.err.println("Unknown action \"" + action + "\"");
                    printHelp();
                    return Optional.empty();
            }
        }

        public static void printHelp() {
            System.out.println("Usage: <action> [OPTIONS]");
            System.out.println();

            System.out.println("Available actions:");
            System.out.println("  " + COMPACT);
            System.out.println("  " + DROP_PARTITION);
            System.out.println("  " + DELETE);
            System.out.println("  " + MERGE_INTO);
            System.out.println("  " + MYSQL_SYNC_TABLE);
            System.out.println("  " + MYSQL_SYNC_DATABASE);

            System.out.println("For detailed options of each action, run <action> --help");
        }
    }

    static Optional<Map<String, String>> getConfigMap(MultipleParameterTool params, String key) {
        if (!params.has(key)) {
            return Optional.empty();
        }

        Map<String, String> map = new HashMap<>();
        for (String param : params.getMultiParameter(key)) {
            String[] kv = param.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
                continue;
            }

            System.err.println("Invalid key " + key + ". Please use format 'key=value'");
            return Optional.empty();
        }
        return Optional.of(map);
    }
}
