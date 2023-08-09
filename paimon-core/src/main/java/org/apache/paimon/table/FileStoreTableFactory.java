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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.paimon.CoreOptions.PATH;

/**
 * Factory to create {@link FileStoreTable}.
 */
public class FileStoreTableFactory {

    public static FileStoreTable create(CatalogContext context) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(CoreOptions.path(context.options()), context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return create(fileIO, context.options());
    }

    public static FileStoreTable create(FileIO fileIO, Path path) {
        Options options = new Options();
        options.set(PATH, path.toString());
        return create(fileIO, options);
    }

    public static FileStoreTable create(FileIO fileIO, Options options) {
        Path tablePath = CoreOptions.path(options);
        TableSchema tableSchema =
                new SchemaManager(fileIO, tablePath)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));
        return create(fileIO, tablePath, tableSchema, options);
    }

    public static FileStoreTable create(FileIO fileIO, Path tablePath, TableSchema tableSchema) {
        // 创建 FileStoreTable
        return create(fileIO, tablePath, tableSchema, new Options());
    }

    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions) {
        /**
         * {
         *   "id" : 0,
         *   "fields" : [ {
         *     "id" : 0,
         *     "name" : "id",
         *     "type" : "BIGINT NOT NULL"
         *   }, {
         *     "id" : 1,
         *     "name" : "a",
         *     "type" : "INT"
         *   }, {
         *     "id" : 2,
         *     "name" : "b",
         *     "type" : "STRING"
         *   }, {
         *     "id" : 3,
         *     "name" : "dt",
         *     "type" : "STRING NOT NULL"
         *   } ],
         *   "highestFieldId" : 3,
         *   "partitionKeys" : [ "dt" ],
         *   "primaryKeys" : [ "id", "dt" ],
         *   "options" : { }
         * }
         */

        FileStoreTable table;
        // 1 获取表 with 属性
        Options coreOptions = Options.fromMap(tableSchema.options());

        // 2 获取表属性 key = write-mode 默认 auto
        WriteMode writeMode = coreOptions.get(CoreOptions.WRITE_MODE);
        if (writeMode == WriteMode.AUTO) {
            // 2.1 判断表创建的时候是否指定 PRIMARY KEY
            // 如果指定 那么该表写模式 CHANGE_LOG 否则为 APPEND_ONLY
            writeMode =
                    tableSchema.primaryKeys().isEmpty()
                            ? WriteMode.APPEND_ONLY
                            : WriteMode.CHANGE_LOG;
            coreOptions.set(CoreOptions.WRITE_MODE, writeMode);
        }

        // 3 根据表写模式创建对应的 FileStoreTable
        if (writeMode == WriteMode.APPEND_ONLY) {
            // 3.1 创建 APPEND_ONLY 表 FileStoreTable
            table = new AppendOnlyFileStoreTable(fileIO, tablePath, tableSchema);
        } else {
            if (tableSchema.primaryKeys().isEmpty()) {
                table = new ChangelogValueCountFileStoreTable(fileIO, tablePath, tableSchema);
            } else {
                // 3.2 创建 CHANGE_LOG 表 FileStoreTable
                table = new ChangelogWithKeyFileStoreTable(fileIO, tablePath, tableSchema);
            }
        }
        return table.copy(dynamicOptions.toMap());
    }
}
