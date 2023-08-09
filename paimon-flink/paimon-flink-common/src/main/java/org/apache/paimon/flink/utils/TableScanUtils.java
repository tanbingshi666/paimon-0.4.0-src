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

package org.apache.paimon.flink.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.TableScan;

import java.util.HashMap;

/**
 * Utility methods for {@link TableScan}, such as validating.
 */
public class TableScanUtils {

    public static void streamingReadingValidate(Table table) {
        // 1 获取表选项属性
        CoreOptions options = CoreOptions.fromMap(table.options());
        // 2 获取表选项属性 key = merge-engine 默认 value = DEDUPLICATE
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        // 3 缓存表引擎 (PARTIAL_UPDATE、AGGREGATE)
        HashMap<CoreOptions.MergeEngine, String> mergeEngineDesc =
                new HashMap<CoreOptions.MergeEngine, String>() {
                    {
                        put(CoreOptions.MergeEngine.PARTIAL_UPDATE, "Partial update");
                        put(CoreOptions.MergeEngine.AGGREGATE, "Pre-aggregate");
                    }
                };
        // 4 如果表定义 Primary-Key 并且表引擎为 PARTIAL_UPDATE、AGGREGATE
        // 则表的 changelog-producer 必须是 lookup 或者 full-compaction
        if (table.primaryKeys().size() > 0 && mergeEngineDesc.containsKey(mergeEngine)) {
            switch (options.changelogProducer()) {
                case NONE:
                case INPUT:
                    throw new RuntimeException(
                            mergeEngineDesc.get(mergeEngine)
                                    + " streaming reading is not supported. You can use "
                                    + "'lookup' or 'full-compaction' changelog producer to support streaming reading.");
                default:
            }
        }
    }
}
