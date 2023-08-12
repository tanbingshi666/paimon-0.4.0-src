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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#LATEST_FULL} startup mode.
 */
public class FullStartingScanner implements StartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(FullStartingScanner.class);

    @Override
    public Result scan(SnapshotManager snapshotManager, SnapshotSplitReader snapshotSplitReader) {
        // 获取上一次 snapshot 也即读取 snapshot 文件下的所有子文件 比如 snapshot-1、snapshot-2 则返回 2
        Long startingSnapshotId = snapshotManager.latestSnapshotId();
        if (startingSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Waiting for snapshot generation.");
            return new NoSnapshot();
        }
        // 创建 ScannedResult
        return new ScannedResult(
                startingSnapshotId,
                // 读取数据切片
                snapshotSplitReader
                        // 读取全部数据类型
                        .withKind(ScanKind.ALL)
                        // 从指定的 snapshot 读取数据
                        .withSnapshot(startingSnapshotId)
                        // 获取文件切片
                        .splits());
    }
}
