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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.utils.Preconditions;

import java.util.Optional;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;

/**
 * An abstraction layer above {@link FileStoreScan} to provide input split generation.
 */
public abstract class AbstractInnerTableScan implements InnerTableScan {

    private final CoreOptions options;
    protected final SnapshotSplitReader snapshotSplitReader;

    protected AbstractInnerTableScan(CoreOptions options, SnapshotSplitReader snapshotSplitReader) {
        this.options = options;
        this.snapshotSplitReader = snapshotSplitReader;
    }

    @VisibleForTesting
    public AbstractInnerTableScan withBucket(int bucket) {
        snapshotSplitReader.withBucket(bucket);
        return this;
    }

    public CoreOptions options() {
        return options;
    }

    protected StartingScanner createStartingScanner(boolean isStreaming) {
        if (options.toConfiguration().get(CoreOptions.STREAMING_COMPACT)) {
            Preconditions.checkArgument(
                    isStreaming, "Set 'streaming-compact' in batch mode. This is unexpected.");
            return new ContinuousCompactorStartingScanner();
        }

        // read from consumer id
        // 1 读取 consumer-id 值
        String consumerId = options.consumerId();
        // 1.1 如果 consumer-id 存在
        if (consumerId != null) {
            // 1.2 获取消费者管理者 ConsumerManager
            ConsumerManager consumerManager = snapshotSplitReader.consumerManager();
            // 1.3 判断消费者路径以及内容是否存在
            Optional<Consumer> consumer = consumerManager.consumer(consumerId);
            if (consumer.isPresent()) {
                // 1.4 如果上一次 consumer_id 存在 则直接根据 consumer_id 继续消费表数据
                return new ContinuousFromSnapshotStartingScanner(consumer.get().nextSnapshot());
            }
        }

        // 2 如果没有 consumer_id 那么一般情况下基于全增量一体读取表数据 也即启动模式为 LATEST_FULL
        // 默认返回 FullStartingScanner
        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner();
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner()
                        : new FullStartingScanner();
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits =
                            options.toConfiguration()
                                    .getOptional(FULL_COMPACTION_DELTA_COMMITS)
                                    .orElse(1);
                    return new FullCompactedStartingScanner(deltaCommits);
                } else {
                    return new CompactedStartingScanner();
                }
            case FROM_TIMESTAMP:
                Long startupMillis = options.scanTimestampMills();
                Preconditions.checkNotNull(
                        startupMillis,
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
                                CoreOptions.StartupMode.FROM_TIMESTAMP,
                                CoreOptions.SCAN_MODE.key()));
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(startupMillis)
                        : new StaticFromTimestampStartingScanner(startupMillis);
            case FROM_SNAPSHOT:
            case FROM_SNAPSHOT_FULL:
                Long snapshotId = options.scanSnapshotId();
                Preconditions.checkNotNull(
                        snapshotId,
                        String.format(
                                "%s can not be null when you use %s for %s",
                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                startupMode,
                                CoreOptions.SCAN_MODE.key()));
                return isStreaming && startupMode == CoreOptions.StartupMode.FROM_SNAPSHOT
                        ? new ContinuousFromSnapshotStartingScanner(snapshotId)
                        : new StaticFromSnapshotStartingScanner(snapshotId);
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }
}
