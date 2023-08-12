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
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.CompactionChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorFollowUpScanner;
import org.apache.paimon.table.source.snapshot.DeltaFollowUpScanner;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.InputChangelogFollowUpScanner;
import org.apache.paimon.table.source.snapshot.SnapshotSplitReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * {@link StreamTableScan} implementation for streaming planning.
 */
public class InnerStreamTableScanImpl extends AbstractInnerTableScan
        implements InnerStreamTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(InnerStreamTableScanImpl.class);

    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final boolean supportStreamingReadOverwrite;

    private StartingScanner startingScanner;
    private FollowUpScanner followUpScanner;
    private BoundedChecker boundedChecker;
    private boolean isFullPhaseEnd = false;
    @Nullable
    private Long nextSnapshotId;

    public InnerStreamTableScanImpl(
            CoreOptions options,
            SnapshotSplitReader snapshotSplitReader,
            SnapshotManager snapshotManager,
            boolean supportStreamingReadOverwrite) {
        super(options, snapshotSplitReader);
        this.options = options;
        this.snapshotManager = snapshotManager;
        this.supportStreamingReadOverwrite = supportStreamingReadOverwrite;
    }

    @Override
    public InnerStreamTableScanImpl withFilter(Predicate predicate) {
        snapshotSplitReader.withFilter(predicate);
        return this;
    }

    @Override
    public Plan plan() {
        // 1 创建 StartingScanner
        // 默认全增量一体读取表数据(LATEST_FULL) FullStartingScanner
        if (startingScanner == null) {
            startingScanner = createStartingScanner(true);
        }

        // 2 根据 key = changelog-producer 创建对应的 FollowUpScanner
        // 2.1 changelog-producer = none -> DeltaFollowUpScanner
        // 2.2 changelog-producer = input -> InputChangelogFollowUpScanner
        // 2.3 changelog-producer = full-compaction -> CompactionChangelogFollowUpScanner
        // 2.4 changelog-producer = lookup -> CompactionChangelogFollowUpScanner
        // 2.5 changelog-producer = null -> null
        if (followUpScanner == null) {
            followUpScanner = createFollowUpScanner();
        }

        // 3 判断读取数据是否指定 key = scan.bounded.watermark
        if (boundedChecker == null) {
            boundedChecker = createBoundedChecker();
        }

        // 4 判断任务启动是否依赖上一次的 checkpoint
        if (nextSnapshotId == null) {
            // 返回 DataFilePlan
            return tryFirstPlan();
        } else {
            // 5 这就就是动态检测哪些文件新生成
            return nextPlan();
        }
    }

    private Plan tryFirstPlan() {
        // 1 获取启动读取表数据扫描器(里面进行数据切片)
        // 默认调用 FullStartingScanner.scan()
        StartingScanner.Result result = startingScanner.scan(snapshotManager, snapshotSplitReader);

        // 2 一般情况下 ScannedResult
        if (result instanceof StartingScanner.ScannedResult) {
            long currentSnapshotId = ((StartingScanner.ScannedResult) result).currentSnapshotId();
            // 记录下一次 snapshot id
            nextSnapshotId = currentSnapshotId + 1;
            // 是否根据 watermark 终止读取数据
            // 一般情况下为 false 除非定义 scan.bounded.watermark=xxx
            isFullPhaseEnd =
                    boundedChecker.shouldEndInput(snapshotManager.snapshot(currentSnapshotId));
        } else if (result instanceof StartingScanner.NextSnapshot) {
            nextSnapshotId = ((StartingScanner.NextSnapshot) result).nextSnapshotId();
            isFullPhaseEnd =
                    snapshotManager.snapshotExists(nextSnapshotId - 1)
                            && boundedChecker.shouldEndInput(
                            snapshotManager.snapshot(nextSnapshotId - 1));
        }

        // 3 返回切片好的 Plan = DataFilePlan
        return DataFilePlan.fromResult(result);
    }

    private Plan nextPlan() {
        while (true) {
            if (isFullPhaseEnd) {
                throw new EndOfScanException();
            }

            if (!snapshotManager.snapshotExists(nextSnapshotId)) {
                LOG.debug(
                        "Next snapshot id {} does not exist, wait for the snapshot generation.",
                        nextSnapshotId);
                return new DataFilePlan(Collections.emptyList());
            }

            Snapshot snapshot = snapshotManager.snapshot(nextSnapshotId);

            if (boundedChecker.shouldEndInput(snapshot)) {
                throw new EndOfScanException();
            }

            // first check changes of overwrite
            if (snapshot.commitKind() == Snapshot.CommitKind.OVERWRITE
                    && supportStreamingReadOverwrite) {
                LOG.debug("Find overwrite snapshot id {}.", nextSnapshotId);
                Plan overwritePlan =
                        followUpScanner.getOverwriteChangesPlan(
                                nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return overwritePlan;
            } else if (followUpScanner.shouldScanSnapshot(snapshot)) {
                LOG.debug("Find snapshot id {}.", nextSnapshotId);
                Plan plan = followUpScanner.scan(nextSnapshotId, snapshotSplitReader);
                nextSnapshotId++;
                return plan;
            } else {
                nextSnapshotId++;
            }
        }
    }

    private FollowUpScanner createFollowUpScanner() {
        if (options.toConfiguration().get(CoreOptions.STREAMING_COMPACT)) {
            return new ContinuousCompactorFollowUpScanner();
        }

        // 1 获取读取表属性 key = changelog-producer
        CoreOptions.ChangelogProducer changelogProducer = options.changelogProducer();
        FollowUpScanner followUpScanner;
        switch (changelogProducer) {
            case NONE:
                // 1.1 changelog-producer = none
                followUpScanner = new DeltaFollowUpScanner();
                break;
            case INPUT:
                // 1.2 changelog-producer = input
                followUpScanner = new InputChangelogFollowUpScanner();
                break;
            case FULL_COMPACTION:
                // 1.3 changelog-producer = full-compaction
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level == options.numLevels() - 1);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            case LOOKUP:
                // 1.4 changelog-producer = lookup
                // this change in data split reader will affect both starting scanner and follow-up
                snapshotSplitReader.withLevelFilter(level -> level > 0);
                followUpScanner = new CompactionChangelogFollowUpScanner();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown changelog producer " + changelogProducer.name());
        }
        return followUpScanner;
    }

    private BoundedChecker createBoundedChecker() {
        Long boundedWatermark = options.scanBoundedWatermark();
        return boundedWatermark != null
                ? BoundedChecker.watermark(boundedWatermark)
                : BoundedChecker.neverEnd();
    }

    @Nullable
    @Override
    public Long checkpoint() {
        return nextSnapshotId;
    }

    @Override
    public void restore(@Nullable Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
    }

    @Override
    public void notifyCheckpointComplete(@Nullable Long nextSnapshot) {
        if (nextSnapshot == null) {
            return;
        }

        String consumerId = options.consumerId();
        if (consumerId != null) {
            snapshotSplitReader
                    .consumerManager()
                    .recordConsumer(consumerId, new Consumer(nextSnapshot));
        }
    }
}
