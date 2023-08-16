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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Compact manager for {@link KeyValueFileStore}.
 */
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class);

    private final ExecutorService executor;
    private final Levels levels;
    private final CompactStrategy strategy;
    private final Comparator<InternalRow> keyComparator;
    private final long minFileSize;
    private final int numSortedRunStopTrigger;
    private final CompactRewriter rewriter;

    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.minFileSize = minFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
    }

    @Override
    public boolean shouldWaitCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        levels.addLevel0File(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;
        // 1 获取 level 的所有 sorted-run
        // 一般情况下 level0 对应多个 sorted-run 非 level0 下每个 level 对应一个 sorted-run
        // 一个 sorted-run 对应多个数据文件 data-file
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        // 如果是 fullCompaction
        if (fullCompaction) {
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaciton. Picking from the following runs\n{}",
                        runs);
            }
            // 2 挑选哪些 sorted-run 需要合并 (针对的是 full-compact)
            // 挑选合并的 sorted 一般情况下执行合并都是针对 bucket (对应一个 LSM) 下的所有 sorted-run 对应的所有数据文件
            optionalUnit = CompactStrategy.pickFullCompaction(levels.numberOfLevels(), runs);
        } else {
            if (taskFuture != null) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaciton. Picking from the following runs\n{}", runs);
            }
            // 3 异步执行 level0 的数据合并 (可以理解为局部 min-compact)
            optionalUnit =
                    // 挑选 sorted-run 合并
                    strategy.pick(levels.numberOfLevels(), runs)
                            .filter(unit -> unit.files().size() > 0)
                            .filter(
                                    unit ->
                                            unit.files().size() > 1
                                                    || unit.files().get(0).level()
                                                    != unit.outputLevel());
        }

        optionalUnit.ifPresent(
                unit -> {
                    /*
                     * As long as there is no older data, We can drop the deletion.
                     * If the output level is 0, there may be older data not involved in compaction.
                     * If the output level is bigger than 0, as long as there is no older data in
                     * the current levels, the output is the oldest, so we can drop the deletion.
                     * See CompactStrategy.pick.
                     */
                    boolean dropDelete =
                            unit.outputLevel() != 0
                                    && unit.outputLevel() >= levels.nonEmptyHighestLevel();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Submit compaction with files (name, level, size): "
                                        + levels.levelSortedRuns().stream()
                                        .flatMap(lsr -> lsr.run().files().stream())
                                        .map(
                                                file ->
                                                        String.format(
                                                                "(%s, %d, %d)",
                                                                file.fileName(),
                                                                file.level(),
                                                                file.fileSize()))
                                        .collect(Collectors.joining(", ")));
                    }

                    // 4 提交合并任务
                    submitCompaction(unit, dropDelete);
                });
    }

    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        // 1 创建合并任务 MergeTreeCompactTask
        MergeTreeCompactTask task =
                new MergeTreeCompactTask(keyComparator, minFileSize, rewriter, unit, dropDelete);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for compaction: {}",
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }
        // 2 提交合并任务 MergeTreeCompactTask 调用其 doCompact()
        taskFuture = executor.submit(task);
    }

    /**
     * Finish current task, and update result files to {@link Levels}.
     */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        // 获取合并结果
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    // 更新 LSM Level
                    levels.update(r.before(), r.after());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    @Override
    public void close() throws IOException {
        rewriter.close();
    }
}
