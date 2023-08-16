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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Universal Compaction Style is a compaction style, targeting the use cases requiring lower write
 * amplification, trading off read amplification and space amplification.
 *
 * <p>See RocksDb Universal-Compaction:
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    private final int maxSizeAmp;
    private final int sizeRatio;
    private final int numRunCompactionTrigger;
    private final int maxSortedRunNum;

    public UniversalCompaction(
            int maxSizeAmp, int sizeRatio, int numRunCompactionTrigger, int maxSortedRunNum) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.maxSortedRunNum = maxSortedRunNum;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        // 1 checking for reducing size amplification
        // 检查是否写放大
        // 如果前面 N-1 个 sorted-run 数据量大小总和 * 100 大于第 N 个 sorted-run 大小 说明写放大了 需要合并
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 2 checking for size ratio
        // 检查大小率
        // 逻辑如下：
        // 从第一个 sorted-run 开始计算
        // 前面 sorted-run 数据量大小总和 * (100.0 + 1) / 100.0 小于当前遍历 sorted-run 的数据量大小
        // 返回为 false 说明前面写入的数据量很大 需要执行合并
        // 如果挑选出来的 sorted-run 都在 level0 将 level0 的数据合并到下一个 level
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 3 checking for file num
        // 检查文件个数
        // 如果 LSM 树的 sorted-run 个数大于 5
        if (runs.size() > numRunCompactionTrigger) {
            // compacting for file num
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            // 逻辑跟检查大小率相同
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        // 1 检查 LSM 树下 sorted-run 个数是否小于阈值
        // 默认 numRunCompactionTrigger -> num-sorted-run.compaction-trigger = 5
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        // 2 统计前面 N-1 个 sorted-run 数据大小
        long candidateSize =
                runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        // 3 获取第 N 个 sorted-run 数据大小
        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // size amplification = percentage of additional size
        // 4 计算放大大小
        // maxSizeAmp -> 默认 compaction.max-size-amplification-percent = 200
        // 也即如果前面 N-1 个 sorted-run 数据量大小总和 * 100 大于第 N 个 sorted-run 大小 说明写放大了 需要合并
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            return CompactUnit.fromLevelRuns(maxLevel, runs);
        }

        return null;
    }

    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        // 1 检查 LSM 树下 sorted-run 个数是否小于阈值
        // 默认 numRunCompactionTrigger -> num-sorted-run.compaction-trigger = 5
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        // 2 挑选
        // 逻辑如下：
        // 从第一个 sorted-run 开始计算
        // 前面 sorted-run 数据量大小总和 * (100.0 + 1) / 100.0 小于当前遍历 sorted-run 的数据量大小
        // 返回为 false 说明前面写入的数据量很大 需要执行合并
        // 如果挑选出来的 sorted-run 都在 level0 将 level0 的数据合并到下一个 level
        return pickForSizeRatio(maxLevel, runs, 1);
    }

    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        // 挑选
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {
        // 1 计算第一个 sorted-run 的数量大小
        long candidateSize = candidateSize(runs, candidateCount);
        // 2 遍历剩余的 sorted-run
        for (int i = candidateCount; i < runs.size(); i++) {
            // 2.1 获取遍历 sorted-run
            LevelSortedRun next = runs.get(i);
            // 2.2 前面 sorted-run 数据量大小总和 * (100.0 + 1) / 100.0 小于当前遍历 sorted-run 的数据量大小
            // 返回为 false 说明前面写入的数据量很大 需要执行合并
            if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.run().totalSize()) {
                break;
            }

            // 2.3 累加
            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        // 3 判断是否需要合并
        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount, maxSortedRunNum);
        }

        return null;
    }

    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    @VisibleForTesting
    static CompactUnit createUnit(
            List<LevelSortedRun> runs, int maxLevel, int runCount, int maxSortedRunNum) {
        int outputLevel;
        // 1 一般情况下 maxSortedRunNum = Integer.MAX_VALUE
        // 判断需要合并到哪个 level
        if (runCount > maxSortedRunNum) {
            runCount = maxSortedRunNum;
        }
        if (runCount == runs.size()) {
            outputLevel = maxLevel;
        } else {
            // level of next run - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        if (outputLevel == 0) {
            // do not output level 0
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }
}
