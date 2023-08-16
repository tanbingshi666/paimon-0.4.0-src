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

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Compact task for merge tree compaction.
 */
public class MergeTreeCompactTask extends CompactTask {

    private final long minFileSize;
    private final CompactRewriter rewriter;
    private final int outputLevel;

    private final List<List<SortedRun>> partitioned;

    private final boolean dropDelete;

    // metric
    private int upgradeFilesNum;

    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete) {
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        // 根据 sorted-run 对应的数据文件元数据进行分区分组排序
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
        this.dropDelete = dropDelete;

        this.upgradeFilesNum = 0;
    }

    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>();
        CompactResult result = new CompactResult();

        // Checking the order and compacting adjacent and contiguous files
        // Note: can't skip an intermediate file to compact, this will destroy the overall
        // orderliness
        // 1 遍历需要合并的 sorted-run
        for (List<SortedRun> section : partitioned) {
            // 说明有多个 sorted-run 进行合并
            if (section.size() > 1) {
                candidate.add(section);
            } else {
                // 说明 sorted-run 都没有重叠的 ket
                SortedRun run = section.get(0);
                // No overlapping:
                // We can just upgrade the large file and just change the level instead of
                // rewriting it
                // But for small files, we will try to compact it
                for (DataFileMeta file : run.files()) {
                    // 如果 sorted-run 对应一个数据文件小于 128MB 说明该数据文件为小文件 需要合并
                    if (file.fileSize() < minFileSize) {
                        // Smaller files are rewritten along with the previous files
                        candidate.add(singletonList(SortedRun.fromSingle(file)));
                    } else {
                        // Large file appear, rewrite previous and upgrade it
                        // 说明一个数据文件大于等于 128MB 不会重写磁盘 而是改变其 level
                        rewrite(candidate, result);
                        upgrade(file, result);
                    }
                }
            }
        }

        // sorted-run 本质就是重写
        rewrite(candidate, result);
        return result;
    }

    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), upgradeFilesNum);
    }

    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        if (file.level() != outputLevel) {
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult);
            upgradeFilesNum++;
        }
    }

    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        // 有两种情况：
        // 第一种是针对单个 sorted-run
        // 第二种是针对多个 sorted-run
        if (candidate.isEmpty()) {
            return;
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate);
                }
                candidate.clear();
                return;
            }
        }

        // 执行重写合并
        // 如果表 changelog-producer = full-compact 或者 lookup 则调用 ChangelogMergeTreeRewriter
        // 否则默认调用 MergeTreeCompactRewriter
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        // 重写合并结果
        toUpdate.merge(rewriteResult);
        candidate.clear();
    }
}
