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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.OrderedPacking;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Merge tree implementation of {@link SplitGenerator}.
 */
public class MergeTreeSplitGenerator implements SplitGenerator {

    private final Comparator<InternalRow> keyComparator;

    private final long targetSplitSize;

    private final long openFileCost;

    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator, long targetSplitSize, long openFileCost) {
        this.keyComparator = keyComparator;
        // 默认 128MB
        this.targetSplitSize = targetSplitSize;
        // 默认 4MB
        this.openFileCost = openFileCost;
    }

    @Override
    public List<List<DataFileMeta>> split(List<DataFileMeta> files) {
        /*
         * The generator aims to parallel the scan execution by slicing the files of each bucket
         * into multiple splits. The generation has one constraint: files with intersected key
         * ranges (within one section) must go to the same split. Therefore, the files are first to go
         * through the interval partition algorithm to generate sections and then through the
         * OrderedPack algorithm. Note that the item to be packed here is each section, the capacity
         * is denoted as the targetSplitSize, and the final number of the bins is the number of
         * splits generated.
         * 这个切片生成器的目的就是切片 Primary-Key 类型的数据, 一般情况下 一张表有多个分区以及对应的多个分桶
         * 每个分桶存储着不同 key 范围数据 也即每个桶下的数据不会存在相同的 key 但是桶下可能存在多个 data-file
         * 这些数据文件可能存在重叠的 key 这些重叠 key 对应的数据文件必须被分配到同一个 split 以便数据读取进行合并
         *
         * For instance, there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
         * with targetSplitSize 128M. After interval partition, there are four sections:
         * - section1: [1, 2]
         * - section2: [3, 4]
         * - section3: [5, 180], [5, 190]
         * - section4: [200, 600], [210, 700]
         *
         * After OrderedPack, section1 and section2 will be put into one bin (split), so the final result will be:
         * - split1: [1, 2] [3, 4]
         * - split2: [5, 180] [5,190]
         * - split3: [200, 600] [210, 700]
         */
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        // 切片
        // 切片逻辑：
        // 1 根据 data-file 文件的key值范围降序排序，然后将相邻data-file存在重叠key的数据文件划分为一个sorted-run 因此sorted-run之间不会存在重叠的key
        // 2 依次判断sorted-run的累加文件数据大小是否大于等于128MB，逻辑根据 AppendOnlySplitGenerator 相同
        return packSplits(sections);
    }

    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return OrderedPacking.pack(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }
}
