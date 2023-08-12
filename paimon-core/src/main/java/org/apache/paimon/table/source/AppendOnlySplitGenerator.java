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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.OrderedPacking;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.append.AppendOnlyCompactManager.fileComparator;

/**
 * Append only implementation of {@link SplitGenerator}.
 */
public class AppendOnlySplitGenerator implements SplitGenerator {

    private final long targetSplitSize;
    private final long openFileCost;

    public AppendOnlySplitGenerator(long targetSplitSize, long openFileCost) {
        // 128MB
        this.targetSplitSize = targetSplitSize;
        // 4MB
        this.openFileCost = openFileCost;
    }

    @Override
    public List<List<DataFileMeta>> split(List<DataFileMeta> input) {
        // 这个 input 就是每个桶的 data-file 文件元数据
        List<DataFileMeta> files = new ArrayList<>(input);
        // 根据元数据的 minSequenceNumber 进行排序
        files.sort(fileComparator(false));
        // 执行文件切片划分
        // 切分逻辑：
        // 1 如果一个 data-file 大小大于 source.split.target-size=128MB 则该文件作为一个切片
        // 2 如果一个 data-file 大小小于 source.split.open-file-cost=4MB 则认为该文件为 4MB 继续遍历累加文件大小 直到大于 128MB 累积的 data-file 作为一个切片
        Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);
        return OrderedPacking.pack(files, weightFunc, targetSplitSize);
    }
}
